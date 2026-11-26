/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.cassandra.db.partitions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.BasicCursorWalks;
import org.apache.cassandra.db.tries.BasicCursorWalks.Step;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/// Tests that applying partition updates to a trie memtable through [PartitionUpdateTrieView] gives the same result as
/// applying them after converting them to [TriePartitionUpdate].
public class PartitionUpdateTrieViewMemtableTest extends CQLTester
{
    @Test
    public void testBTreeUpdates()
    {
        testUpdates(BTreePartitionUpdate.FACTORY, false);
    }

    @Test
    public void testStage2Updates()
    {
        testUpdates(TriePartitionUpdateStage2.FACTORY, false);
    }

    @Test
    public void testStage2UpdatesDistinctDeletions()
    {
        testUpdates(TriePartitionUpdateStage2.FACTORY, true);
    }

    private void testUpdates(PartitionUpdate.Factory factory, boolean distinctDeletions)
    {
        createTable("CREATE TABLE %s (" + RandomPartitionUpdates.TABLE_COLUMNS + ")");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        TableMetadata metadata = cfs.metadata();
        RandomPartitionUpdates generator = new RandomPartitionUpdates(metadata, factory, new Random(1), distinctDeletions);

        for (int round = 0; round < 20; ++round)
        {
            Memtable converted = cfs.createMemtable(new AtomicReference<>(CommitLogPosition.NONE));
            Memtable viewed = cfs.createMemtable(new AtomicReference<>(CommitLogPosition.NONE));
            assertTrue(converted instanceof TrieMemtable);
            try
            {
                for (int i = 0; i < 100; ++i)
                {
                    PartitionUpdate update = generator.update();
                    try (OpOrder.Group opGroup = Keyspace.writeOrder.start())
                    {
                        converted.put(TriePartitionUpdate.asTrieUpdate(update), UpdateTransaction.NO_OP, opGroup);
                        viewed.put(update, UpdateTransaction.NO_OP, opGroup);
                    }
                }
                compare(metadata, converted, viewed, distinctDeletions);
            }
            finally
            {
                discard(converted);
                discard(viewed);
            }
        }
    }

    private static void discard(Memtable memtable)
    {
        OpOrder.Barrier barrier = Keyspace.writeOrder.newBarrier();
        barrier.issue();
        memtable.switchOut(barrier, new AtomicReference<>(CommitLogPosition.NONE));
        barrier.await();
        memtable.discard();
    }

    /// Compare the memtable states, including the partitions' statistics (included in the partition markers' string
    /// representation). If `readRows` is true, also compare the rows and markers read from the memtables. (This is not
    /// done when deletions can have the same timestamp, because such updates can produce deletion boundaries that
    /// trie-backed partitions fail to convert to range tombstone markers.)
    private static void compare(TableMetadata metadata, Memtable expected, Memtable actual, boolean readRows)
    {
        assertEquals(expected.partitionCount(), actual.partitionCount());
        assertEquals(expected.getLiveDataSize(), actual.getLiveDataSize());

        List<Partition> expectedPartitions = partitions(metadata, expected);
        List<Partition> actualPartitions = partitions(metadata, actual);
        assertEquals(expectedPartitions.size(), actualPartitions.size());
        for (int i = 0; i < expectedPartitions.size(); ++i)
        {
            TrieBackedPartition e = (TrieBackedPartition) expectedPartitions.get(i);
            TrieBackedPartition a = (TrieBackedPartition) actualPartitions.get(i);
            String message = "Partition " + e.partitionKey();
            assertEquals(message, e.partitionKey(), a.partitionKey());
            compareTries(message, e, a);
            if (!readRows)
                continue;
            assertEquals(message, e.partitionLevelDeletion(), a.partitionLevelDeletion());
            assertEquals(message, e.staticRow(), a.staticRow());
            assertEquals(message, e.rowCount(), a.rowCount());
            assertEquals(message, e.tombstoneCount, a.tombstoneCount);
            assertEquals(message, unfiltereds(e), unfiltereds(a));
        }
    }

    private static void compareTries(String message, TrieBackedPartition expected, TrieBackedPartition actual)
    {
        List<Step> e = BasicCursorWalks.walk(expected.trie.basicCursor(Direction.FORWARD));
        List<Step> a = BasicCursorWalks.walk(actual.trie.basicCursor(Direction.FORWARD));
        String details = message + "\nexpected\n" + e + "\nactual\n" + a;
        assertEquals(details, e.size(), a.size());
        for (int i = 0; i < e.size(); ++i)
        {
            Step es = e.get(i);
            Step as = a.get(i);
            assertTrue(details, Arrays.equals(es.path, as.path));
            assertEquals(details, es.onReturnPath, as.onReturnPath);
            assertEquals(details, es.isDeletionBranchRoot, as.isDeletionBranchRoot);
            // Memtable content objects do not implement equals; compare their representations.
            assertEquals(details, String.valueOf(es.content), String.valueOf(as.content));
            assertEquals(details, es.deletion, as.deletion);
        }
    }

    private static List<Partition> partitions(TableMetadata metadata, Memtable memtable)
    {
        List<Partition> partitions = new ArrayList<>();
        try (UnfilteredPartitionIterator iterator = memtable.partitionIterator(ColumnFilter.all(metadata),
                                                                               DataRange.allData(metadata.partitioner),
                                                                               SSTableReadsListener.NOOP_LISTENER))
        {
            while (iterator.hasNext())
            {
                try (UnfilteredRowIterator partition = iterator.next())
                {
                    partitions.add(memtable.getPartition(partition.partitionKey()));
                }
            }
        }
        return partitions;
    }

    private static List<Unfiltered> unfiltereds(Partition partition)
    {
        List<Unfiltered> list = new ArrayList<>();
        try (UnfilteredRowIterator iterator = partition.unfilteredIterator())
        {
            while (iterator.hasNext())
                list.add(iterator.next());
        }
        return list;
    }
}
