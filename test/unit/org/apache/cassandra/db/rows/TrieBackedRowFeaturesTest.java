/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.rows;

import java.util.Iterator;
import java.util.Objects;
import java.util.Random;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/// Verifies that the row properties [TrieBackedRow] collects during construction match the ones computed by walking
/// the row's trie, for rows coming from trie memtables (as used for flushing and reads) and rows derived from them.
public class TrieBackedRowFeaturesTest extends CQLTester
{
    static final int PARTITIONS = 5;
    static final int CLUSTERINGS = 12;

    @Test
    public void testFeaturesMatchTrieContent() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, st int static, v1 int, v2 text, s set<int>, m map<int, text>, " +
                    "PRIMARY KEY (pk, ck)) WITH memtable = 'trie'");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();
        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        assertTrue(memtable instanceof TrieMemtable);

        long seed = System.nanoTime();
        logger.info("Using seed {}", seed);
        Random rand = new Random(seed);
        for (int i = 0; i < 1500; ++i)
            applyRandomOperation(rand);

        TableMetadata metadata = cfs.metadata();
        PartitionPosition min = cfs.getPartitioner().getMinimumToken().minKeyBound();
        int rowsChecked = 0;
        for (Partition partition : memtable.getFlushSet(min, min))
        {
            // Rows as presented to flush
            rowsChecked += checkRows(partition.unfilteredIterator(), metadata);
            // Rows as presented to reads, including slices and reverse iteration
            rowsChecked += checkRows(partition.unfilteredIterator(ColumnFilter.all(metadata), Slices.ALL, true), metadata);
            Slices slices = Slices.with(metadata.comparator,
                                        Slice.make(metadata.comparator.make(2), metadata.comparator.make(7)));
            rowsChecked += checkRows(partition.unfilteredIterator(ColumnFilter.all(metadata), slices, false), metadata);
            // Rows copied to heap
            rowsChecked += checkRows(memtable.rowIterator(partition.partitionKey()), metadata);
        }
        assertTrue("Too few rows checked: " + rowsChecked, rowsChecked > 100);
    }

    private void applyRandomOperation(Random rand) throws Throwable
    {
        int pk = rand.nextInt(PARTITIONS);
        int ck = rand.nextInt(CLUSTERINGS);
        String ttl = rand.nextInt(5) == 0 ? " USING TTL " + (1000 + rand.nextInt(1000)) : "";
        switch (rand.nextInt(16))
        {
            case 0:
            case 1:
                execute("INSERT INTO %s (pk, ck, v1, v2) VALUES (?, ?, ?, ?)" + ttl, pk, ck, rand.nextInt(), "v" + rand.nextInt(100));
                break;
            case 2:
                execute("UPDATE %s" + ttl + " SET v1 = ? WHERE pk = ? AND ck = ?", rand.nextInt(), pk, ck);
                break;
            case 3:
                execute("UPDATE %s" + ttl + " SET s = s + ? WHERE pk = ? AND ck = ?", set(rand.nextInt(10), rand.nextInt(10)), pk, ck);
                break;
            case 4:
                execute("UPDATE %s" + ttl + " SET s = ? WHERE pk = ? AND ck = ?", set(rand.nextInt(10)), pk, ck);
                break;
            case 5:
                execute("UPDATE %s" + ttl + " SET m[?] = ? WHERE pk = ? AND ck = ?", rand.nextInt(10), "m" + rand.nextInt(10), pk, ck);
                break;
            case 6:
                execute("DELETE v1 FROM %s WHERE pk = ? AND ck = ?", pk, ck);
                break;
            case 7:
                execute("DELETE s FROM %s WHERE pk = ? AND ck = ?", pk, ck);
                break;
            case 8:
                execute("DELETE m[?] FROM %s WHERE pk = ? AND ck = ?", rand.nextInt(10), pk, ck);
                break;
            case 9:
                execute("DELETE m FROM %s WHERE pk = ? AND ck = ?", pk, ck);
                break;
            case 10:
                execute("DELETE FROM %s WHERE pk = ? AND ck = ?", pk, ck);
                break;
            case 11:
                execute("DELETE FROM %s WHERE pk = ? AND ck > ? AND ck < ?", pk, ck, ck + 1 + rand.nextInt(3));
                break;
            case 12:
                if (rand.nextInt(10) == 0)
                    execute("DELETE FROM %s WHERE pk = ?", pk);
                break;
            case 13:
                execute("INSERT INTO %s (pk, ck) VALUES (?, ?)" + ttl, pk, ck);
                break;
            case 14:
                execute("UPDATE %s" + ttl + " SET st = ? WHERE pk = ?", rand.nextInt(), pk);
                break;
            case 15:
                execute("DELETE st FROM %s WHERE pk = ?", pk);
                break;
        }
    }

    private int checkRows(Iterator<? extends Unfiltered> iterator, TableMetadata metadata)
    {
        if (iterator == null)
            return 0;

        int count = 0;
        if (iterator instanceof UnfilteredRowIterator)
        {
            Row staticRow = ((UnfilteredRowIterator) iterator).staticRow();
            if (staticRow instanceof TrieBackedRow)
                count += checkRowAndDerived((TrieBackedRow) staticRow, metadata);
        }

        while (iterator.hasNext())
        {
            Unfiltered unfiltered = iterator.next();
            if (unfiltered instanceof TrieBackedRow)
                count += checkRowAndDerived((TrieBackedRow) unfiltered, metadata);
        }
        return count;
    }

    private int checkRowAndDerived(TrieBackedRow row, TableMetadata metadata)
    {
        checkFeatures(row);

        long nowInSec = FBUtilities.nowInSeconds();
        ColumnMetadata v1 = metadata.getColumn(ColumnIdentifier.getInterned("v1", false));
        ColumnMetadata s = metadata.getColumn(ColumnIdentifier.getInterned("s", false));
        ColumnFilter partialFilter = ColumnFilter.selection(RegularAndStaticColumns.builder().add(v1).add(s).build());
        checkIfTrieBacked(row.filter(partialFilter, metadata));
        checkIfTrieBacked(row.withOnlyQueriedData(ColumnFilter.selectionBuilder().add(v1).build()));
        checkIfTrieBacked(row.purge(DeletionPurger.PURGE_ALL, nowInSec, false));
        checkIfTrieBacked(row.updateAllTimestamp(FBUtilities.timestampMicros()));
        checkIfTrieBacked(row.withRowDeletion(DeletionTime.build(FBUtilities.timestampMicros(), nowInSec)));
        checkIfTrieBacked(row.filter(ColumnFilter.all(metadata), DeletionTime.build(1, nowInSec), true, metadata));
        return 1;
    }

    private void checkIfTrieBacked(Row row)
    {
        if (row instanceof TrieBackedRow)
            checkFeatures((TrieBackedRow) row);
    }

    private void checkFeatures(TrieBackedRow row)
    {
        DeletionAwareTrie<Object, TrieTombstoneMarker> data = row.trie();
        check(row, "columnCount", TrieBackedRow.countColumns(data), row.columnCount());
        check(row, "cellsCount", countCells(data), row.cellsCount());
        check(row, "hasComplexDeletion", hasComplexDeletion(data), row.hasComplexDeletion());
        check(row, "isEmpty", TrieBackedRow.isEmpty(data), row.isEmpty());
        check(row, "isEmptyAfterDeletion", TrieBackedRow.isEmptyAfterDeletion(data), row.isEmptyAfterDeletion());
        check(row, "primaryKeyLivenessInfo", TrieBackedRow.getLivenessInfo(data), row.primaryKeyLivenessInfo());
        check(row, "deletion", TrieBackedRow.getDeletion(data), row.deletion());

        long minLocalDeletionTime = minLocalDeletionTime(data);
        if (minLocalDeletionTime > Long.MIN_VALUE)
            check(row, "hasDeletion before " + minLocalDeletionTime, false, row.hasDeletion(minLocalDeletionTime - 1));
        check(row, "hasDeletion at " + minLocalDeletionTime, true, row.hasDeletion(minLocalDeletionTime));
    }

    private static void check(TrieBackedRow row, String property, Object expected, Object actual)
    {
        if (!Objects.equals(expected, actual))
            fail(property + " expected " + expected + " but was " + actual + " for row trie\n" + row.trie().dump(Object::toString));
    }

    private static int countCells(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        int count = 0;
        for (Iterator<CellData> it = data.contentOnlyTrie().filteredValuesIterator(Direction.FORWARD, CellData.class); it.hasNext(); it.next())
            ++count;
        return count;
    }

    private static boolean hasComplexDeletion(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        for (Iterator<TrieTombstoneMarker> it = data.deletionBranchAtRoot().valueIterator(); it.hasNext();)
        {
            TrieTombstoneMarker.Covering introducedDeletion = it.next().rightDeletion();
            if (introducedDeletion != null && introducedDeletion.deletionKind() == TrieTombstoneMarker.Kind.COLUMN)
                return true;
        }
        return false;
    }

    /// The minimum local deletion time as computed by walking the trie.
    private static long minLocalDeletionTime(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        TrieTombstoneMarker deletionRoot = data.deletionAtRoot();
        if (deletionRoot != null && (data instanceof InMemoryDeletionAwareTrie || deletionRoot.applicableToPointForward() != null))
            return Long.MIN_VALUE;

        boolean includeMarkers = deletionRoot != null;
        long[] result = new long[] { CellData.MAX_DELETION_TIME };
        data.process(Direction.FORWARD, new DeletionAwareTrie.ValueConsumer<Object, TrieTombstoneMarker>()
        {
            @Override
            public void content(Object content)
            {
                if (content instanceof LivenessInfo)
                {
                    LivenessInfo info = (LivenessInfo) content;
                    if (info.isExpiring())
                        result[0] = Math.min(result[0], info.localExpirationTime());
                }
                else if (content instanceof CellData)
                {
                    CellData<?, ?> cell = (CellData<?, ?>) content;
                    result[0] = cell.isTombstone() ? Long.MIN_VALUE : Math.min(result[0], cell.localDeletionTime());
                }
            }

            @Override
            public void deletionMarker(TrieTombstoneMarker marker)
            {
                if (includeMarkers && marker.isBoundary() && marker.applicableToPointForward() != null)
                    result[0] = Long.MIN_VALUE;
            }
        });
        return result[0];
    }
}
