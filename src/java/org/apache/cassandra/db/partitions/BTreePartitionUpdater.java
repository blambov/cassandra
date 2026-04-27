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

package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.Cloner;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;

/**
 *  the function we provide to the trie and btree utilities to perform any row and column replacements
 */
public class BTreePartitionUpdater extends BasePartitionUpdater implements UpdateFunction<Row, Row>
{
    final MemtableAllocator allocator;
    final OpOrder.Group writeOp;
    final UpdateTransaction indexer;
    public int partitionsAdded = 0;

    public BTreePartitionUpdater(MemtableAllocator allocator, Cloner cloner, OpOrder.Group writeOp, UpdateTransaction indexer)
    {
        super(cloner);
        this.allocator = allocator;
        this.writeOp = writeOp;
        this.indexer = indexer;
    }

    public BTreePartitionData mergePartitions(BTreePartitionData current, final BTreePartitionUpdate update)
    {
        if (current == null)
        {
            keySize = update.partitionKey.getKeyLength();
            current = BTreePartitionData.EMPTY;
            onAllocatedOnHeap(BTreePartitionData.UNSHARED_HEAP_SIZE);
            ++partitionsAdded;
        }

        try
        {
            indexer.start();

            return makeMergedPartition(current, update);
        }
        finally
        {
            indexer.commit();
            reportAllocatedMemory();
        }
    }

    protected BTreePartitionData makeMergedPartition(BTreePartitionData current, BTreePartitionUpdate update)
    {
        if (cloner.isContextAwareCloningSupported()) // to avoid an estimation cost if context aware cloning is not supported
        {
            int estimitedCloneSize = 0;
            // a typical case when all values in the update are used in the result of the merge
            // clustering key cloning is needed when we have an insert but not needed when we have an update,
            // so we may allocate a bit more than needed sometimes
            for (Row row : update)
            {
                estimitedCloneSize += (int) row.accumulate((cd, v) -> v + cd.estimateCloneSize(cloner), 0);
                estimitedCloneSize += cloner.estimateCloneSize(row.clustering());
            }

            Row staticRow = update.staticRow();
            if (!staticRow.isEmpty())
            {
                estimitedCloneSize += (int) staticRow.accumulate((cd, v) -> v + cd.estimateCloneSize(cloner), 0);
                // there are no clustering keys for static rows
            }

            makeContextAwareCloner(estimitedCloneSize);
        }
        else
        {
            useNonContextAwareCloner();
        }

        DeletionInfo newDeletionInfo = merge(current.deletionInfo, update.deletionInfo());

        RegularAndStaticColumns columns = current.columns;
        RegularAndStaticColumns newColumns = update.columns().mergeTo(columns);
        onAllocatedOnHeap(newColumns.unsharedHeapSize() - columns.unsharedHeapSize());
        Row newStatic = mergeStatic(current.staticRow, update.staticRow());

        Object[] tree = BTree.update(current.tree, update.holder().tree, update.metadata().comparator, this);
        EncodingStats newStats = current.stats.mergeWith(update.stats());
        onAllocatedOnHeap(newStats.unsharedHeapSize() - current.stats.unsharedHeapSize());

        return new BTreePartitionData(newColumns, tree, newDeletionInfo, newStatic, newStats);
    }

    private Row mergeStatic(Row current, Row update)
    {
        if (update.isEmpty())
            return current;
        if (current.isEmpty())
            return insert(update);

        return merge(current, update);
    }

    private DeletionInfo merge(DeletionInfo existing, DeletionInfo update)
    {
        if (update.isLive() || !update.mayModify(existing))
            return existing;

        if (!update.getPartitionDeletion().isLive())
            indexer.onPartitionDeletion(update.getPartitionDeletion());

        if (update.hasRanges())
            update.rangeIterator(false).forEachRemaining(indexer::onRangeTombstone);

        // Like for rows, we have to clone the update in case internal buffers (when it has range tombstones) reference
        // memory we shouldn't hold into. But we don't ever store this off-heap currently so we just default to the
        // HeapAllocator (rather than using 'allocator').
        DeletionInfo newInfo = existing.mutableCopy().add(update.clone(HeapCloner.instance));
        onAllocatedOnHeap(newInfo.unsharedHeapSize() - existing.unsharedHeapSize());
        return newInfo;
    }

    @Override
    public Row insert(Row insert)
    {
        Row data = insert.clone(contextCloner);
        indexer.onInserted(insert);

        dataSize += data.dataSize();
        heapSize += data.unsharedHeapSizeExcludingData();
        return data;
    }

    public Row merge(Row existing, Row update)
    {
        Row reconciled = ((BTreeRow) existing).mergeWith((BTreeRow) update, this);
        indexer.onUpdated(existing, reconciled);

        return reconciled;
    }

    public void reportAllocatedMemory()
    {
        allocator.onHeap().adjust(heapSize, writeOp);
        adjustUnusedContextAwareCloner();
    }
}
