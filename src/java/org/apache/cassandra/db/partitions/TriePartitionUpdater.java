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

import javax.annotation.Nullable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryBaseTrie;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.memory.Cloner;

import static org.apache.cassandra.db.rows.TrieBackedRow.RowData;
import static org.apache.cassandra.db.memtable.TrieMemtable.PartitionData;

/**
 *  The function we provide to the trie utilities to perform any partition and row inserts and updates
 */
public final class TriePartitionUpdater
extends BasePartitionUpdater
implements InMemoryBaseTrie.UpsertTransformerWithKeyProducer<Object, Object>
{
    private final UpdateTransaction indexer;
    private final TableMetadata metadata;
    private PartitionData currentPartition;
    private final TrieMemtable.MemtableShard owner;
    private ClusteringBound<byte[]> rangeTombstoneOpenPosition = null;
    private final DeletionTime partitionLevelDeletion;
    public int partitionsAdded = 0;

    public TriePartitionUpdater(Cloner cloner,
                                UpdateTransaction indexer,
                                DeletionTime partitionLevelDeletion,
                                TableMetadata metadata,
                                TrieMemtable.MemtableShard owner)
    {
        super(cloner);
        this.indexer = indexer;
        this.metadata = metadata;
        this.owner = owner;
        this.partitionLevelDeletion = partitionLevelDeletion;
        if (!partitionLevelDeletion.isLive())
            indexer.onPartitionDeletion(partitionLevelDeletion);
    }

    @Override
    public Object apply(@Nullable Object existing, Object update, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        if (update instanceof Cell)
            return applyCell((Cell<?>) existing, (Cell<?>) update, keyState);
        else if (update == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return update; // TODO check if something else needs to be done
        else if (update instanceof RowData)
            return applyRow((RowData) existing, (RowData) update, keyState);
        else if (update == TrieBackedPartition.PARTITION_MARKER)
            return mergePartitionMarkers((PartitionData) existing);
        else
            throw new AssertionError("Unexpected update type: " + update.getClass());
    }

    public TrieTombstoneMarker mergeMarkers(@Nullable TrieTombstoneMarker existing, TrieTombstoneMarker update, InMemoryBaseTrie.KeyProducer<TrieTombstoneMarker> keyState)
    {
        if (indexer != UpdateTransaction.NO_OP)
        {
            if (update.hasPointData(TrieTombstoneMarker.PointDataType.ROW))
            {
                // This row has deletions. The highest deletion time of any deletion is placed at the point data, and
                // the row deletion is given by the boundary's succeeding side.
                // TODO: Figure out how to pass the row to the indexer.
                TrieTombstoneMarker rowDeletion = update.succedingState(Direction.FORWARD);
                DeletionTime deletionTime = rowDeletion != null ? rowDeletion.deletionTime() : null;
                if (deletionTime != null)
                {
                    Clustering<?> clustering = metadata.comparator.clusteringFromByteComparable(
                        ByteArrayAccessor.instance,
                        ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                  keyState.getBytes()));
                    if (existing != null && existing.succedingState(Direction.FORWARD) != null)
                        indexer.onUpdated(BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(existing.succedingState(Direction.FORWARD).deletionTime())),
                                          BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(update.deletionTime())));
                    else
                        indexer.onInserted(BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(update.deletionTime())));
                }
            }
            else if (update.isBoundary())
            {
                // TODO: We need to differentiate between partition, range and column deletions
                if (rangeTombstoneOpenPosition != null)
                {
                    TrieTombstoneMarker preceding = update.precedingState(Direction.FORWARD);
                    assert preceding != null; // open markers are always closed
                    DeletionTime deletionTime = preceding.deletionTime();
                    ClusteringBound<?> bound = metadata.comparator.boundFromByteComparable(
                        ByteArrayAccessor.instance,
                        ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                  keyState.getBytes()),
                        true);
                    indexer.onRangeTombstone(new RangeTombstone(Slice.make(rangeTombstoneOpenPosition,
                                                                           bound),
                                                                deletionTime));
                }

                TrieTombstoneMarker succeeding = update.succedingState(Direction.FORWARD);
                // Ignore the partition deletion.
                if (succeeding != null && !succeeding.deletionTime().equals(partitionLevelDeletion))
                {
                    rangeTombstoneOpenPosition = metadata.comparator.boundFromByteComparable(
                        ByteArrayAccessor.instance,
                        ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                  keyState.getBytes()),
                        false);
                }
                else
                {
                    rangeTombstoneOpenPosition = null;
                }
            }
        }

        if (existing == null)
        {
            currentPartition.markAddedTombstones(1);
            this.heapSize += update.unsharedHeapSize();
            return update;
        }
        else
        {
            TrieTombstoneMarker merged = update.mergeWith(existing);
            this.heapSize += (merged != null ? merged.unsharedHeapSize() : 0) - existing.unsharedHeapSize();
            return merged;
        }
    }

    public Object applyMarker(Object existingContent, TrieTombstoneMarker updateMarker, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        // Most common case first
        if (existingContent instanceof Cell)
            return applyCellDeletion((Cell<?>) existingContent, updateMarker);
        else if (existingContent == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return existingContent; // TODO: How can we check if there's remaining data and remove this if there is none? Cell counter in marker?
        else if (existingContent instanceof RowData)
            return applyRowDeletion((RowData) existingContent, updateMarker, keyState);
        else if (existingContent instanceof PartitionData)
            return applyPartitionDeletion((PartitionData) existingContent, updateMarker);
        else
            throw new AssertionError("Unexpected content in trie " + existingContent + " for deletion " + updateMarker);
    }

    private Cell<?> applyCellDeletion(Cell<?> existingContent, TrieTombstoneMarker updateMarker)
    {
        if (!updateMarker.deletionTime().deletes(existingContent))
            return existingContent;
        heapSize -= existingContent.unsharedHeapSizeExcludingData();
        dataSize -= existingContent.dataSize();
        return null;
    }

    public Object applyPartitionDeletion(PartitionData existing, TrieTombstoneMarker updateMarker)
    {
        indexer.onPartitionDeletion(updateMarker.deletionTime()); // static clustering is deleted only on partition deletion
        existing.clearStats();
        return existing;
    }

    public Object applyRowDeletion(RowData existing, TrieTombstoneMarker updateMarker, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        TrieTombstoneMarker rowDeletion = updateMarker.succedingState(Direction.FORWARD);
        if (rowDeletion == null)
            return existing; // there is no row deletion here

        if (rowDeletion.deletionTime().deletes(existing))
        {
            this.heapSize -= existing.unsharedHeapSize();
            return RowData.NO_LIVENESS; // TODO: How can we remove this if nothing survives? Cell counter in row data and return path processing?
            // TODO: and also do currentPartition.markInsertedRows(-1) in that case?
            // TODO: Does strict row liveness apply here? How do we drop tail trie if it does?
        }
        return existing;

        // TODO: indexer update needs tail trie.
//        if (indexer != UpdateTransaction.NO_OP && updated != existing)
//        {
//            Clustering<?> clustering = clusteringFor(keyState);
//            if (updated != null)
//                indexer.onUpdated(existing.toRow(clustering, DeletionTime.LIVE),
//                                  updated.toRow(clustering, DeletionTime.LIVE));
//            else
//                indexer.onUpdated(existing.toRow(clustering, DeletionTime.LIVE),
//                                  BTreeRow.emptyDeletedRow(clustering, Row.Deletion.regular(updateMarker.deletionTime())));
//        }
//        return updated;
    }

    public Object applyMarker(TrieTombstoneMarker marker, Object content)
    {
        // This is called to apply an existing tombstone to incoming data, before applyRow is called on the result.
        // No size tracking is needed, because the result of this then gets applied to the trie with applyRow.
        if (content instanceof Cell)
            return marker.deletionTime().deletes((Cell<?>) content) ? null : content;
        else if (content == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return content;
        else if (content instanceof RowData)
        {
            TrieTombstoneMarker rowDeletion = marker.succedingState(Direction.FORWARD);
            if (rowDeletion == null || !rowDeletion.deletionTime().deletes((RowData) content))
                return content;
            else
                return null;
        }
        else if (content instanceof PartitionData)
            return content;
        else
            throw new AssertionError("Unexpected content in trie " + content + " for deletion " + marker);
    }

    /**
     * Called when a row needs to be copied to the Memtable trie.
     *
     * @param existing Existing RowData for this clustering, or null if there isn't any.
     * @param insert RowData to be inserted.
     * @param keyState Used to obtain the path through which this node was reached.
     * @return the insert row, or the merged row, copied using our allocator
     */
    private RowData applyRow(@Nullable RowData existing, RowData insert, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        if (existing == null)
        {
            // TODO: index update with the tail trie?
//            if (indexer != UpdateTransaction.NO_OP)
//                indexer.onInserted(insert.toRow(clusteringFor(keyState), DeletionTime.LIVE));

            this.dataSize += insert.dataSize();
            this.heapSize += insert.unsharedHeapSize();
            currentPartition.markInsertedRows(1);  // null pointer here means a problem in applyDeletion
            return insert;
        }
        else
        {
            RowData reconciled = RowData.merge(existing, insert);

            // TODO index update
//            if (indexer != UpdateTransaction.NO_OP)
//            {
//                Clustering<?> clustering = clusteringFor(keyState);
//                indexer.onUpdated(existing.toRow(clustering, DeletionTime.LIVE),
//                                  reconciled.toRow(clustering, DeletionTime.LIVE));
//            }

            if (reconciled != existing)
            {
                this.dataSize += reconciled.dataSize() - existing.dataSize();
                this.heapSize += reconciled.unsharedHeapSize() - existing.unsharedHeapSize();
            }
            return reconciled;
        }
    }

    private Cell<?> applyCell(@Nullable Cell<?> existing, Cell<?> update, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        if (existing == null)
        {
            this.dataSize += update.dataSize();
            this.heapSize += update.unsharedHeapSizeExcludingData();
            return update;
        }
        else
        {
            Cell<?> reconciled = Cells.reconcile(existing, update);
            if (reconciled != existing)
            {
                this.dataSize += reconciled.dataSize() - existing.dataSize();
                this.heapSize += reconciled.unsharedHeapSizeExcludingData() - existing.unsharedHeapSizeExcludingData();
            }
            return reconciled;
        }
        // TODO: index update?
    }

    private Clustering<?> clusteringFor(InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        return metadata.comparator.clusteringFromByteComparable(
            ByteArrayAccessor.instance,
            ByteComparable.preencoded(TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                      keyState.getBytes(TrieBackedPartition.IS_PARTITION_BOUNDARY)));
    }

    /**
     * Called at the partition boundary to merge the existing and new metadata associated with the partition. This needs
     * to make sure that the statistics we track for the partition (dataSize) are updated for the changes caused by
     * merging the update's rows.
     *
     * @param existing Any partition data already associated with the partition.
     * @return the combined partition data, creating a new marker if one did not already exist.
     */
    private PartitionData mergePartitionMarkers(@Nullable PartitionData existing)
    {
        // TODO: Check if we need to call onPartitionDeletion

        if (existing == null)
        {
            // Note: Always on-heap, regardless of cloner
            PartitionData newRef = new PartitionData(owner);
            this.heapSize += newRef.unsharedHeapSize();
            ++this.partitionsAdded;
            return currentPartition = newRef;
        }

        assert owner == existing.owner;
        return currentPartition = existing;
    }
}
