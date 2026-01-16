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

import com.google.common.base.Predicates;

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
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieCellData;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryBaseTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.memtable.TrieMemtable.PartitionData;

/**
 *  The function we provide to the trie utilities to perform any partition and row inserts and updates
 */
public final class TriePartitionUpdaterLegacyIndex extends TriePartitionUpdater
{
    private UpdateTransaction indexer;
    private TableMetadata metadata;
    private ClusteringBound<byte[]> rangeTombstoneOpenPosition;
    private DeletionTime partitionLevelDeletion;

    public TriePartitionUpdaterLegacyIndex(TrieMemtable.MemtableShard owner,
                                           InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        super(owner, data);
    }

    public void startUpdate(UpdateTransaction indexer,
                            PartitionUpdate update,
                            TableMetadata metadata)
    {
        super.startUpdate();
        this.indexer = indexer;
        this.metadata = metadata;
        this.rangeTombstoneOpenPosition = null;
        assert indexer != UpdateTransaction.NO_OP;

        this.partitionLevelDeletion = update.partitionLevelDeletion();
        if (!partitionLevelDeletion.isLive())
            indexer.onPartitionDeletion(partitionLevelDeletion);
    }

    @Override
    public TrieTombstoneMarker mergeMarkers(@Nullable TrieTombstoneMarker existing, TrieTombstoneMarker update, InMemoryBaseTrie.KeyProducer<TrieTombstoneMarker> keyState)
    {
        if (indexer != UpdateTransaction.NO_OP)
        {
            if (update.hasPointData())
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
            return update;
        }
        else
        {
            TrieTombstoneMarker merged = update.mergeWith(existing);
            return merged;
        }
    }

    @Override
    public Object applyRowDeletion(LivenessInfo existing, TrieTombstoneMarker updateMarker, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        TrieTombstoneMarker rowDeletion = updateMarker.succedingState(Direction.FORWARD);
        if (rowDeletion == null)
            return existing; // there is no row deletion here

        if (rowDeletion.deletionTime().deletes(existing))
        {
            return LivenessInfo.EMPTY;
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

    /**
     * Called when a row needs to be copied to the Memtable trie.
     *
     * @param existing Existing LivenessInfo for this clustering, or null if there isn't any.
     * @param insert LivenessInfo to be inserted.
     * @param keyState Used to obtain the path through which this node was reached.
     * @return the insert row, or the merged row, copied using our allocator
     */
    @Override
    LivenessInfo applyRow(@Nullable LivenessInfo existing, LivenessInfo insert, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        if (existing == null)
        {
            // TODO: index update with the tail trie?
//            if (indexer != UpdateTransaction.NO_OP)
//                indexer.onInserted(insert.toRow(clusteringFor(keyState), DeletionTime.LIVE));

            this.dataSize += insert.dataSize();
            currentPartition.markInsertedRows(1);  // null pointer here means a problem in applyDeletion
            return insert;
        }
        else
        {
            LivenessInfo reconciled = LivenessInfo.merge(existing, insert);

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
            }
            return reconciled;
        }
    }

    @Override
    CellData applyCell(@Nullable TrieCellData existing, CellData<?> update, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        if (existing == null)
        {
            this.dataSize += update.valueSize();
            return update;
        }
        else
        {
            CellData reconciled = Cells.reconcile(existing, update);
            if (reconciled != existing)
            {
                long timeDelta = Math.abs(reconciled.timestamp() - existing.timestamp());
                if (timeDelta < colUpdateTimeDelta)
                    colUpdateTimeDelta = timeDelta;
                this.dataSize += reconciled.valueSize() - existing.valueSize();
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
}
