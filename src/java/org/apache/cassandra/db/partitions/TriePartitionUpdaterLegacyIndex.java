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
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
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
    private int currentPartitionDepth;

    public TriePartitionUpdaterLegacyIndex(TrieMemtable.MemtableShard owner,
                                           InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data,
                                           TableMetadata metadata)
    {
        super(owner, data);
        this.metadata = metadata;
    }

    public void startUpdate(UpdateTransaction indexer,
                            PartitionUpdate update)
    {
        super.startUpdate();
        this.indexer = indexer;
        this.rangeTombstoneOpenPosition = null;
        assert indexer != UpdateTransaction.NO_OP;

        DeletionTime partitionLevelDeletion = update.partitionLevelDeletion();
        if (!partitionLevelDeletion.isLive())
            indexer.onPartitionDeletion(partitionLevelDeletion);
    }

    @Override
    public TrieTombstoneMarker mergeMarkers(@Nullable TrieTombstoneMarker existing, TrieTombstoneMarker update)
    {
        // We should only report range and row tombstones.
        // Identifying row tombstones is easy, look for the level marker.
        if (update.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW))
        {
            Clustering<?> clustering = metadata.comparator.clusteringFromByteComparable(
                ByteArrayAccessor.instance,
                byteComparableForCurrentDeletionBranchKey());

            TrieBackedRow updateRow = TrieBackedRow.create(metadata,
                                                           clustering,
                                                           DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY,
                                                                                            TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                                                            mutator.getMutationDeletionTailTrie()));
            if (existing != null && existing.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW))
            {
                indexer.onUpdated(TrieBackedRow.create(metadata,
                                                       clustering,
                                                       DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY,
                                                                                        TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                                                        mutator.getExistingDeletionTailTrie())),
                                  updateRow);
            }
            else
                indexer.onInserted(updateRow);
        }
        else if (existing != null && existing.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW))
        {
            TrieTombstoneMarker.Covering rowDeletion = update.applicableToPointForward();
            if (rowDeletion != null)    // TODO: skip partition deletions?
            {
                Clustering<?> clustering = metadata.comparator.clusteringFromByteComparable(
                    ByteArrayAccessor.instance,
                    byteComparableForCurrentDeletionBranchKey());
                TrieBackedRow updateRow = TrieBackedRow.emptyDeletedRow(clustering, update.applicableToPointForward());
                indexer.onUpdated(TrieBackedRow.create(metadata,
                                                       clustering,
                                                       DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY,
                                                                                        TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                                                        mutator.getExistingDeletionTailTrie())),
                                  updateRow);
            }
        }
        else if (update.isBoundary())
        {
            // For range tombstones, we should only report when they start and stop. This means ignoring all switches
            // that include a lower-level change.
            TrieTombstoneMarker.Covering leftSide = update.leftDeletion();
            TrieTombstoneMarker.Covering rightSide = update.rightDeletion();
            boolean skip = false;

            if (leftSide != null)
                switch (leftSide.deletionKind())
                {
                    case ROW:
                        throw new AssertionError("Row deletion without row level marker");
                    case COLUMN:
                        skip = true;
                        break;
                    case PARTITION:
                        leftSide = null; // ignore this side
                        break;
                }

            if (rightSide != null)
                switch (rightSide.deletionKind())
                {
                    case ROW:
                        throw new AssertionError("Row deletion without row level marker");
                    case COLUMN:
                        skip = true;
                        break;
                    case PARTITION:
                        rightSide = null; // ignore this side
                        break;
                }

            if (!skip && (leftSide != null || rightSide != null))
            {
                if (rangeTombstoneOpenPosition != null)
                {
                    assert leftSide != null; // open markers are always closed
                    ClusteringBound<?> bound = metadata.comparator.boundFromByteComparable(
                        ByteArrayAccessor.instance,
                        byteComparableForCurrentDeletionBranchKey(),
                        true);
                    indexer.onRangeTombstone(new RangeTombstone(Slice.make(rangeTombstoneOpenPosition,
                                                                           bound),
                                                                leftSide));
                }
                else
                    assert leftSide == null;

                if (rightSide != null)
                {
                    rangeTombstoneOpenPosition = metadata.comparator.boundFromByteComparable(
                        ByteArrayAccessor.instance,
                        byteComparableForCurrentDeletionBranchKey(),
                        false);
                }
                else
                    rangeTombstoneOpenPosition = null;
            }
        }
        return super.mergeMarkers(existing, update);
    }

    @Override
    public Object applyRowDeletion(LivenessInfo existing, TrieTombstoneMarker updateMarker)
    {
        TrieTombstoneMarker.Covering rowDeletion = updateMarker.applicableToPointForward();
        if (rowDeletion == null)
            return existing; // there is no row deletion here

        Clustering<?> clustering = clusteringForCurrentKey();
        if (updateMarker.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW))
        {
            indexer.onUpdated(TrieBackedRow.create(metadata, clustering, mutator.getExistingTailTrie()),
                              TrieBackedRow.create(metadata, clustering, DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY,
                                                                                                          TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                                                                                          mutator.getMutationDeletionTailTrie())));
        }
        else
        {
            indexer.onUpdated(TrieBackedRow.create(metadata, clustering, mutator.getExistingTailTrie()),
                              TrieBackedRow.emptyDeletedRow(clustering, rowDeletion));
        }

        return super.applyRowDeletion(existing, updateMarker);
    }

    @Override
    protected PartitionData mergePartitionMarkers(@Nullable PartitionData existing)
    {
        currentPartitionDepth = mutator.currentDepth();
        return super.mergePartitionMarkers(existing);
    }

    /**
     * Called when a row needs to be copied to the Memtable trie.
     *
     * @param existing Existing LivenessInfo for this clustering, or null if there isn't any.
     * @param insert LivenessInfo to be inserted.
     * @return the insert row, or the merged row, copied using our allocator
     */
    @Override
    LivenessInfo applyRow(@Nullable LivenessInfo existing, LivenessInfo insert)
    {
        if (existing == null)
        {
            indexer.onInserted(TrieBackedRow.create(metadata, clusteringForCurrentKey(), mutator.getMutationTailTrie()));
        }
        else
        {
            Clustering<?> clustering = clusteringForCurrentKey();
            indexer.onUpdated(TrieBackedRow.create(metadata, clustering, mutator.getExistingTailTrie()),
                              TrieBackedRow.create(metadata, clustering, mutator.getMutationTailTrie()));
        }
        return super.applyRow(existing, insert);
    }

    private ByteComparable byteComparableForCurrentDeletionBranchKey()
    {
        return ByteComparable.preencoded(mutator.byteComparableVersion(),
                                         mutator.getDeletionBranchKeyBytes());
    }

    private Clustering<?> clusteringForCurrentKey()
    {
        return metadata.comparator.clusteringFromByteComparable(
            ByteArrayAccessor.instance,
            ByteComparable.preencoded(mutator.byteComparableVersion(),
                                      mutator.getCurrentKeyBytes(currentPartitionDepth)));
    }
}
