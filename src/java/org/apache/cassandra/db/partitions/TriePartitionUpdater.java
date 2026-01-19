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

import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.rows.Cells;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieCellData;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryBaseTrie;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;

import static org.apache.cassandra.db.memtable.TrieMemtable.PartitionData;

/**
 *  The function we provide to the trie utilities to perform any partition and row inserts and updates
 */
public class TriePartitionUpdater
implements InMemoryBaseTrie.UpsertTransformerWithKeyProducer<Object, Object>
{
    protected final TrieMemtable.MemtableShard owner;
    public final InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator;

    public long dataSize;
    public long colUpdateTimeDelta;

    protected PartitionData currentPartition;
    public int partitionsAdded;

    public TriePartitionUpdater(TrieMemtable.MemtableShard owner,
                                InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        this.owner = owner;
        this.mutator = data.mutator(this,
                                    this::mergeMarkers,
                                    this::applyMarker,
                                    this::applyMarker,
                                    true,
                                    TrieMemtable.FORCE_COPY_PARTITION_BOUNDARY,
                                    x -> { throw new AssertionError("Force copy should already be in effect for all range tries"); });
    }

    public void startUpdate()
    {
        this.currentPartition = null;
        this.partitionsAdded = 0;
        this.dataSize = 0;
        this.colUpdateTimeDelta = Long.MAX_VALUE;
    }

    @Override
    public Object apply(@Nullable Object existing, Object update, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        if (update instanceof CellData)
            return applyCell((TrieCellData) existing, (CellData<?>) update, keyState);
        else if (update == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return update; // TODO check if something else needs to be done
        else if (update instanceof LivenessInfo)
            return applyRow((LivenessInfo) existing, (LivenessInfo) update, keyState);
        else if (update == TrieBackedPartition.PARTITION_MARKER)
            return mergePartitionMarkers((PartitionData) existing);
        else
            throw new AssertionError("Unexpected update type: " + update.getClass());
    }

    public TrieTombstoneMarker mergeMarkers(@Nullable TrieTombstoneMarker existing, TrieTombstoneMarker update, InMemoryBaseTrie.KeyProducer<TrieTombstoneMarker> keyState)
    {
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

    public Object applyMarker(Object existingContent, TrieTombstoneMarker updateMarker, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        // Most common case first
        if (existingContent instanceof CellData)
            return applyCellDeletion((CellData) existingContent, updateMarker);
        else if (existingContent == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return existingContent;
        else if (existingContent instanceof LivenessInfo)
            return applyRowDeletion((LivenessInfo) existingContent, updateMarker, keyState);
        else if (existingContent instanceof PartitionData)
            return applyPartitionDeletion((PartitionData) existingContent, updateMarker);
        else
            throw new AssertionError("Unexpected content in trie " + existingContent + " for deletion " + updateMarker);
    }

    private CellData applyCellDeletion(CellData existingContent, TrieTombstoneMarker updateMarker)
    {
        if (!updateMarker.applicableToPointForward().deletes(existingContent))
            return existingContent;
        dataSize -= existingContent.valueSize();
        return null;
    }

    public Object applyPartitionDeletion(PartitionData existing, TrieTombstoneMarker unused)
    {
        existing.clearStats();
        return existing;
    }

    public Object applyRowDeletion(LivenessInfo existing, TrieTombstoneMarker updateMarker, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        TrieTombstoneMarker.Covering rowDeletion = updateMarker.succedingState(Direction.FORWARD);
        if (rowDeletion == null)
            return existing; // there is no row deletion here

        if (rowDeletion.deletes(existing))
        {
            return LivenessInfo.EMPTY;
            // TODO: and also do currentPartition.markInsertedRows(-1) in that case?
            // TODO: Does strict row liveness apply here? How do we drop tail trie if it does?
        }
        return existing;
    }

    public Object applyMarker(TrieTombstoneMarker marker, Object content)
    {
        // This is called to apply an existing tombstone to incoming data, before applyRow is called on the result.
        // No size tracking is needed, because the result of this then gets applied to the trie with applyRow.
        if (content instanceof Cell)
            return marker.applicableToPointForward().deletes((Cell<?>) content) ? null : content;
        else if (content == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return content;
        else if (content instanceof LivenessInfo)
        {
            TrieTombstoneMarker.Covering rowDeletion = marker.succedingState(Direction.FORWARD);
            if (rowDeletion == null || !rowDeletion.deletes((LivenessInfo) content))
                return content;
            else
                return LivenessInfo.EMPTY;
        }
        else if (content instanceof PartitionData)
            return content;
        else
            throw new AssertionError("Unexpected content in trie " + content + " for deletion " + marker);
    }

    /**
     * Called when a row needs to be copied to the Memtable trie.
     *
     * @param existing Existing LivenessInfo for this clustering, or null if there isn't any.
     * @param insert LivenessInfo to be inserted.
     * @param keyState Used to obtain the path through which this node was reached.
     * @return the insert row, or the merged row, copied using our allocator
     */
    LivenessInfo applyRow(@Nullable LivenessInfo existing, LivenessInfo insert, InMemoryBaseTrie.KeyProducer<Object> keyState)
    {
        if (existing == null)
        {
            this.dataSize += insert.dataSize();
            currentPartition.markInsertedRows(1);  // null pointer here means a problem in applyDeletion
            return insert;
        }
        else
        {
            LivenessInfo reconciled = LivenessInfo.merge(existing, insert);
            if (reconciled != existing)
                this.dataSize += reconciled.dataSize() - existing.dataSize();

            return reconciled;
        }
    }

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
        if (existing == null)
        {
            PartitionData newRef = new PartitionData(owner);
            ++this.partitionsAdded;
            return currentPartition = newRef;
        }

        assert owner == existing.owner;
        return currentPartition = existing;
    }
}
