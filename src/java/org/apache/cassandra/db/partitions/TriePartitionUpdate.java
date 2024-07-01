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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.db.tries.InMemoryDTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Stores updates made on a partition. Immutable.
 * <p>
 * Provides factories for simple variations (e.g. singleRowUpdate) and a mutable builder for constructing one.
 * The builder holds a mutable trie to which content may be added in any order, also taking care of
 * merging any duplicate rows, and keeping track of statistics and column coverage.
 */
public class TriePartitionUpdate extends TrieBackedPartition implements PartitionUpdate
{
    protected static final Logger logger = LoggerFactory.getLogger(TriePartitionUpdate.class);

    public static final Factory FACTORY = new TrieFactory();

    private TriePartitionUpdate(TableMetadata metadata,
                                DecoratedKey key,
                                RegularAndStaticColumns columns,
                                EncodingStats stats,
                                Trie<Object> trie,
                                boolean canHaveShadowedData)
    {
        super(key, columns, stats, trie, metadata, canHaveShadowedData);
    }

    protected TriePartitionUpdate(TableMetadata metadata,
                                  DecoratedKey key)
    {
        this(metadata,
             key,
             RegularAndStaticColumns.NONE,
             EncodingStats.NO_STATS,
             newTrie(MutableDeletionInfo.live()),
             false);
    }

    private static InMemoryDTrie<Object> newTrie(DeletionInfo deletion)
    {
        InMemoryDTrie<Object> trie = InMemoryDTrie.shortLived();
        try
        {
            trie.putRecursive(ByteComparable.EMPTY, deletion, NO_CONFLICT_RESOLVER);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return trie;
    }

    /**
     * Creates a empty immutable partition update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the created update.
     *
     * @return the newly created empty (and immutable) update.
     */
    public static TriePartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey key)
    {
        return new TriePartitionUpdate(metadata, key);
    }

    /**
     * Creates an immutable partition update that entirely deletes a given partition.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition that the created update should delete.
     * @param timestamp the timestamp for the deletion.
     * @param nowInSec the current time in seconds to use as local deletion time for the partition deletion.
     *
     * @return the newly created partition deletion update.
     */
    public static TriePartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, long nowInSec)
    {
        return new TriePartitionUpdate(metadata,
                                       key,
                                       RegularAndStaticColumns.NONE,
                                       new EncodingStats(timestamp, nowInSec, LivenessInfo.NO_TTL),
                                       newTrie(new MutableDeletionInfo(timestamp, nowInSec)),
                                       false);
    }

    /**
     * Creates an immutable partition update that contains a single row update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition to update.
     * @param row the row for the update (may be null).
     * @param row the static row for the update (may be null).
     *
     * @return the newly created partition update containing only {@code row}.
     */
    public static TriePartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey key, Row row)
    {
        EncodingStats stats = EncodingStats.NO_STATS;   // As in trunk TODO: shouldn't this be EncodingStats.Collector.forRow(row)?
        InMemoryDTrie<Object> trie = newTrie(DeletionInfo.LIVE);

        RegularAndStaticColumns columns;
        if (row.isStatic())
            columns = new RegularAndStaticColumns(Columns.from(row.columns()), Columns.NONE);
        else
            columns = new RegularAndStaticColumns(Columns.NONE, Columns.from(row.columns()));

        try
        {
            putInTrie(metadata.comparator, useRecursive(metadata.comparator), trie, row);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }

        return new TriePartitionUpdate(metadata, key, columns, stats, trie, false);
    }

    /**
     * Creates an immutable partition update that contains a single row update.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition to update.
     * @param row the row for the update.
     *
     * @return the newly created partition update containing only {@code row}.
     */
    public static TriePartitionUpdate singleRowUpdate(TableMetadata metadata, ByteBuffer key, Row row)
    {
        return singleRowUpdate(metadata, metadata.partitioner.decorateKey(key), row);
    }

    /**
     * Turns the given iterator into an update.
     *
     * @param iterator the iterator to turn into updates.
     *
     * Warning: this method does not close the provided iterator, it is up to
     * the caller to close it.
     */
    @SuppressWarnings("resource")
    public static TriePartitionUpdate fromIterator(UnfilteredRowIterator iterator)
    {
        InMemoryDTrie<Object> trie = build(iterator);

        return new TriePartitionUpdate(iterator.metadata(),
                                       iterator.partitionKey(),
                                       iterator.metadata().regularAndStaticColumns(),
                                       collectStats(trie),
                                       trie,
                                       false);
    }

    /**
     * Turns the given iterator into an update.
     *
     * @param iterator the iterator to turn into updates.
     * @param filter the column filter used when querying {@code iterator}. This is used to make
     * sure we don't include data for which the value has been skipped while reading (as we would
     * then be writing something incorrect).
     *
     * Warning: this method does not close the provided iterator, it is up to
     * the caller to close it.
     */
    @SuppressWarnings("resource")
    public static TriePartitionUpdate fromIterator(UnfilteredRowIterator iterator, ColumnFilter filter)
    {
        return fromIterator(UnfilteredRowIterators.withOnlyQueriedData(iterator, filter));
    }

    public static TriePartitionUpdate asTrieUpdate(PartitionUpdate update)
    {
        return fromIterator(update.unfilteredIterator());
    }

    public static Trie<Object> asMergableTrie(PartitionUpdate update)
    {
        return asTrieUpdate(update).trie.prefix(update.partitionKey());
    }

    public TriePartitionUpdate withOnlyPresentColumns()
    {
        Set<ColumnMetadata> columnSet = new HashSet<>();

        // TODO: this was skipping static... recheck it's fine
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
        {
            Row row = it.next();
            for (ColumnData column : row)
                columnSet.add(column.column());
        }

        RegularAndStaticColumns columns = RegularAndStaticColumns.builder().addAll(columnSet).build();
        return new TriePartitionUpdate(this.metadata, this.partitionKey, columns, stats, trie, canHaveShadowedData);
    }

    protected boolean canHaveShadowedData()
    {
        return canHaveShadowedData;
    }

    /**
     * Creates a partition update that entirely deletes a given partition.
     *
     * @param metadata the metadata for the created update.
     * @param key the partition key for the partition that the created update should delete.
     * @param timestamp the timestamp for the deletion.
     * @param nowInSec the current time in seconds to use as local deletion time for the partition deletion.
     *
     * @return the newly created partition deletion update.
     */
    public static TriePartitionUpdate fullPartitionDelete(TableMetadata metadata, ByteBuffer key, long timestamp, long nowInSec)
    {
        return fullPartitionDelete(metadata, metadata.partitioner.decorateKey(key), timestamp, nowInSec);
    }

    /**
     * Merges the provided updates, yielding a new update that incorporates all those updates.
     *
     * @param updates the collection of updates to merge. This shouldn't be empty.
     *
     * @return a partition update that include (merge) all the updates from {@code updates}.
     */
    public PartitionUpdate merge(List<? extends PartitionUpdate> updates)
    {
        // TODO: Use tries to do these merges, and test their correctness.
        assert !updates.isEmpty();
        final int size = updates.size();

        if (size == 1)
            return Iterables.getOnlyElement(updates);

        List<UnfilteredRowIterator> asIterators = Lists.transform(updates, Partition::unfilteredIterator);
        return fromIterator(UnfilteredRowIterators.merge(asIterators), ColumnFilter.all(updates.get(0).metadata()));
    }

    /**
     * Modify this update to set every timestamp for live data to {@code newTimestamp} and
     * every deletion timestamp to {@code newTimestamp - 1}.
     *
     * There is no reason to use that except on the Paxos code path, where we need to ensure that
     * anything inserted uses the ballot timestamp (to respect the order of updates decided by
     * the Paxos algorithm). We use {@code newTimestamp - 1} for deletions because tombstones
     * always win on timestamp equality and we don't want to delete our own insertions
     * (typically, when we overwrite a collection, we first set a complex deletion to delete the
     * previous collection before adding new elements. If we were to set that complex deletion
     * to the same timestamp that the new elements, it would delete those elements). And since
     * tombstones always wins on timestamp equality, using -1 guarantees our deletion will still
     * delete anything from a previous update.
     */
    public TriePartitionUpdate withUpdatedTimestamps(long newTimestamp)
    {

        InMemoryDTrie<Object> t = InMemoryDTrie.shortLived();
        try
        {
            t.apply(trie, new InMemoryDTrie.UpsertTransformer<Object, Object>()
            {
                public Object apply(Object shouldBeNull, Object o)
                {
                    assert shouldBeNull == null;
                    if (o instanceof RowData)
                        return applyRowData((RowData) o);
                    else
                        return applyDeletion((DeletionInfo) o);
                }

                public RowData applyRowData(RowData update)
                {
                    LivenessInfo newInfo = update.livenessInfo.isEmpty()
                                           ? update.livenessInfo
                                           : update.livenessInfo.withUpdatedTimestamp(newTimestamp);
                    // If the deletion is shadowable and the row has a timestamp, we'll force the deletion timestamp to be less
                    // than the row one, so we should get rid of said deletion.
                    Row.Deletion newDeletion = update.deletion.isLive() || (update.deletion.isShadowable() && !update.livenessInfo.isEmpty())
                                               ? Row.Deletion.LIVE
                                               : new Row.Deletion(DeletionTime.build(newTimestamp - 1, update.deletion.time().localDeletionTime()),
                                                                  update.deletion.isShadowable());

                    Row updated = toRow(update, Clustering.EMPTY).updateAllTimestamp(newTimestamp);
                    return new RowData(((BTreeRow)updated).getBTree(), newInfo, newDeletion);
                }

                public DeletionInfo applyDeletion(DeletionInfo update)
                {
                    MutableDeletionInfo mdi = update.mutableCopy();
                    mdi.updateAllTimestamp(newTimestamp - 1);
                    return mdi;
                }
            }, x -> false);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return new TriePartitionUpdate(metadata, partitionKey, columns, stats, t, canHaveShadowedData);
    }

    /**
     * The number of "operations" contained in the update.
     * <p>
     * This is used by {@code Memtable} to approximate how much work this update does. In practice, this
     * count how many rows are updated and how many ranges are deleted by the partition update.
     *
     * @return the number of "operations" performed by the update.
     */
    public int operationCount()
    {
        return rowCount()
             + (staticRow().isEmpty() ? 0 : 1)
             + deletionInfo().rangeCount()
             + (deletionInfo().getPartitionDeletion().isLive() ? 0 : 1);
    }

    /**
     * The size of the data contained in this update.
     *
     * @return the size of the data contained in this update.
     */
    public int dataSize()
    {
        long size = 0;
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
        {
            Row row = it.next();
            size += row.dataSize() + row.clustering().dataSize();
        }

        return Ints.saturatedCast(size + deletionInfo().dataSize());
    }

    /**
     * The size of the data contained in this update.
     *
     * @return the size of the data contained in this update.
     */
    public long unsharedHeapSize()
    {
        long size = 0;
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
        {
            Row row = it.next();
            size += row.unsharedHeapSize() + row.clustering().unsharedHeapSize();
        }

        return size + deletionInfo().unsharedHeapSize();
    }

    /**
     * Validates the data contained in this update.
     *
     * @throws org.apache.cassandra.serializers.MarshalException if some of the data contained in this update is corrupted.
     */
    public void validate()
    {
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
        {
            Row row = it.next();
            metadata().comparator.validate(row.clustering());
            for (ColumnData cd : row)
                cd.validate();
        }
    }

    /**
     * The maximum timestamp used in this update.
     *
     * @return the maximum timestamp used in this update.
     */
    public long maxTimestamp()
    {
        long maxTimestamp = deletionInfo().maxTimestamp();
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
        {
            Row row = it.next();
            maxTimestamp = Math.max(maxTimestamp, row.primaryKeyLivenessInfo().timestamp());
            for (ColumnData cd : row)
            {
                if (cd.column().isSimple())
                {
                    maxTimestamp = Math.max(maxTimestamp, ((Cell<?>)cd).timestamp());
                }
                else
                {
                    ComplexColumnData complexData = (ComplexColumnData)cd;
                    maxTimestamp = Math.max(maxTimestamp, complexData.complexDeletion().markedForDeleteAt());
                    for (Cell<?> cell : complexData)
                        maxTimestamp = Math.max(maxTimestamp, cell.timestamp());
                }
            }
        }
        return maxTimestamp;
    }

    private static EncodingStats collectStats(InMemoryDTrie<Object> trie)
    {
        EncodingStats.Collector statsCollector = new EncodingStats.Collector();

        for (Object o : trie.values())
        {
            if (o instanceof DeletionInfo)
                ((DeletionInfo) o).collectStats(statsCollector);
            else
                Rows.collectStats(((RowData) o).toRow(Clustering.EMPTY), statsCollector);
        }

        return statsCollector.get();
    }

    /**
     * For an update on a counter table, returns a list containing a {@code CounterMark} for
     * every counter contained in the update.
     *
     * @return a list with counter marks for every counter in this update.
     */
    public List<CounterMark> collectCounterMarks()
    {
        assert metadata().isCounter();
        // We will take aliases on the rows of this update, and update them in-place. So we should be sure the
        // update is now immutable for all intent and purposes.
        List<CounterMark> marks = new ArrayList<>();
        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
        {
            Row row = it.next();
            addMarksForRow(row, marks);
        }
        return marks;
    }

    /**
     *
     * @return the estimated number of rows affected by this mutation 
     */
    public int affectedRowCount()
    {
        // If there is a partition-level deletion, we intend to delete at least one row.
        if (!partitionLevelDeletion().isLive())
            return 1;

        int count = 0;

        // Each range delete should correspond to at least one intended row deletion.
        if (deletionInfo().hasRanges())
            count += deletionInfo().rangeCount();

        count += rowCount();

        if (!staticRow().isEmpty())
            count++;

        return count;
    }

    /**
     *
     * @return the estimated total number of columns that either have live data or are covered by a delete
     */
    public int affectedColumnCount()
    {
        // If there is a partition-level deletion, we intend to delete at least the columns of one row.
        if (!partitionLevelDeletion().isLive())
            return metadata().regularAndStaticColumns().size();

        int count = 0;

        // Each range delete should correspond to at least one intended row deletion, and with it, its regular columns.
        if (deletionInfo().hasRanges())
            count += deletionInfo().rangeCount() * metadata().regularColumns().size();

        for (Iterator<Row> it = rowsIncludingStatic(); it.hasNext();)
        {
            Row row = it.next();
            if (row.deletion().isLive())
                // If the row is live, this will include simple tombstones as well as cells w/ actual data. 
                count += row.columnCount();
            else
                // We have a row deletion, so account for the columns that might be deleted.
                count += metadata().regularColumns().size();
        }

        return count;
    }

    private static void addMarksForRow(Row row, List<CounterMark> marks)
    {
        for (Cell<?> cell : row.cells())
        {
            if (cell.isCounterCell())
                marks.add(new CounterMark(row, cell.column(), cell.path()));
        }
    }

    /**
     * Builder for PartitionUpdates
     *
     * This class is not thread safe, but the PartitionUpdate it produces is (since it is immutable).
     */
    public static class Builder implements PartitionUpdate.Builder
    {
        private final TableMetadata metadata;
        private final DecoratedKey key;
        private final MutableDeletionInfo deletionInfo;
        private final boolean canHaveShadowedData;
        private final RegularAndStaticColumns columns;
        private final InMemoryDTrie<Object> trie = InMemoryDTrie.shortLived();
        private final EncodingStats.Collector statsCollector = new EncodingStats.Collector();
        private final boolean useRecursive;

        public Builder(TableMetadata metadata,
                       DecoratedKey key,
                       RegularAndStaticColumns columns)
        {
            this(metadata, key, columns, true, Rows.EMPTY_STATIC_ROW, DeletionInfo.LIVE);
        }

        private Builder(TableMetadata metadata,
                        DecoratedKey key,
                        RegularAndStaticColumns columns,
                        boolean canHaveShadowedData,
                        Row staticRow,
                        DeletionInfo deletionInfo)
        {
            this.metadata = metadata;
            this.key = key;
            this.columns = columns;
            this.canHaveShadowedData = canHaveShadowedData;
            this.deletionInfo = deletionInfo.mutableCopy();
            useRecursive = useRecursive(metadata.comparator);
            add(staticRow);
        }

        // This is wasteful, only to be used for testing.
        @VisibleForTesting
        public Builder(TriePartitionUpdate base)
        {
            this(base.metadata, base.partitionKey, base.columns(), base.canHaveShadowedData, Rows.EMPTY_STATIC_ROW, base.deletionInfo());
            for (Iterator<Row> it = base.rowsIncludingStatic(); it.hasNext();)
                add(it.next());
        }

        /**
         * Adds a row to this update.
         * <p>
         * There is no particular assumption made on the order of row added to a partition update. It is further
         * allowed to add the same row (more precisely, multiple row objects for the same clustering).
         * <p>
         * Note however that the columns contained in the added row must be a subset of the columns used when
         * creating this update.
         *
         * @param row the row to add.
         */
        public void add(Row row)
        {
            if (row.isEmpty())
                return;

            // this assert is expensive, and possibly of limited value; we should consider removing it
            // or introducing a new class of assertions for test purposes
            assert (row.isStatic() ? columns().statics : columns().regulars).containsAll(row.columns())
            : (row.isStatic() ? columns().statics : columns().regulars) + " is not superset of " + row.columns();

            try
            {
                trie.putSingleton(metadata.comparator.asByteComparable(row.clustering()),
                                  row,
                                  this::merge,
                                  useRecursive);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
            Rows.collectStats(row, statsCollector);
        }

        public void addPartitionDeletion(DeletionTime deletionTime)
        {
            deletionInfo.add(deletionTime);
        }

        public void add(RangeTombstone range)
        {
            deletionInfo.add(range, metadata.comparator);
        }

        public DecoratedKey partitionKey()
        {
            return key;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public TriePartitionUpdate build()
        {
            try
            {
                trie.putRecursive(ByteComparable.EMPTY, deletionInfo, NO_CONFLICT_RESOLVER);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
            deletionInfo.collectStats(statsCollector);
            TriePartitionUpdate pu = new TriePartitionUpdate(metadata,
                                                             partitionKey(),
                                                             columns,
                                                             statsCollector.get(),
                                                             trie,
                                                             canHaveShadowedData);

            return pu;
        }

        RowData merge(Object existing, Row update)
        {
            if (existing != null)
                update = Rows.merge(((RowData) existing).toRow(update.clustering()), update);

            return rowToData(update);
        }

        public RegularAndStaticColumns columns()
        {
            return columns;
        }

        public DeletionTime partitionLevelDeletion()
        {
            return deletionInfo.getPartitionDeletion();
        }

        @Override
        public String toString()
        {
            return "Builder{" +
                   "metadata=" + metadata +
                   ", key=" + key +
                   ", deletionInfo=" + deletionInfo +
                   ", canHaveShadowedData=" + canHaveShadowedData +
                   ", columns=" + columns +
                   '}';
        }
    }

    public static class TrieFactory implements PartitionUpdate.Factory
    {

        @Override
        public PartitionUpdate.Builder builder(TableMetadata metadata, DecoratedKey partitionKey, RegularAndStaticColumns columns, int initialRowCapacity)
        {
            return new TriePartitionUpdate.Builder(metadata, partitionKey, columns);
        }

        @Override
        public PartitionUpdate emptyUpdate(TableMetadata metadata, DecoratedKey partitionKey)
        {
            return TriePartitionUpdate.emptyUpdate(metadata, partitionKey);
        }

        @Override
        public PartitionUpdate singleRowUpdate(TableMetadata metadata, DecoratedKey valueKey, Row row)
        {
            return TriePartitionUpdate.singleRowUpdate(metadata, valueKey, row);
        }

        @Override
        public PartitionUpdate fullPartitionDelete(TableMetadata metadata, DecoratedKey key, long timestamp, long nowInSec)
        {
            return TriePartitionUpdate.fullPartitionDelete(metadata, key, timestamp, nowInSec);
        }

        @Override
        public PartitionUpdate fromIterator(UnfilteredRowIterator iterator)
        {
            return TriePartitionUpdate.fromIterator(iterator);
        }

        @Override
        public PartitionUpdate fromIterator(UnfilteredRowIterator iterator, ColumnFilter filter)
        {
            return TriePartitionUpdate.fromIterator(iterator, filter);
        }
    }
}
