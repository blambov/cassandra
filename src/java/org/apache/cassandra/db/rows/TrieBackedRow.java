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

import java.nio.ByteBuffer;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;

import org.agrona.collections.Object2IntHashMap;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.MultiCellCapableType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.RangeTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieEntriesIterator;
import org.apache.cassandra.db.tries.TrieSet;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.db.tries.TrieTailsIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.cassandra.utils.memory.Cloner;

import static org.apache.cassandra.db.partitions.TrieBackedPartition.BYTE_COMPARABLE_VERSION;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.mergeTombstoneRanges;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.noConflictInData;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.noExistingSelfDeletion;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.noIncomingSelfDeletion;

/**
 * Immutable implementation of a Row object.
 */
public class TrieBackedRow extends AbstractRow
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(emptyRow(Clustering.EMPTY));
    private static final int COLUMN_NOT_PRESENT = -1;

    public static final Object COMPLEX_COLUMN_MARKER = new Object()
    {
        @Override
        public String toString()
        {
            return "COMPLEX_COLUMN_MARKER";
        }
    };

    private final Clustering<?> clustering;
    private final Object2IntHashMap<ColumnIdentifier> columnIds;
    private final Columns columns;

    // We need to filter the tombstones of a row on every read (twice in fact: first to remove purgeable tombstone, and then after reconciliation to remove
    // all tombstone since we don't return them to the client) as well as on compaction. But it's likely that many rows won't have any tombstone at all, so
    // we want to speed up that case by not having to iterate/copy the row in this case. We could keep a single boolean telling us if we have tombstones,
    // but that doesn't work for expiring columns. So instead we keep the deletion time for the first thing in the row to be deleted. This allow at any given
    // time to know if we have any deleted information or not. If we any "true" tombstone (i.e. not an expiring cell), this value will be forced to
    // Integer.MIN_VALUE, but if we don't and have expiring cells, this will the time at which the first expiring cell expires. If we have no tombstones and
    // no expiring cells, this will be Integer.MAX_VALUE;
    private int minLocalDeletionTime;
    boolean minLocalDeletionTimeSet = false;

    ///  Data trie contains:
    ///  - LivenessInfo header at the root
    ///  - A Cell for each (simple or complex) cell of the row
    ///    - Cells may be expiring or even expired (not really expected for memtables but possible)
    ///  - Deletion branch with tombstones
    private final DeletionAwareTrie<Object, TrieTombstoneMarker> data;

    public static TrieBackedRow from(TableMetadata metadata, Row row)
    {
        Builder builder = builder(metadata, row.clustering());
        for (ColumnData cd : row)
        {
            if (cd.column.isSimple())
                builder.addCell((Cell<?>) cd);
            else
            {
                var ccd = (ComplexColumnData) cd;
                builder.addComplexDeletion(ccd.column, ccd.complexDeletion());
                for (Cell<?> cell : ccd)
                    builder.addCell(cell);
            }
        }
        return builder.build();
    }

    private static final Map<Columns, Object2IntHashMap<ColumnIdentifier>> columnsMapCache = new HashMap<>();

    public static TrieBackedRow create(TableMetadata tableMetadata, Clustering<?> clustering, DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        return new TrieBackedRow(tableMetadata, clustering, data);
    }

    TrieBackedRow(TableMetadata tableMetadata, Clustering<?> clustering, DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        this(tableMetadata.regularAndStaticColumns().columns(clustering == Clustering.STATIC_CLUSTERING), clustering, data);
    }

    TrieBackedRow(Columns columns, Clustering<?> clustering, DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        this(columns, columnsMapCache.computeIfAbsent(columns, TrieBackedRow::makeColumnIdsMap), clustering, data);
    }

    private TrieBackedRow(Columns columns,
                          Object2IntHashMap<ColumnIdentifier> columnIds,
                          Clustering<?> clustering,
                          DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        this.columns = columns;
        this.columnIds = columnIds;
        this.clustering = clustering;
        this.data = data;
    }

    private static Object2IntHashMap<ColumnIdentifier> makeColumnIdsMap(Columns columns)
    {
        Object2IntHashMap<ColumnIdentifier> columnIds = new Object2IntHashMap<>(COLUMN_NOT_PRESENT);
        for (int i = 0; i < columns.size(); i++)
            columnIds.put(columns.getSimple(i).name, i);
        return columnIds;
    }

    public static TrieBackedRow createLive(Columns columns,
                                           Object2IntHashMap<ColumnIdentifier> columnIds,
                                           Clustering clustering,
                                           LivenessInfo livenessInfo,
                                           InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data) throws TrieSpaceExhaustedException
    {
        data.putRecursive(ByteComparable.EMPTY, (livenessInfo), noConflictInData());

        return new TrieBackedRow(columns, columnIds, clustering, data);
    }

    static final DeletionAwareTrie<Object, TrieTombstoneMarker> EMPTY_ROW = DeletionAwareTrie.singleton(ByteComparable.EMPTY,
                                                                                                        BYTE_COMPARABLE_VERSION,
                                                                                                        LivenessInfo.EMPTY);
    static final Object2IntHashMap<ColumnIdentifier> EMPTY_COLUMN_IDS = makeColumnIdsMap(Columns.NONE);

    public static TrieBackedRow emptyRow(Clustering<?> clustering)
    {
        return new TrieBackedRow(Columns.NONE, EMPTY_COLUMN_IDS, clustering, EMPTY_ROW);
    }

    public static TrieBackedRow singleCellRow(Clustering<?> clustering, Cell<?> cell)
    {
        try
        {
            Columns columns = Columns.of(cell.column);
            Object2IntHashMap<ColumnIdentifier> columnIds = makeColumnIdsMap(columns);
            InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie = InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION);
            ByteComparable cellKey = cellKey(columnIds, cell);
            if (cell.column.isComplex())
                trie.putRecursive(columnKey(columnIds, cell.column), COMPLEX_COLUMN_MARKER, noConflictInData());
            trie.putRecursive(cellKey, cell, noConflictInData());
            return createLive(columns, columnIds, clustering, LivenessInfo.EMPTY, trie);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    public static TrieBackedRow emptyDeletedRow(Clustering<?> clustering, DeletionTime deletion)
    {
        try
        {
            InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie = InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION);
            // We need to put the deletion as well as a deletion-path row marker.
            RangeTrie<TrieTombstoneMarker> deletionTrie = rowDeletionTrie(deletion);

            trie.apply(DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY,
                                                        BYTE_COMPARABLE_VERSION,
                                                        deletionTrie),
                       noConflictInData(),
                       mergeTombstoneRanges(),
                       noIncomingSelfDeletion(),
                       TrieBackedPartition.noExistingSelfDeletion(),
                       true,
                       x -> false);
            return new TrieBackedRow(Columns.NONE, EMPTY_COLUMN_IDS, clustering, trie);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    private static RangeTrie<TrieTombstoneMarker> rowDeletionTrie(DeletionTime deletion)
    {
        return deletionTrie(ByteComparable.EMPTY, deletion);
    }

    private static RangeTrie<TrieTombstoneMarker> deletionTrie(ByteComparable prefix, DeletionTime deletion)
    {
        return withDeletionRoot(RangeTrie.branch(prefix,
                                                 BYTE_COMPARABLE_VERSION,
                                                 TrieTombstoneMarker.covering(deletion)),
                                deletion);
    }

    private static RangeTrie<TrieTombstoneMarker> withDeletionRoot(RangeTrie<TrieTombstoneMarker> trie, DeletionTime deletion)
    {
        return trie.mergeWith(RangeTrie.point(ByteComparable.EMPTY,
                                              BYTE_COMPARABLE_VERSION,
                                              true,
                                              TrieTombstoneMarker.point(TrieTombstoneMarker.PointDataType.ROW, deletion)),
                              TrieTombstoneMarker::mergeWith);
    }

    public static TrieBackedRow noCellLiveRow(Clustering<?> clustering, LivenessInfo primaryKeyLivenessInfo)
    {
        assert !primaryKeyLivenessInfo.isEmpty();
        try
        {
            return createLive(Columns.NONE,
                              EMPTY_COLUMN_IDS,
                              clustering,
                              primaryKeyLivenessInfo,
                              InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION));
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    private static int minDeletionTime(Cell<?> cell)
    {
        return cell.isTombstone() ? Integer.MIN_VALUE : cell.localDeletionTime();
    }

    private static int minDeletionTime(LivenessInfo info)
    {
        return info.isExpiring() ? info.localExpirationTime() : Integer.MAX_VALUE;
    }

    private static int minDeletionTime(DeletionTime dt)
    {
        return dt.isLive() ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }

    private static int minDeletionTime(TrieTombstoneMarker marker)
    {
        assert !marker.deletionTime().isLive();
        return Integer.MIN_VALUE;
    }

    static class Accumulator implements DeletionAwareTrie.ValueConsumer<Object, TrieTombstoneMarker>
    {
        final LongAccumulator<Cell<?>> cellAccumulator;
        final LongAccumulator<LivenessInfo> livenessAccumulator;
        final LongAccumulator<DeletionTime> markerAccumulator;
        long value;

        Accumulator(long initialValue,
                    LongAccumulator<Cell<?>> cellAccumulator,
                    LongAccumulator<LivenessInfo> livenessAccumulator,
                    LongAccumulator<DeletionTime> markerAccumulator)
        {
            this.cellAccumulator = cellAccumulator;
            this.livenessAccumulator = livenessAccumulator;
            this.markerAccumulator = markerAccumulator;
            this.value = initialValue;
        }

        @Override
        public void content(Object content)
        {
            if (content instanceof LivenessInfo)
                value = livenessAccumulator.apply((LivenessInfo) content, value);
            else if (content instanceof Cell)
                value = cellAccumulator.apply((Cell<?>) content, value);
            else if (content != COMPLEX_COLUMN_MARKER)
                throw new AssertionError("Unexpected content type: " + content);
        }

        @Override
        public void deletionMarker(TrieTombstoneMarker marker)
        {
            if (marker.hasPointData(TrieTombstoneMarker.PointDataType.ROW))
                value = markerAccumulator.apply(marker.deletionTime(), value); // Covering deletion will be seen as a boundary.
            else if (marker.isBoundary())
            {
                // We only apply the function to one side of the marker; the other has to be already be seen as a
                // succeeding side of a different marker.
                TrieTombstoneMarker succedingState = marker.succedingState(Direction.FORWARD);
                if (succedingState != null)
                    value = markerAccumulator.apply(succedingState.deletionTime(), value);
            }
        }
    }

    @Override
    public long accumulate(LongAccumulator<ColumnData> accumulator, long initialValue)
    {
        // TODO: this isn't efficient at all
        long v = initialValue;
        for (ColumnData c : this)
            v = accumulator.apply(c, v);
        return v;
    }

    @Override
    public <A> long accumulate(BiLongAccumulator<A, ColumnData> accumulator, A arg, long initialValue)
    {
        // TODO: this isn't efficient at all
        long v = initialValue;
        for (ColumnData c : this)
            v = accumulator.apply(arg, c, v);
        return v;
    }

    long accumulate(long initialValue,
                    LongAccumulator<LivenessInfo> livenessAccumulator,
                    LongAccumulator<Cell<?>> cellAccumulator,
                    LongAccumulator<DeletionTime> markerAccumulator)
    {
        // Note: this does not provide cell paths
        Accumulator accumulator = new Accumulator(initialValue, cellAccumulator, livenessAccumulator, markerAccumulator);
        data.process(Direction.FORWARD, accumulator);
        return accumulator.value;
    }

    /**
     * Computes the maximum timestamp for any data (deletion info, PK liveness or cell) in this row.
     */
    public long maxTimestamp()
    {
        return accumulate(Long.MIN_VALUE,
                          (livenessInfo, maxTimestamp) -> Math.max(maxTimestamp, livenessInfo.timestamp()),
                          (cell, maxTimestamp) -> Math.max(maxTimestamp, cell.timestamp()),
                          (marker, maxTimestamp) -> Math.max(maxTimestamp, marker.markedForDeleteAt()));
    }

    /**
     * Computes the minimum timestamp for any data (deletion info, PK liveness or cell) in this row.
     */
    public long minTimestamp()
    {
        return accumulate(Long.MAX_VALUE,
                          (livenessInfo, minTimestamp) -> Math.min(minTimestamp, livenessInfo.timestamp()),
                          (cell, minTimestamp) -> Math.min(minTimestamp, cell.timestamp()),
                          (marker, minTimestamp) -> Math.min(minTimestamp, marker.markedForDeleteAt()));
    }

    public Clustering<?> clustering()
    {
        return clustering;
    }

    public LivenessInfo primaryKeyLivenessInfo()
    {
        LivenessInfo info = (LivenessInfo) data.get(ByteComparable.EMPTY);
        return info != null ? info : LivenessInfo.EMPTY;
    }

    public boolean isEmpty()
    {
        // Empty has no live or deletion branch.
        // TODO: make a garbage-free method for this
        return !data.contentOnlyTrie().valueIterator().hasNext() &&
               !data.deletionOnlyTrie().valueIterator().hasNext();
    }

    public boolean isEmptyAfterDeletion()
    {
        // TODO: should we return false for deletion-branch column deletions?
        return !data.contentOnlyTrie().valueIterator().hasNext();
    }

    public Deletion deletion()
    {
        TrieTombstoneMarker marker = data.applicableDeletion(ByteComparable.EMPTY);
        if (marker == null)
            return Deletion.LIVE;
        return Deletion.regular(marker.deletionTime());
    }

    static ByteComparable cellKey(Object2IntHashMap<ColumnIdentifier> columnIds, Cell<?> cell)
    {
        ColumnMetadata column = cell.column;
        return cellKey(columnIds, column, cell.path());
    }

    private static ByteComparable cellKey(Object2IntHashMap<ColumnIdentifier> columnIds, ColumnMetadata column, CellPath path)
    {
        int id = columnIds.get(column.name);
        assert id != COLUMN_NOT_PRESENT;
        if (!column.isComplex())
            return v -> ByteSource.variableLengthInteger(id);
        else
            return cellPath(id, column, path);
    }

    private static ByteSource columnIdPrefix(int columnId)
    {
        if (columnId < 0)
            return ByteSource.EMPTY;
        else
            return ByteSource.variableLengthInteger(columnId);
    }

    static ByteComparable cellPath(int columnId, ColumnMetadata column, CellPath path)
    {
        if (path == CellPath.BOTTOM)
            return v -> ByteSource.concat(columnIdPrefix(columnId),
                                          ByteSource.oneByte(ByteSource.LT_NEXT_COMPONENT));
        else if (path == CellPath.TOP)
            return v -> ByteSource.concat(columnIdPrefix(columnId),
                                          ByteSource.oneByte(ByteSource.GT_NEXT_COMPONENT));
        else
        return v -> ByteSource.concat(columnIdPrefix(columnId),
                                      ByteSource.withTerminator(ByteSource.TERMINATOR,
                                                                getCellPathType(column).asComparableBytes(path.get(0), v)
                                      ));
        // TODO: figure out a better way to do path slices and remove the leading path byte as
//        return v -> ByteSource.concat(columnIdPrefix(columnId),
//                                      ((MultiCellCapableType<Object>)column.type).nameComparator().asComparableBytes(path.get(0), v),
//                                      ByteSource.oneByte(ByteSource.TERMINATOR));
    }

    private static AbstractType<?> getCellPathType(ColumnMetadata column)
    {
        assert column.isComplex();
        return ((MultiCellCapableType<Object>) column.type).nameComparator();
    }

    private static ByteComparable columnKey(Object2IntHashMap<ColumnIdentifier> columnIds, ColumnMetadata column)
    {
        int id = columnIds.getValue(column.name);
        assert id != COLUMN_NOT_PRESENT;
        return v -> ByteSource.variableLengthInteger(id);
    }

    static CellPath cellPath(ColumnMetadata column, ByteSource.Peekable src)
    {
        int next = src.next();
        if (next == ByteSource.LT_NEXT_COMPONENT)
            return CellPath.BOTTOM;
        else if (next == ByteSource.GT_NEXT_COMPONENT)
            return CellPath.TOP;

        ByteSource.Peekable componentSource = ByteSourceInverse.nextComponentSource(src, next);
        ByteBuffer path = getCellPathType(column).fromComparableBytes(componentSource, BYTE_COMPARABLE_VERSION);
        return CellPath.create(path);
        // TODO: figure out a better way to do path slices and remove the leading path byte as
//        return CellPath.create(getCellPathType(column).fromComparableBytes(src, BYTE_COMPARABLE_VERSION));
    }

    public Cell<?> getCell(ColumnMetadata c)
    {
        assert !c.isComplex();
        return (Cell<?>) data.get(cellKey(columnIds, c, null));
    }

    public Cell<?> getCell(ColumnMetadata c, CellPath path)
    {
        assert c.isComplex();
        return (Cell<?>) data.get(cellKey(columnIds, c, path));
    }

    public ComplexColumnData getComplexColumnData(ColumnMetadata c)
    {
        assert c.isComplex();
        DeletionAwareTrie<Object, TrieTombstoneMarker> tail = data.tailTrie(columnKey(columnIds, c));
        if (tail != null)
            return new TrieBackedComplexColumn(c, tail);
        else
            return null;
    }

    public ColumnData getColumnData(ColumnMetadata c)
    {
        return c.isComplex() ? getComplexColumnData(c) : getCell(c);
    }

    public Collection<ColumnMetadata> columns()
    {
        return new AbstractCollection<ColumnMetadata>()
        {
            @Override public Iterator<ColumnMetadata> iterator()
            {
                return Iterators.transform(TrieBackedRow.this.iterator(), ColumnData::column);
            }
            @Override public int size()
            {
                return columnCount();
            }
        };
    }

    private static Object combineDataAndDeletion(Object content, TrieTombstoneMarker marker)
    {
        if (content == COMPLEX_COLUMN_MARKER)
            return content;
        if (content instanceof Cell)
            return content;
        if (marker.hasPointData(TrieTombstoneMarker.PointDataType.ROW))
            return null; // do not return row deletions
        // This must be a complex column deletion marker. Return it, which will also result in skipping the return path
        // marker.
        return marker;
    }

    static class ColumnDataIterator extends TrieTailsIterator.DeletionAware<Object, TrieTombstoneMarker, Object, ColumnData>
    {
        private final Columns columns;

        ColumnDataIterator(Columns columns, DeletionAwareTrie<Object, TrieTombstoneMarker> trie, Direction direction)
        {
            super(trie, direction, TrieBackedRow::combineDataAndDeletion);
            this.columns = columns;
        }

        @Override
        protected ColumnData mapContent(Object value, DeletionAwareTrie<Object, TrieTombstoneMarker> tailTrie, byte[] bytes, int byteLength)
        {
            if (value instanceof Cell)
                return (Cell<?>) value;

            // Column may have become empty after a deletion. If this is the case, don't return it.
            if (!tailTrie.filteredValuesIterator(Direction.FORWARD, Cell.class).hasNext() &&
                !tailTrie.deletionOnlyTrie().valueIterator().hasNext())
                return null;

            long columnIndex = ByteSourceInverse.getVariableLengthInteger(ByteSource.preencoded(bytes, 0, byteLength));
            assert ((int) columnIndex) == columnIndex;

            return new TrieBackedComplexColumn(columns.getSimple((int) columnIndex),
                                               tailTrie);
        }
    }

    public int columnCount()
    {
        return Iterators.size(new ColumnDataIterator(columns, data, Direction.FORWARD));
    }

    public Iterator<ColumnData> iterator()
    {
        return new ColumnDataIterator(columns, data, Direction.FORWARD);
    }

    public Iterable<Cell<?>> cells()
    {
        return () -> new CellsWithPath(data.contentOnlyTrie(), Direction.FORWARD);
    }

    public Row filter(ColumnFilter filter, TableMetadata metadata)
    {
        return filter(filter, DeletionTime.LIVE, false, metadata);
    }

    public static Object deleteData(Object existing, TrieTombstoneMarker marker)
    {
        return deleteData(marker, existing);
    }

    public static Object deleteData(TrieTombstoneMarker marker, Object existing)
    {
        DeletionTime deletion = marker.deletionTime();
        if (existing == COMPLEX_COLUMN_MARKER)
            return existing;
        if (existing instanceof LivenessInfo)
        {
            if (deletion.deletes(((LivenessInfo) existing).timestamp()))
                return LivenessInfo.EMPTY;
            else
                return existing;
        }
        if (existing instanceof Cell)
        {
            if (deletion.deletes((Cell<?>) existing))
                return null;
            else
                return existing;
        }
        throw new AssertionError("Unknown content type: " + existing);
    }

    public static Object dropCellValue(Object existing)
    {
        if (!(existing instanceof Cell))
            return existing;
        return ((Cell<?>) existing).withSkippedValue();
    }

    public Row filter(ColumnFilter filter, DeletionTime activeDeletion, boolean setActiveDeletionToRow, TableMetadata metadata)
    {
        Map<ByteBuffer, DroppedColumn> droppedColumns = metadata.droppedColumns;

        boolean mayFilterColumns = !filter.fetchesAllColumns(isStatic()) || !filter.allFetchedColumnsAreQueried();
        // When merging sstable data in Row.Merger#merge(), rowDeletion is removed if it doesn't supersede activeDeletion.
        boolean mayHaveDeleted = !activeDeletion.isLive();
        DeletionAwareTrie<Object, TrieTombstoneMarker> filteredData = data;
        if (!mayFilterColumns && !mayHaveDeleted && droppedColumns.isEmpty())
            return this;

//        // Metadata changes are not something we can handle.
//        if (!columns.equals(isStatic() ? metadata.staticColumns() : metadata.regularColumns()))
//            throw new IllegalArgumentException("Metadata columns do not match");

        if (!droppedColumns.isEmpty())
        {
            List<RangeTrie<TrieTombstoneMarker>> drops = new ArrayList<>();
            for (ColumnMetadata c : columns)
            {
                DroppedColumn dropped = droppedColumns.get(c.name.bytes);
                if (dropped != null)
                {
                    drops.add(RangeTrie.branch(columnKey(columnIds, c),
                                               BYTE_COMPARABLE_VERSION,
                                               TrieTombstoneMarker.covering(new DeletionTime(dropped.droppedTime, Integer.MIN_VALUE))));
                }
            }
            if (!drops.isEmpty())
                filteredData = filteredData.mergeWithDeletion(RangeTrie.merge(drops, TrieTombstoneMarker::merge),
                                                              TrieBackedRow::deleteData,
                                                              TrieTombstoneMarker::dropShadowed,
                                                              true);
        }

        if (mayFilterColumns)
        {
            // TODO: Column filter may include cell-level filters for complex columns, in both fetched and queried
            Columns queried = filter.queriedColumns().columns(isStatic());
            BitSet queriedIds = getColumnIds(queried);
            DeletionAwareTrie<Object, TrieTombstoneMarker> queriedData;
            if (queriedIds.cardinality() != columns.size())
                queriedData = filteredData.intersect(TrieSet.ranges(BYTE_COMPARABLE_VERSION, mapIdsToColumnKeys(queriedIds)));
            else
                queriedData = filteredData;

            Columns fetched = filter.fetchedColumns().columns(isStatic());
            BitSet fetchedButNotQueried = getColumnIds(fetched);
            fetchedButNotQueried.andNot(queriedIds);
            if (!fetchedButNotQueried.isEmpty())
            {
                DeletionAwareTrie<Object, TrieTombstoneMarker> fetchedButNotQueriedData =
                filteredData.intersect(TrieSet.ranges(BYTE_COMPARABLE_VERSION, mapIdsToColumnKeys(fetchedButNotQueried)))
                            .mapValues(TrieBackedRow::dropCellValue);
                filteredData = queriedData.mergeWith(fetchedButNotQueriedData,
                                                     Trie.throwingResolver(),
                                                     TrieTombstoneMarker::mergeWith,
                                                     noExistingSelfDeletion(),
                                                     true);
            }
            else
                filteredData = queriedData;
        }

        if (mayHaveDeleted)
        {
            filteredData = filteredData.mergeWithDeletion(RangeTrie.branch(ByteComparable.EMPTY,
                                                                           BYTE_COMPARABLE_VERSION,
                                                                           TrieTombstoneMarker.covering(activeDeletion)),
                                                          TrieBackedRow::deleteData,
                                                          setActiveDeletionToRow ? TrieTombstoneMarker::mergeWith
                                                                                 : TrieTombstoneMarker::dropShadowed,
                                                          true);
        }

        // TODO: We can return a view/filter-on-the-fly version, can't we?
//        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> newTrie = InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION);
//        try
//        {
//            newTrie.apply(filteredData,
//                          noConflictInData(),
//                          mergeTombstoneRanges(),
//                          noIncomingSelfDeletion(),
//                          noExistingSelfDeletion(),
//                          true,
//                          Predicates.alwaysFalse());
//        }
//        catch (TrieSpaceExhaustedException e)
//        {
//            throw new AssertionError(e);
//        }
        // TODO: Should we use `fetched` for `columns`? Note the ids cannot change.
        return new TrieBackedRow(columns, columnIds, clustering, filteredData);
    }

    private static ByteComparable[] mapIdsToColumnKeys(BitSet fetchedIds)
    {
        ByteComparable[] keys = new ByteComparable[fetchedIds.cardinality() * 2];
        int keyPos = 0;
        for (int i = fetchedIds.nextSetBit(0); i >= 0; i = fetchedIds.nextSetBit(i + 1))
        {
            final int id = i;
            ByteComparable columnKey = v -> ByteSource.variableLengthInteger(id);
            keys[keyPos++] = columnKey; // add twice for inclusive start and end
            keys[keyPos++] = columnKey;
        }
        assert keyPos == keys.length;
        return keys;
    }

    private BitSet getColumnIds(Columns fetched)
    {
        BitSet fetchedIds = new BitSet();
        for (ColumnMetadata c : fetched)
        {
            int idx = columnIds.get(c.name);
            if (idx == COLUMN_NOT_PRESENT)
                continue;
            fetchedIds.set(idx);
        }
        return fetchedIds;
    }

    public Row withOnlyQueriedData(ColumnFilter filter)
    {
        if (filter.allFetchedColumnsAreQueried())
            return this;

        // TODO: Column filter may include cell-level filters for complex columns
        Columns queried = filter.queriedColumns().columns(isStatic());
        BitSet queriedIds = getColumnIds(queried);
        if (queriedIds.cardinality() != columns.size())
            return new TrieBackedRow(columns,
                                     columnIds,
                                     clustering,
                                     data.intersect(TrieSet.ranges(BYTE_COMPARABLE_VERSION,
                                                                   mapIdsToColumnKeys(queriedIds))));
        else
            return this;
    }

    public boolean hasComplex()
    {
        // First entry in any order is LivenessInfo. The second entry is either a complex column marker or a cell.
        // Note: valueIterator ignores deletion branches.
        return Iterators.get(data.valueIterator(Direction.REVERSE), 1, null) == COMPLEX_COLUMN_MARKER;
    }

    public boolean hasComplexDeletion()
    {
        for (Map.Entry<ByteComparable.Preencoded, TrieTombstoneMarker> entry : data.deletionOnlyTrie().entrySet())
        {
            if (entry.getKey().getPreencodedBytes().peek() != ByteSource.END_OF_STREAM)
                return true; // entry below the root level exists, this must be a complex column deletion
        }
        return false;
    }

    public Row markCounterLocalToBeCleared()
    {
        return transformAndFilter(x -> x,
                                  c -> c.column().isCounterColumn() ? c.markCounterLocalToBeCleared()
                                                                    : c);
    }

    public boolean hasDeletion(int nowInSec)
    {
        return nowInSec >= getMinLocalDeletionTime();
    }

    public boolean hasInvalidDeletions()
    {
        return accumulate(0,
                          (liveness, v) -> (liveness.isExpiring() && (liveness.ttl() < 0 || liveness.localExpirationTime() < 0)) ? 1 : v,
                          (cell, v) -> cell.hasInvalidDeletions() ? 1 : v,
                          (marker, v) -> !marker.validate() ? 1 : v)
               != 0;
    }

    public DeletionAwareTrie<Object, TrieTombstoneMarker> trie()
    {
        return data;
    }

    /**
     * Returns a copy of the row where all timestamps for live data have replaced by {@code newTimestamp} and
     * all deletion timestamp by {@code newTimestamp - 1}.
     * </p>
     * This exists for the Paxos path, see {@link PartitionUpdate#withUpdatedTimestamps(long)} for additional details.
     */
    public Row updateAllTimestamp(long newTimestamp)
    {
        return transformAndFilter(liveness -> liveness.withUpdatedTimestamp(newTimestamp),
                                  cell -> cell.updateAllTimestamp(newTimestamp),
                                  dt -> dt.isLive() ? dt : new DeletionTime(newTimestamp - 1, dt.localDeletionTime()));
    }

    public Row withRowDeletion(DeletionTime newDeletion)
    {
        // Applies the deletion to the branch, removing any shadowed data (caller should ensure there isn't any, but
        // we do this properly for safety).
        return new TrieBackedRow(columns, columnIds, clustering,
                                 data.mergeWithDeletion(rowDeletionTrie(newDeletion),
                                                        TrieBackedRow::deleteData,
                                                        TrieTombstoneMarker::mergeWith,
                                                        true));
    }

    @Override
    public Row purge(DeletionPurger purger, int nowInSec, boolean enforceStrictLiveness)
    {
        // TODO: evaluate need/performance effect
        if (!hasDeletion(nowInSec))
            return this;

        if (enforceStrictLiveness)
        {
            // when enforceStrictLiveness is set, a row is considered dead when it's PK liveness info is not present
            LivenessInfo primaryLiveness = primaryKeyLivenessInfo();
            primaryLiveness = purger.shouldPurge(primaryLiveness, nowInSec) ? LivenessInfo.EMPTY : primaryLiveness;
            DeletionTime rowDeletion = TrieTombstoneMarker.deletionOfCovering(data.deletionOnlyTrie().applicableRange(ByteComparable.EMPTY));
            rowDeletion = rowDeletion != null && !purger.shouldPurge(rowDeletion) ? rowDeletion : null;
            if (primaryLiveness.isEmpty() && rowDeletion == null)
                return null;
        }

        return transformAndFilter(primaryKeyLivenessInfo -> purger.shouldPurge(primaryKeyLivenessInfo, nowInSec) ? LivenessInfo.EMPTY : primaryKeyLivenessInfo,
                                  cell -> cell.purge(purger, nowInSec),
                                  deletion -> purger.shouldPurge(deletion) ? null : deletion);
    }

    @Override
    public Row transformAndFilter(Function<LivenessInfo, LivenessInfo> livenessInfoFunction,
                                  Function<Cell<?>, Cell<?>> cellFunction)
    {
        return new TrieBackedRow(columns, columnIds, clustering, data.mapValues(
            (Object x) ->
            {
                if (x instanceof LivenessInfo)
                {
                    return (livenessInfoFunction.apply((LivenessInfo) x));
                }
                else if (x instanceof Cell)
                {
                    return cellFunction.apply((Cell<?>) x);
                }
                else
                    return x;   // complex column marker
            }));
    }

    public Row transformAndFilter(Function<LivenessInfo, LivenessInfo> livenessInfoFunction,
                                  Function<Cell<?>, Cell<?>> cellFunction,
                                  Function<DeletionTime, DeletionTime> markerFunction)
    {
        return new TrieBackedRow(columns, columnIds, clustering, data.mapValuesAndDeletions(
            (Object x) ->
            {
                if (x instanceof LivenessInfo)
                {
                    return (livenessInfoFunction.apply((LivenessInfo) x));
                }
                else if (x instanceof Cell)
                {
                    return cellFunction.apply((Cell<?>) x);
                }
                else
                    return x;   // complex column marker
            },
            t -> t.map(markerFunction)));
    }

    @Override
    public Row clone(Cloner cloner)
    {
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> newTrie = InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION);
        try
        {
            newTrie.apply(data,
                          ((ex, toClone) -> toClone instanceof Cell ? cloner.clone((Cell<?>) toClone) : toClone),
                          mergeTombstoneRanges(),
                          noIncomingSelfDeletion(),
                          noExistingSelfDeletion(),
                          true,
                          Predicates.alwaysFalse());
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return new TrieBackedRow(columns, columnIds, cloner.clone(clustering), newTrie);
    }

    // TODO: Redo size collection to be more direct.

    public int dataSize()
    {
        int dataSize = clustering.dataSize()
                     + primaryKeyLivenessInfo().dataSize()
                     + deletion().dataSize();

        return Ints.checkedCast(accumulate((cd, v) -> v + cd.dataSize(), dataSize));
    }

    @Override
    public int liveDataSize(int nowInSec)
    {
        int dataSize = clustering.dataSize()
                       + primaryKeyLivenessInfo().dataSize()
                       + deletion().dataSize();

        return Ints.checkedCast(accumulate((cd, v) -> v + cd.liveDataSize(nowInSec), dataSize));
    }

    public long unsharedHeapSizeExcludingData()
    {
        // TODO: this should not be used
        long heapSize = EMPTY_SIZE + clustering.unsharedHeapSizeExcludingData();
        if (data instanceof InMemoryDeletionAwareTrie)
            heapSize += ((InMemoryDeletionAwareTrie) data).usedSizeOnHeap();

        return accumulate(heapSize,
                          (liveness, v) -> v + liveness.unsharedHeapSize(),
                          (cell, v) -> v + cell.unsharedHeapSizeExcludingData(),
                          (marker, v) -> v + marker.unsharedHeapSize());
    }

    @Override
    public void apply(Consumer<ColumnData> function)
    {
        for (ColumnData cd : this)
            function.accept(cd);
    }

    @Override
    public <A> void apply(BiConsumer<A, ColumnData> function, A arg)
    {
        for (ColumnData cd : this)
            function.accept(arg, cd);
    }

    private static Builder builder(TableMetadata metadata, Clustering<?> clustering)
    {
        Builder builder = new Builder(metadata.regularAndStaticColumns());
        builder.newRow(clustering);
        return builder;
    }

    public static Row.Builder builder(RegularAndStaticColumns regularAndStaticColumns)
    {
        return new Builder(regularAndStaticColumns);
    }

    private static Object mergeData(Object existing, Object update, ColumnData.PostReconciliationFunction reconcileF)
    {
        if (update instanceof LivenessInfo)
            return LivenessInfo.merge((LivenessInfo) existing, (LivenessInfo) update);
        else if (update instanceof Cell)
        {
            Cell<?> existingCell = (Cell<?>) existing;
            Cell<?> updateCell = (Cell<?>) update;
            if (existingCell == null)
                return reconcileF.insert(updateCell);
            else
                return reconcileF.merge(existingCell, Cells.reconcile(existingCell, updateCell));
        }
        else
        {
            assert existing == COMPLEX_COLUMN_MARKER;
            return existing;
        }
    }

    private static Object deleteData(Object existing, TrieTombstoneMarker marker, ColumnData.PostReconciliationFunction reconcileF)
    {
        DeletionTime deletion = marker.deletionTime();
        if (existing instanceof LivenessInfo)
            return deletion.deletes((LivenessInfo) existing) ? LivenessInfo.EMPTY : existing;
        else if (existing instanceof Cell)
        {
            Cell<?> existingCell = (Cell<?>) existing;
            assert existingCell != null;
            if (!deletion.deletes(existingCell))
                return existingCell;

            reconcileF.delete(existingCell);
            return null;
        }
        else
        {
            assert existing == COMPLEX_COLUMN_MARKER;
            return existing;
        }
    }

    public Row mergeWith(Row updateAsRow,
                         ColumnData.PostReconciliationFunction reconcileF)
    {
        if (!(updateAsRow instanceof TrieBackedRow))
            throw new IllegalArgumentException("Merging different row types.");
        TrieBackedRow update = (TrieBackedRow) updateAsRow;
        // TODO: This should be merging into in-memory trie
        if (!this.columns.equals(update.columns))
            throw new IllegalArgumentException("Can't handle varying column lists.");

        return new TrieBackedRow(this.columns,
                                 this.columnIds,
                                 this.clustering,
                                 this.data.mergeWith(update.data,
                                                     (ex, up) -> mergeData(ex, up, reconcileF),
                                                     TrieTombstoneMarker::mergeWith,
                                                     (marker, ex) -> deleteData(ex, marker, reconcileF),
                                                     true
                                 ));
    }

    public int getMinLocalDeletionTime()
    {
        if (!minLocalDeletionTimeSet)
        {
            long accumulated = accumulate(Integer.MAX_VALUE,
                                         (livenessInfo, mldt) -> Math.min(mldt, minDeletionTime(livenessInfo)),
                                         (cell, mldt) -> Math.min(mldt, minDeletionTime(cell)),
                                         (marker, mldt) -> Math.min(mldt, minDeletionTime(marker)));
            minLocalDeletionTime = (int) accumulated;
            minLocalDeletionTimeSet = true;
        }
        return minLocalDeletionTime;
    }

    static class CellsWithPath extends TrieEntriesIterator.WithNullFiltering<Object, Cell<?>>
    {
        protected CellsWithPath(Trie<Object> trie, Direction direction)
        {
            super(trie, direction);
        }

        @Override
        protected Cell<?> mapContent(Object content, byte[] bytes, int byteLength)
        {
            if (!(content instanceof Cell))
                return null;

            Cell<?> c = (Cell<?>) content;
            if (c.column.isSimple() || c.path() != null)
                return c;

            ByteSource.Peekable pathBytes = ByteSource.preencoded(bytes, 0, byteLength);
            ByteSourceInverse.getVariableLengthInteger(pathBytes); // skip column id
            return c.withPath(cellPath(c.column, pathBytes));
        }
    }

    public static class Builder implements Row.Builder
    {
        protected final RegularAndStaticColumns regularAndStaticColumns;
        protected final Object2IntHashMap<ColumnIdentifier> regularColumnIds;
        protected final Object2IntHashMap<ColumnIdentifier> staticColumnIds;
        protected Clustering<?> clustering;
        protected Object2IntHashMap<ColumnIdentifier> columnIds;
        private InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data;

        // For complex column at index i of 'columns', we store at complexDeletions[i] its complex deletion.

        protected Builder(RegularAndStaticColumns regularAndStaticColumns)
        {
            this.regularAndStaticColumns = regularAndStaticColumns;
            regularColumnIds = columnsMapCache.computeIfAbsent(regularAndStaticColumns.regulars, TrieBackedRow::makeColumnIdsMap);
            staticColumnIds = columnsMapCache.computeIfAbsent(regularAndStaticColumns.statics, TrieBackedRow::makeColumnIdsMap);
            data = InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION);
        }

        protected Builder(Builder builder)
        {
            this.regularAndStaticColumns = builder.regularAndStaticColumns;
            this.regularColumnIds = builder.regularColumnIds;
            this.staticColumnIds = builder.staticColumnIds;
            this.clustering = builder.clustering;
            this.columnIds = builder.columnIds;
            try
            {
                data.apply(builder.data,
                           noConflictInData(),
                           mergeTombstoneRanges(),
                           noIncomingSelfDeletion(),
                           noExistingSelfDeletion(),
                           true,
                           Predicates.alwaysFalse());
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Builder copy()
        {
            return new Builder(this);
        }

        public boolean isSorted()
        {
            return true;
        }

        public void newRow(Clustering<?> clustering)
        {
            assert this.clustering == null; // Ensures we've properly called build() if we've use this builder before
            this.clustering = clustering;
            this.columnIds = clustering == Clustering.STATIC_CLUSTERING ? staticColumnIds : regularColumnIds;
        }

        public Clustering<?> clustering()
        {
            return clustering;
        }

        protected void reset()
        {
            this.clustering = null;
            data = InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION);
        }

        public void addPrimaryKeyLivenessInfo(LivenessInfo info)
        {
            TrieTombstoneMarker rowDeletion = data.applicableDeletion(ByteComparable.EMPTY);
            if (rowDeletion != null && TrieTombstoneMarker.deletionOfCovering(rowDeletion).deletes(info))
                return;

            try
            {
                data.putRecursive(ByteComparable.EMPTY, (info), (x, y) -> y);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void addRowDeletion(Deletion deletion)
        {
            if (deletion.isLive())
                return;

            try
            {
                data.delete(rowDeletionTrie(deletion.time()),
                            TrieBackedRow::deleteData,
                            TrieBackedPartition.mergeTombstoneRanges(),
                            true,
                            Predicates.alwaysFalse());
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void addCell(Cell<?> cell)
        {
            assert cell.column().isStatic() == (clustering == Clustering.STATIC_CLUSTERING) : "Column is " + cell.column() + ", clustering = " + clustering;
            ByteComparable key = cellKey(columnIds, cell.column, cell.path());

            // TODO: Use apply to take care of this?
            TrieTombstoneMarker cellDeletion = data.applicableDeletion(key);
            if (cellDeletion != null && TrieTombstoneMarker.deletionOfCovering(cellDeletion).deletes(cell))
                return;

            try
            {
                data.putRecursive(key, cell.withPath(null), (x, y) -> Cells.reconcile((Cell<?>) x, y));
                if (cell.column.isComplex())
                    data.putRecursive(columnKey(columnIds, cell.column), COMPLEX_COLUMN_MARKER, (x, y) -> y);
                if (data.get(ByteComparable.EMPTY) == null)
                    data.putRecursive(ByteComparable.EMPTY, LivenessInfo.EMPTY, (x, y) -> y);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new RuntimeException(e);
            }
        }

        public void addComplexDeletion(ColumnMetadata column, DeletionTime deletion)
        {
            ByteComparable key = columnKey(columnIds, column);
            try
            {
                data.delete(deletionTrie(key, deletion),
                            TrieBackedRow::deleteData,
                            TrieBackedPartition.mergeTombstoneRanges(),
                            true,
                            Predicates.alwaysFalse());
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new RuntimeException(e);
            }
        }

        public TrieBackedRow build()
        {
            TrieBackedRow row = new TrieBackedRow(regularAndStaticColumns.columns(clustering == Clustering.STATIC_CLUSTERING),
                                                  columnIds,
                                                  clustering,
                                                  data);
            reset();
            return row;
        }
    }
}
