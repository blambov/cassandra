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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
import org.apache.cassandra.db.tries.InMemoryBaseTrie;
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

/// Immutable implementation of a [Row] object.
/// Stores either [Cell], or [TrieCellData] (if the row comes from a memtable trie).
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

    static final TrieSet REMOVE_ROOTS_CHILDREN = TrieSet.range(BYTE_COMPARABLE_VERSION,
                                                               v -> ByteSource.oneByte(0),
                                                               v -> ByteSource.oneByte(255))
                                                        .negation();

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
        builder.addPrimaryKeyLivenessInfo(row.primaryKeyLivenessInfo());
        builder.addRowDeletion(row.deletion());
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

    private static final Map<Columns, Object2IntHashMap<ColumnIdentifier>> columnsMapCache = new ConcurrentHashMap<>();

    public static boolean shouldPreserveContentWithoutChildren(Object o)
    {
        return o != LivenessInfo.EMPTY && o != COMPLEX_COLUMN_MARKER && o != TrieTombstoneMarker.LevelMarker.ROW;
    }

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

    public static TrieBackedRow emptyDeletedRow(Clustering<?> clustering, DeletionTime deletion)
    {
        try
        {
            assert !deletion.isLive();
            InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> trie = newTrie();
            // We need to put the deletion as well as a deletion-path row marker.
            RangeTrie<TrieTombstoneMarker> deletionTrie = rowDeletionTrie(deletion);

            trie.mutator(TrieBackedPartition.noConflictInData(),
                         TrieBackedPartition.mergeTombstoneRanges(),
                         TrieBackedPartition.noIncomingSelfDeletion(),
                         TrieBackedPartition.noExistingSelfDeletion(),
                         true,
                         Predicates.alwaysFalse())
                .delete(deletionTrie);
            return new TrieBackedRow(Columns.NONE, EMPTY_COLUMN_IDS, clustering, trie);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    private static RangeTrie<TrieTombstoneMarker> rowDeletionTrie(DeletionTime deletion)
    {
        return deletionTrie(ByteComparable.EMPTY, deletion, TrieTombstoneMarker.Kind.ROW);
    }

    private static RangeTrie<TrieTombstoneMarker> deletionTrie(ByteComparable prefix, DeletionTime deletion, TrieTombstoneMarker.Kind kind)
    {
        return withDeletionRoot(RangeTrie.branch(prefix,
                                                 BYTE_COMPARABLE_VERSION,
                                                 TrieTombstoneMarker.covering(deletion, kind)));
    }

    private static RangeTrie<TrieTombstoneMarker> withDeletionRoot(RangeTrie<TrieTombstoneMarker> trie)
    {
        // Range tries present separate content in the two directions. We need to add a marker in both.
        return trie.mergeWith(RangeTrie.point(ByteComparable.EMPTY,
                                              BYTE_COMPARABLE_VERSION,
                                              true,
                                              TrieTombstoneMarker.LevelMarker.ROW),
                              TrieTombstoneMarker::mergeUpdate)
                   .mergeWith(RangeTrie.point(ByteComparable.EMPTY,
                                              BYTE_COMPARABLE_VERSION,
                                              false,
                                              TrieTombstoneMarker.LevelMarker.ROW),
                              TrieTombstoneMarker::mergeUpdate);
    }

    private static int minDeletionTime(CellData cell)
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

    static class Accumulator implements DeletionAwareTrie.ValueConsumer<Object, TrieTombstoneMarker>
    {
        final LongAccumulator<CellData> cellAccumulator;
        final LongAccumulator<LivenessInfo> livenessAccumulator;
        final LongAccumulator<DeletionTime> markerAccumulator;
        long value;

        Accumulator(long initialValue,
                    LongAccumulator<CellData> cellAccumulator,
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
            else if (content instanceof CellData)
                value = cellAccumulator.apply((CellData) content, value);
            else if (content != COMPLEX_COLUMN_MARKER)
                throw new AssertionError("Unexpected content type: " + content);
        }

        @Override
        public void deletionMarker(TrieTombstoneMarker marker)
        {
            if (marker.isBoundary())
            {
                // We only apply the function to one side of the marker; the other has to be already be seen as a
                // succeeding side of a different marker.
                TrieTombstoneMarker.Covering succedingState = marker.succedingState(Direction.FORWARD);
                if (succedingState != null)
                    value = markerAccumulator.apply(succedingState, value);
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
                    LongAccumulator<CellData> cellAccumulator,
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
        // Empty has no live or deletion branch but may have an empty row marker.
        // TODO: make a garbage-free method for this
        return isEmpty(data);
    }

    public static boolean isEmpty(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        if (data == null)
            return true;

        if (!isEmptyAfterDeletion(data))
            return false;

        if (data instanceof InMemoryDeletionAwareTrie)
        {
            // the row deletion marker will only be present if there is a deletion present
            return data.applicableDeletion(ByteComparable.EMPTY) == null;
        }
        else
        {
            // The row deletion marker may remain even if the data is deleted/filtered out.
            // Check for the existence of a deletion marker
            return !data.deletionOnlyTrie().filteredValuesIterator(Direction.FORWARD, TrieTombstoneMarker.Boundary.class).hasNext();
        }
    }

    public boolean isEmptyAfterDeletion()
    {
        return isEmptyAfterDeletion(data);
    }

    public static boolean isEmptyAfterDeletion(DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        if (data instanceof InMemoryDeletionAwareTrie)
        {
            // the liveness marker will be dropped if there are no cells
            return data.get(ByteComparable.EMPTY) == null;
        }
        else
        {
            // The liveness marker may remain even if the data is deleted/filtered out.
            // Check for the existence of:
            // - non-empty liveness
            LivenessInfo info = (LivenessInfo) data.get(ByteComparable.EMPTY);
            if (info != null && info != LivenessInfo.EMPTY)
                return false;

            // - a cell
            return !data.contentOnlyTrie().filteredValuesIterator(Direction.FORWARD, CellData.class).hasNext();
        }
    }

    public Deletion deletion()
    {
        TrieTombstoneMarker marker = data.applicableDeletion(ByteComparable.EMPTY);
        if (marker == null)
            return Deletion.LIVE;
        DeletionTime delTime = marker.applicableToPointForward();
        if (delTime == null)
            return Deletion.LIVE;
        else
            return Deletion.regular(delTime);
    }

    public static ByteSource columnKey(Columns columns, ColumnMetadata column)
    {
        return ByteSource.variableLengthUnsignedInteger(columns.simpleIdx(column));
    }

    public static ByteSource cellPathKey(ColumnMetadata column, CellPath path, ByteComparable.Version version)
    {
        return ByteSource.withTerminator(ByteSource.TERMINATOR,
                                         getCellPathType(column).asComparableBytes(path.get(0), version));
    }

    private static ByteComparable cellKey(Object2IntHashMap<ColumnIdentifier> columnIds, ColumnMetadata column, CellPath path)
    {
        int id = columnIds.get(column.name);
        assert id != COLUMN_NOT_PRESENT;
        if (!column.isComplex())
            return v -> ByteSource.variableLengthUnsignedInteger(id);
        else
            return cellPath(id, column, path);
    }

    private static ByteSource columnIdPrefix(int columnId)
    {
        if (columnId < 0)
            return ByteSource.EMPTY;
        else
            return ByteSource.variableLengthUnsignedInteger(columnId);
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
                                          cellPathKey(column, path, v));
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
        return v -> ByteSource.variableLengthUnsignedInteger(id);
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
        Object o = data.get(cellKey(columnIds, c, null));
        if (o == null || o instanceof Cell)
            return (Cell) o;
        CellData cellData = (CellData) o;
        return cellData.toCell(c, null);
    }

    public Cell<?> getCell(ColumnMetadata c, CellPath path)
    {
        assert c.isComplex();
        Object o = data.get(cellKey(columnIds, c, path));
        if (o == null || o instanceof Cell)
            return (Cell) o;
        CellData cellData = (CellData) o;
        return cellData.toCell(c, path);
    }

    public ComplexColumnData getComplexColumnData(ColumnMetadata c)
    {
        assert c.isComplex();
        DeletionAwareTrie<Object, TrieTombstoneMarker> tail = data.tailTrie(columnKey(columnIds, c));
        if (!isColumnDataTrieEmpty(tail))
            return new TrieBackedComplexColumn(c, tail);
        else
            return null;
    }

    private static boolean isColumnDataTrieEmpty(DeletionAwareTrie<Object, TrieTombstoneMarker> tail)
    {
        if (tail == null)
            return true;
        // We may be left with only a COMPLEX_COLUMN_MARKER after some transformation.
        if (tail instanceof InMemoryDeletionAwareTrie)
            return false; // in-memory trie will drop the marker
        if (tail.applicableDeletion(ByteComparable.EMPTY) != null)
            return false;
        // otherwise it's empty if it has no cells
        return !tail.filteredValuesIterator(Direction.FORWARD, CellData.class).hasNext();
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
        if (content instanceof CellData)
            return content;
        if (content == COMPLEX_COLUMN_MARKER)
            return content;
        if (content instanceof LivenessInfo)
            return null;
        if (marker.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW))
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
            super(trie, direction, TrieBackedRow::combineDataAndDeletion, false);
            this.columns = columns;
        }

        @Override
        protected ColumnData mapContent(Object value, DeletionAwareTrie<Object, TrieTombstoneMarker> tailTrie, byte[] bytes, int byteLength)
        {
            if (value instanceof CellData)
                return cellFromCellData((CellData) value, bytes, byteLength, columns);

            // We may be left with only a COLUMN_DATA_MARKER after a deletion. If this is the case, don't return it.
            if (isColumnDataTrieEmpty(tailTrie))
                return null;

            long columnIndex = ByteSourceInverse.getVariableLengthUnsignedInteger(ByteSource.preencoded(bytes, 0, byteLength));
            assert ((int) columnIndex) == columnIndex;

            return new TrieBackedComplexColumn(columns.getSimple((int) columnIndex),
                                               tailTrie);
        }

    }

    private static Cell<?> cellFromCellData(CellData value, byte[] bytes, int byteLength, Columns columns)
    {
        if (value instanceof Cell)
            return (Cell<?>) value;
        ByteSource.Peekable pathBytes = ByteSource.preencoded(bytes, 0, byteLength);
        long columnIdx = ByteSourceInverse.getVariableLengthUnsignedInteger(pathBytes);
        ColumnMetadata column = columns.getSimple((int) columnIdx);
        return value.toCell(column, column.isComplex() ? cellPath(column, pathBytes) : null);
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
        if (existing == COMPLEX_COLUMN_MARKER)
            return existing;

        DeletionTime deletion = marker.applicableToPointForward();
        if (deletion == null)
            return existing;

        if (existing instanceof LivenessInfo)
        {
            if (deletion.deletes(((LivenessInfo) existing).timestamp()))
                return LivenessInfo.EMPTY;
            else
                return existing;
        }
        if (existing instanceof CellData)
        {
            if (deletion.deletes((CellData) existing))
                return null;
            else
                return existing;
        }
        throw new AssertionError("Unknown content type: " + existing);
    }

    public static Object dropCellValue(Object existing)
    {
        if (!(existing instanceof CellData))
            return existing;
        return ((CellData) existing).withSkippedValue();
    }

    public static Object mergeRowHeader(Object x, Object y)
    {
        if (x == y)
            return x;
        else if (x instanceof LivenessInfo && y instanceof LivenessInfo)
            return LivenessInfo.merge((LivenessInfo) x, (LivenessInfo) y);
        else
            throw new IllegalArgumentException("Unexpected data clash, " + x + " and " + y);
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
                                               TrieTombstoneMarker.covering(dropped.droppedTime, Integer.MIN_VALUE, TrieTombstoneMarker.Kind.COLUMN)));
                }
            }
            if (!drops.isEmpty())
                filteredData = filteredData.mappingMergeWithDeletion(RangeTrie.merge(drops, TrieTombstoneMarker::merge),
                                                                     TrieBackedRow::deleteData,
                                                                     TrieTombstoneMarker::dropShadowedUpdate,
                                                                     true);
        }

        if (mayFilterColumns)
        {
            // TODO: Column filter may include cell-level filters for complex columns, in both fetched and queried
            Columns queried = filter.queriedColumns().columns(isStatic());
            BitSet queriedIds = getColumnIds(queried);

            // Getting queried and fetchedButNotQueried separately and merging looks more efficieny, but in general
            // most of the time we'll either have fetched getting all columns or fetched equal to queried.
            // Filtering by fetched first avoids one operation in the former case.
            Columns fetched = filter.fetchedColumns().columns(isStatic());
            BitSet fetchedIds = getColumnIds(fetched);
            if (!filter.fetchesAllColumns(isStatic()))
            {
                // Select only the fetched columns.
                if (!fetched.isEmpty())
                    filteredData = filteredData.intersect(TrieSet.ranges(BYTE_COMPARABLE_VERSION, mapIdsToColumnKeys(fetchedIds)));
                else
                {
                    // If the fetched list is empty, we will lose the row liveness info and marker if we apply the
                    // filter as above. Instead we intersect with a set that removes all children.
                    filteredData = filteredData.intersect(REMOVE_ROOTS_CHILDREN);
                }
            }

            BitSet fetchedButNotQueried = fetchedIds;
            fetchedButNotQueried.andNot(queriedIds);

            if (!fetchedButNotQueried.isEmpty())
            {
                DeletionAwareTrie<Object, TrieTombstoneMarker> fetchedButNotQueriedData =
                filteredData.intersect(TrieSet.ranges(BYTE_COMPARABLE_VERSION, mapIdsToColumnKeys(fetchedButNotQueried)))
                            .mapValues(TrieBackedRow::dropCellValue);
                // TODO: value mapper for intersections?
                filteredData = filteredData.mergeWith(fetchedButNotQueriedData,
                                                      (x, y) -> y, // fetchedButNotQueried overrides data cells
                                                      TrieTombstoneMarker::mergeUpdate,
                                                      noExistingSelfDeletion(),
                                                      true);
            }
        }

        if (filteredData != data)
        {
            // Because we do not support column-level deletions for simple columns, we need to keep the row-level deletion
            // at the root. The intersections above move it down to the cell level.
            TrieTombstoneMarker rowDeletion = data.applicableDeletion(ByteComparable.EMPTY);
            if (rowDeletion != null && rowDeletion.applicableToPointForward() != null)
                filteredData = filteredData.mergeWithDeletion(RangeTrie.branch(ByteComparable.EMPTY,
                                                                               BYTE_COMPARABLE_VERSION,
                                                                               rowDeletion.applicableToPointForward()),
                                                              (x, y) -> y, // deletion is already applied
                                                              TrieTombstoneMarker::mergeUpdate,
                                                              true);
        }


        if (mayHaveDeleted)
        {
            if (setActiveDeletionToRow)
                filteredData = filteredData.mergeWithDeletion(RangeTrie.branch(ByteComparable.EMPTY,
                                                                               BYTE_COMPARABLE_VERSION,
                                                                               TrieTombstoneMarker.covering(activeDeletion, TrieTombstoneMarker.Kind.COLUMN)),
                                                              TrieBackedRow::deleteData,
                                                              TrieTombstoneMarker::mergeUpdate,
                                                              true);
            else // we need mappingMerge to make sure that the resolver is called for all update markers so that we can drop them
                filteredData = filteredData.mappingMergeWithDeletion(RangeTrie.branch(ByteComparable.EMPTY,
                                                                                      BYTE_COMPARABLE_VERSION,
                                                                                      TrieTombstoneMarker.covering(activeDeletion, TrieTombstoneMarker.Kind.COLUMN)),
                                                                     TrieBackedRow::deleteData,
                                                                     TrieTombstoneMarker::dropShadowedUpdate,
                                                                     true);
        }

        // TODO: Should we use `fetched` for `columns`? Note the ids cannot change.

        if (isEmpty(filteredData))
            return null;

        return new TrieBackedRow(columns, columnIds, clustering, filteredData);
    }

    private static ByteComparable[] mapIdsToColumnKeys(BitSet fetchedIds)
    {
        ByteComparable[] keys = new ByteComparable[fetchedIds.cardinality() * 2];
        int keyPos = 0;
        for (int i = fetchedIds.nextSetBit(0); i >= 0; i = fetchedIds.nextSetBit(i + 1))
        {
            final int id = i;
            ByteComparable columnKey = v -> ByteSource.variableLengthUnsignedInteger(id);
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
                                  c -> c.markCounterLocalToBeCleared());
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
        if (newDeletion.isLive())
            return this;

        return new TrieBackedRow(columns, columnIds, clustering,
                                 data.mergeWithDeletion(rowDeletionTrie(newDeletion),
                                                        TrieBackedRow::deleteData,
                                                        TrieTombstoneMarker::mergeUpdate,
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
            TrieTombstoneMarker deletion = data.applicableDeletion(ByteComparable.EMPTY);
            DeletionTime rowDeletion = deletion != null ? deletion.applicableToPointForward() : null;
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
                                  Function<CellData<?>, CellData<?>> cellFunction)
    {
        return new TrieBackedRow(columns, columnIds, clustering, data.mapValues(
            (Object x) ->
            {
                if (x instanceof LivenessInfo)
                {
                    return (livenessInfoFunction.apply((LivenessInfo) x));
                }
                else if (x instanceof CellData)
                {
                    return cellFunction.apply((CellData<?>) x);
                }
                else
                    return x;   // complex column marker
            }));
    }

    public Row transformAndFilter(Function<LivenessInfo, LivenessInfo> livenessInfoFunction,
                                  Function<CellData, CellData> cellFunction,
                                  Function<DeletionTime, DeletionTime> markerFunction)
    {
        DeletionAwareTrie<Object, TrieTombstoneMarker> mappedData = data.mapValuesAndDeletions(
            (Object x) ->
            {
                if (x instanceof LivenessInfo)
                {
                    return (livenessInfoFunction.apply((LivenessInfo) x));
                }
                else if (x instanceof CellData)
                {
                    return cellFunction.apply((CellData) x);
                }
                else
                    return x;   // complex column marker
            },
            t -> t.map(markerFunction));
        if (isEmpty(mappedData))
            return null;

        return new TrieBackedRow(columns, columnIds, clustering, mappedData);
    }

    @Override
    public Row clone(Cloner cloner)
    {
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> newTrie = newTrie();
        try
        {
            newTrie.mutator(((ex, toClone) -> toClone instanceof CellData ? ((CellData) toClone).clone(cloner) : toClone),
                            mergeTombstoneRanges(),
                            noIncomingSelfDeletion(),
                            noExistingSelfDeletion(),
                            true,
                            Predicates.alwaysFalse())
            .apply(data);
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

    private static Object mergeData(Object existing, Object update)
    {
        if (update instanceof LivenessInfo)
            return LivenessInfo.merge((LivenessInfo) existing, (LivenessInfo) update);
        else if (update instanceof CellData)
        {
            CellData existingCell = (CellData) existing;
            CellData updateCell = (CellData) update;
            return Cells.reconcile(existingCell, updateCell);
        }
        else
        {
            assert existing == COMPLEX_COLUMN_MARKER;
            return existing;
        }
    }

    public static InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> newTrie()
    {
        return InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION, TrieBackedRow::shouldPreserveContentWithoutChildren);
    }

    public Row mergeWith(Row updateAsRow)
    {
        if (!(updateAsRow instanceof TrieBackedRow))
            throw new IllegalArgumentException("Merging different row types.");
        TrieBackedRow update = (TrieBackedRow) updateAsRow;
        if (!this.columns.containsAll(update.columns))
            throw new IllegalArgumentException("Can't handle varying column lists.");

        try
        {
            InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> mergedData = newTrie();
            makeMutator(mergedData)
                .apply(this.data.mergeWith(update.data,
                                           TrieBackedRow::mergeData,
                                           TrieTombstoneMarker::mergeUpdate,
                                           TrieBackedRow::deleteData,
                                           true
                ));
            return new TrieBackedRow(this.columns,
                                     this.columnIds,
                                     this.clustering,
                                     mergedData);
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
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

    class CellsWithPath extends TrieEntriesIterator.WithNullFiltering<Object, Cell<?>>
    {
        protected CellsWithPath(Trie<Object> trie, Direction direction)
        {
            super(trie, direction);
        }

        @Override
        protected Cell<?> mapContent(Object content, byte[] bytes, int byteLength)
        {
            if (!(content instanceof CellData))
                return null;
            return cellFromCellData((CellData) content, bytes, byteLength, columns);
        }
    }

    static InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker>
    makeMutator(InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        return data.mutator(noConflictInData(),
                            TrieBackedPartition.mergeTombstoneRanges(),
                            (InMemoryBaseTrie.UpsertTransformer<Object, TrieTombstoneMarker>) TrieBackedRow::deleteData,
                            TrieBackedRow::deleteData,
                            true,
                            Predicates.alwaysFalse(),
                            Predicates.alwaysFalse());
    }

    public static class Builder implements Row.Builder
    {
        protected final RegularAndStaticColumns regularAndStaticColumns;
        protected final Object2IntHashMap<ColumnIdentifier> regularColumnIds;
        protected final Object2IntHashMap<ColumnIdentifier> staticColumnIds;
        protected Clustering<?> clustering;
        protected Object2IntHashMap<ColumnIdentifier> columnIds;
        private InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> data;
        private InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker>.Mutator<Object, TrieTombstoneMarker> mutator;

        // For complex column at index i of 'columns', we store at complexDeletions[i] its complex deletion.

        protected Builder(RegularAndStaticColumns regularAndStaticColumns)
        {
            this.regularAndStaticColumns = regularAndStaticColumns;
            regularColumnIds = columnsMapCache.computeIfAbsent(regularAndStaticColumns.regulars, TrieBackedRow::makeColumnIdsMap);
            staticColumnIds = columnsMapCache.computeIfAbsent(regularAndStaticColumns.statics, TrieBackedRow::makeColumnIdsMap);
            reset();
        }

        protected Builder(Builder builder)
        {
            this.regularAndStaticColumns = builder.regularAndStaticColumns;
            this.regularColumnIds = builder.regularColumnIds;
            this.staticColumnIds = builder.staticColumnIds;
            this.clustering = builder.clustering;
            this.columnIds = builder.columnIds;
            reset();
            try
            {
                mutator.apply(builder.data);
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
            data = newTrie();
            mutator = makeMutator(data);
        }

        public void addPrimaryKeyLivenessInfo(LivenessInfo info)
        {
            TrieTombstoneMarker rowDeletion = data.applicableDeletion(ByteComparable.EMPTY);
            if (rowDeletion != null && rowDeletion.applicableToPointForward().deletes(info))
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
                mutator.delete(rowDeletionTrie(deletion.time()));
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
            if (cellDeletion != null && cellDeletion.applicableToPointForward().deletes(cell))
                return;

            try
            {
                data.putRecursive(key, cell, (x, y) -> Cells.reconcile((Cell<?>) x, y));
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
            if (deletion.isLive())
                return;

            ByteComparable key = columnKey(columnIds, column);
            try
            {
                mutator.delete(deletionTrie(key, deletion, TrieTombstoneMarker.Kind.COLUMN));
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
