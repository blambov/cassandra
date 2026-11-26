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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

import org.agrona.collections.Object2IntHashMap;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieBackedRow;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.BasicDeletionAwareCursor;
import org.apache.cassandra.db.tries.BasicDeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.EntryStreamCursor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.partitions.TrieBackedPartition.BYTE_COMPARABLE_VERSION;
import static org.apache.cassandra.db.partitions.TrieBackedPartition.PARTITION_MARKER;

/// Presents a partition update as a [BasicDeletionAwareTrie] with the same content as the trie representation of the
/// update, i.e. the trie [TriePartitionUpdate#asMergableTrie] would build (prefixed by the partition key), without
/// building that trie. This makes it possible to merge non-trie partition updates (e.g. b-tree-backed ones)
/// directly into trie memtables.
///
/// The view is built on the update's [UnfilteredRowIterator], the same source that is used to convert updates to
/// trie form. Each cursor walks a new iterator.
///
/// The deletions of the update are presented as the merged deletion state that the trie representation stores in its
/// deletion branch (rooted at the partition root). In the rare cases where deletions of different levels (partition,
/// range, row or column) have exactly the same deletion time, the kind of the presented deletion can differ from the
/// one the trie conversion would produce.
public class PartitionUpdateTrieView implements BasicDeletionAwareTrie<Object, TrieTombstoneMarker>
{
    private final PartitionUpdate update;
    private final boolean hasDeletions;

    private PartitionUpdateTrieView(PartitionUpdate update)
    {
        this.update = update;
        this.hasDeletions = hasDeletions(update);
    }

    /// Returns a trie presenting the content of the given update in trie form, prefixed by the partition key. Trie
    /// updates are presented directly; other updates are presented through a view.
    public static BasicDeletionAwareTrie<Object, TrieTombstoneMarker> mergableTrie(PartitionUpdate update)
    {
        if (update instanceof TriePartitionUpdate)
            return TriePartitionUpdate.asMergableTrie(update);
        return new PartitionUpdateTrieView(update);
    }

    /// Returns a view of the given update in trie form, prefixed by the partition key.
    public static PartitionUpdateTrieView of(PartitionUpdate update)
    {
        return new PartitionUpdateTrieView(update);
    }

    private static boolean hasDeletions(PartitionUpdate update)
    {
        if (!update.deletionInfo().isLive())
            return true;
        if (hasDeletions(update.staticRow()))
            return true;
        for (Iterator<Row> it = update.rowIterator(); it.hasNext();)
        {
            if (hasDeletions(it.next()))
                return true;
        }
        return false;
    }

    private static boolean hasDeletions(Row row)
    {
        return !row.deletion().isLive() || row.hasComplexDeletion();
    }

    @Override
    public BasicDeletionAwareCursor<Object, TrieTombstoneMarker> basicCursor(Direction direction)
    {
        return new ViewCursor(update, direction, hasDeletions);
    }

    /// Returns the effective deletion of a branch covered by `outer`, where `inner` applies. Mirrors the result of
    /// applying the deletions in the order they are applied when building the trie representation, where the outer
    /// deletion takes precedence if the two have the same deletion time.
    private static TrieTombstoneMarker.Covering effective(TrieTombstoneMarker.Covering outer, TrieTombstoneMarker.Covering inner)
    {
        if (inner == null || outer != null && !inner.supersedes(outer))
            return outer;
        return inner;
    }

    private static TrieTombstoneMarker.Covering covering(DeletionTime deletionTime, TrieTombstoneMarker.Kind kind)
    {
        return deletionTime.isLive() ? null : TrieTombstoneMarker.covering(deletionTime, kind);
    }

    /// Returns the marker for a boundary with the given sides and level marker, or null if this is not a boundary.
    private static TrieTombstoneMarker boundary(TrieTombstoneMarker.Covering left,
                                                TrieTombstoneMarker.Covering right,
                                                TrieTombstoneMarker.LevelMarker levelMarker)
    {
        TrieTombstoneMarker marker = TrieTombstoneMarker.make(left, right, levelMarker);
        return marker != null && marker.isBoundary() ? marker : null;
    }

    private enum Stage
    {
        PARTITION_ROOT, STATIC_ROW, UNFILTERED, ROW, PARTITION_RETURN, DONE
    }

    private enum RowStage
    {
        ROOT, COLUMNS, COMPLEX_CELLS, COMPLEX_RETURN, RETURN
    }

    private static class ViewCursor extends EntryStreamCursor<Object, TrieTombstoneMarker>
    {
        final boolean forward;
        final TableMetadata metadata;
        final ClusteringComparator comparator;
        final UnfilteredRowIterator iterator;
        final int partitionKeyLength;
        final boolean hasDeletions;
        final TrieTombstoneMarker.Covering partitionDeletion;
        /// The range tombstone that is open at the current point in the iteration.
        TrieTombstoneMarker.Covering rangeDeletion;

        Stage stage;
        Stage stageAfterRow;
        /// The minimum length of the path prefix that has not changed since the last entry.
        int pathUnchangedSinceEntry;

        // State of the current row.
        RowStage rowStage;
        Object2IntHashMap<ColumnIdentifier> columnIds;
        int rowPathLength;
        Object rowContent;
        TrieTombstoneMarker.Covering rowDeletion;
        TrieTombstoneMarker.Covering rowEffectiveDeletion;
        TrieTombstoneMarker rowStartMarker;
        TrieTombstoneMarker rowEndMarker;
        ColumnData[] columns = new ColumnData[16];
        int columnCount;
        int columnIndex;

        // State of the current complex column.
        ColumnMetadata complexColumn;
        int complexPathLength;
        Iterator<Cell<?>> complexCells;
        DeletionTime complexCellDeletion;
        TrieTombstoneMarker complexEndMarker;

        ViewCursor(PartitionUpdate update, Direction direction, boolean hasDeletions)
        {
            super(direction, BYTE_COMPARABLE_VERSION);
            this.forward = direction.isForward();
            this.metadata = update.metadata();
            this.comparator = metadata.comparator;
            this.iterator = forward ? update.unfilteredIterator()
                                    : update.unfilteredIterator(ColumnFilter.all(metadata), Slices.ALL, true);
            this.hasDeletions = hasDeletions;
            this.partitionDeletion = covering(iterator.partitionLevelDeletion(), TrieTombstoneMarker.Kind.PARTITION);
            this.rangeDeletion = null;
            appendToPath(iterator.partitionKey().asComparableBytes(BYTE_COMPARABLE_VERSION));
            this.partitionKeyLength = pathLength;
            this.pathUnchangedSinceEntry = 0;
            this.stage = Stage.PARTITION_ROOT;
            start();
        }

        @Override
        public void close()
        {
            iterator.close();
        }

        /// Truncate the path to the given length, in preparation for appending new bytes.
        private void truncatePath(int length)
        {
            pathLength = length;
            pathUnchangedSinceEntry = Math.min(pathUnchangedSinceEntry, length);
        }

        private void appendBytes(ByteSource bytes)
        {
            pathUnchangedSinceEntry = Math.min(pathUnchangedSinceEntry, pathLength);
            appendToPath(bytes);
        }

        /// Complete the preparation of an entry.
        private boolean entry()
        {
            unchangedPathPrefix = pathUnchangedSinceEntry;
            pathUnchangedSinceEntry = pathLength;
            return true;
        }

        private boolean returnPathEntry(int length, TrieTombstoneMarker deletion)
        {
            truncatePath(length);
            entryOnReturnPath = true;
            entryDeletion = deletion;
            return entry();
        }

        @Override
        protected boolean nextEntry()
        {
            while (true)
            {
                switch (stage)
                {
                    case PARTITION_ROOT:
                        stage = forward ? Stage.STATIC_ROW : Stage.UNFILTERED;
                        truncatePath(partitionKeyLength);
                        entryContent = PARTITION_MARKER;
                        entryDeletion = forward ? boundary(null, partitionDeletion, null)
                                                : boundary(partitionDeletion, null, null);
                        entryIsDeletionBranchRoot = hasDeletions;
                        return entry();
                    case STATIC_ROW:
                    {
                        Stage next = forward ? Stage.UNFILTERED : Stage.PARTITION_RETURN;
                        Row staticRow = iterator.staticRow();
                        if (staticRow.isEmpty())
                        {
                            stage = next;
                            break;
                        }
                        startRow(staticRow, next);
                        break;
                    }
                    case UNFILTERED:
                    {
                        if (!iterator.hasNext())
                        {
                            stage = forward ? Stage.PARTITION_RETURN : Stage.STATIC_ROW;
                            break;
                        }
                        Unfiltered unfiltered = iterator.next();
                        if (unfiltered.isRow())
                            startRow((Row) unfiltered, Stage.UNFILTERED);
                        else if (rangeTombstoneEntry((RangeTombstoneMarker) unfiltered))
                            return true;
                        break;
                    }
                    case ROW:
                        if (nextRowEntry())
                            return true;
                        stage = stageAfterRow;
                        break;
                    case PARTITION_RETURN:
                        stage = Stage.DONE;
                        if (partitionDeletion != null)
                            return returnPathEntry(partitionKeyLength,
                                                   forward ? boundary(partitionDeletion, null, null)
                                                           : boundary(null, partitionDeletion, null));
                        break;
                    case DONE:
                        return false;
                    default:
                        throw new AssertionError();
                }
            }
        }

        private boolean rangeTombstoneEntry(RangeTombstoneMarker marker)
        {
            boolean reversed = !forward;
            TrieTombstoneMarker.Covering before = rangeDeletion;
            rangeDeletion = marker.isOpen(reversed)
                            ? covering(marker.openDeletionTime(reversed), TrieTombstoneMarker.Kind.RANGE)
                            : null;
            TrieTombstoneMarker.Covering left = effective(partitionDeletion, forward ? before : rangeDeletion);
            TrieTombstoneMarker.Covering right = effective(partitionDeletion, forward ? rangeDeletion : before);
            TrieTombstoneMarker deletion = boundary(left, right, null);
            if (deletion == null)
                return false;

            truncatePath(partitionKeyLength);
            appendBytes(comparator.asByteComparable(marker.clustering()).asComparableBytes(BYTE_COMPARABLE_VERSION));
            entryDeletion = deletion;
            // Range tombstone boundaries are placed before their (empty) branch, which in reverse is the return path.
            entryOnReturnPath = reversed;
            return entry();
        }

        private void startRow(Row row, Stage stageAfterRow)
        {
            this.stage = Stage.ROW;
            this.stageAfterRow = stageAfterRow;
            this.rowStage = RowStage.ROOT;
            boolean isStatic = row.isStatic();
            columnIds = TrieBackedRow.columnIdsMap(isStatic ? metadata.staticColumns() : metadata.regularColumns());

            truncatePath(partitionKeyLength);
            appendBytes(comparator.asByteComparable(row.clustering()).asComparableBytes(BYTE_COMPARABLE_VERSION));
            rowPathLength = pathLength;

            rowDeletion = covering(row.deletion().time(), TrieTombstoneMarker.Kind.ROW);
            boolean hasComplexDeletion = row.hasComplexDeletion();
            TrieTombstoneMarker.Covering outerDeletion = effective(partitionDeletion, rangeDeletion);
            rowEffectiveDeletion = effective(outerDeletion, rowDeletion);

            columnCount = 0;
            for (ColumnData cd : row)
            {
                if (columnCount == columns.length)
                    columns = Arrays.copyOf(columns, columnCount * 2);
                columns[columnCount++] = cd;
            }
            columnIndex = forward ? 0 : columnCount - 1;

            // Rows with deletions have row-level markers on both sides. If the row's deletion does not change the
            // applicable deletion, the trie representation only keeps the markers if there are deletion boundaries
            // below the row (see TrieBackedRow.shouldPreserveContentWithoutChildren).
            if ((rowDeletion != null || hasComplexDeletion) &&
                (!Objects.equals(outerDeletion, rowEffectiveDeletion) || hasComplexDeletion && hasComplexBoundaries()))
            {
                rowStartMarker = boundary(outerDeletion, rowEffectiveDeletion, TrieTombstoneMarker.LevelMarker.ROW);
                rowEndMarker = boundary(rowEffectiveDeletion, outerDeletion, TrieTombstoneMarker.LevelMarker.ROW);
            }
            else
                rowStartMarker = rowEndMarker = null;

            LivenessInfo liveness = row.primaryKeyLivenessInfo();
            if (!liveness.isEmpty())
                rowContent = liveness;
            else
                rowContent = hasSurvivingCells() ? LivenessInfo.EMPTY : null;
        }

        /// Returns true if any of the row's complex deletions changes the applicable deletion, i.e. results in
        /// boundaries.
        private boolean hasComplexBoundaries()
        {
            for (int i = 0; i < columnCount; ++i)
            {
                ColumnData cd = columns[i];
                if (cd.column().isComplex())
                {
                    DeletionTime complexDeletion = ((ComplexColumnData) cd).complexDeletion();
                    if (!complexDeletion.isLive() &&
                        (rowEffectiveDeletion == null || complexDeletion.supersedes(rowEffectiveDeletion)))
                        return true;
                }
            }
            return false;
        }

        private boolean hasSurvivingCells()
        {
            for (int i = 0; i < columnCount; ++i)
            {
                ColumnData cd = columns[i];
                if (cd.column().isSimple())
                {
                    if (!deletes(rowDeletion, (Cell<?>) cd))
                        return true;
                }
                else
                {
                    ComplexColumnData ccd = (ComplexColumnData) cd;
                    if (hasSurvivingCells(ccd, complexCellDeletion(ccd)))
                        return true;
                }
            }
            return false;
        }

        private static boolean hasSurvivingCells(ComplexColumnData ccd, DeletionTime deletion)
        {
            if (deletion == null)
                return ccd.cellsCount() > 0;
            for (Cell<?> cell : ccd)
            {
                if (!deletion.deletes(cell))
                    return true;
            }
            return false;
        }

        private static boolean deletes(DeletionTime deletion, Cell<?> cell)
        {
            return deletion != null && deletion.deletes(cell);
        }

        /// The deletion that applies to the cells of the given complex column within the row.
        private DeletionTime complexCellDeletion(ComplexColumnData ccd)
        {
            DeletionTime complexDeletion = ccd.complexDeletion();
            if (complexDeletion.isLive())
                return rowDeletion;
            if (rowDeletion == null || complexDeletion.supersedes(rowDeletion))
                return complexDeletion;
            return rowDeletion;
        }

        private boolean nextRowEntry()
        {
            while (true)
            {
                switch (rowStage)
                {
                    case ROOT:
                    {
                        rowStage = RowStage.COLUMNS;
                        TrieTombstoneMarker deletion = forward ? rowStartMarker : rowEndMarker;
                        if (rowContent == null && deletion == null)
                            break;
                        truncatePath(rowPathLength);
                        entryContent = rowContent;
                        entryDeletion = deletion;
                        return entry();
                    }
                    case COLUMNS:
                        if (forward ? columnIndex >= columnCount : columnIndex < 0)
                        {
                            rowStage = RowStage.RETURN;
                            break;
                        }
                        ColumnData cd = columns[forward ? columnIndex++ : columnIndex--];
                        if (columnEntry(cd))
                            return true;
                        break;
                    case COMPLEX_CELLS:
                        while (complexCells.hasNext())
                        {
                            Cell<?> cell = complexCells.next();
                            if (deletes(complexCellDeletion, cell))
                                continue;
                            truncatePath(complexPathLength);
                            appendBytes(TrieBackedRow.cellPathKey(complexColumn, cell.path(), BYTE_COMPARABLE_VERSION));
                            entryContent = cell;
                            return entry();
                        }
                        rowStage = RowStage.COMPLEX_RETURN;
                        break;
                    case COMPLEX_RETURN:
                        rowStage = RowStage.COLUMNS;
                        if (complexEndMarker != null)
                            return returnPathEntry(complexPathLength, complexEndMarker);
                        break;
                    case RETURN:
                        stage = stageAfterRow;
                        rowStage = null;
                        TrieTombstoneMarker deletion = forward ? rowEndMarker : rowStartMarker;
                        if (deletion != null)
                            return returnPathEntry(rowPathLength, deletion);
                        return false;
                    default:
                        throw new AssertionError();
                }
            }
        }

        /// Prepare the entry for the given column, returning false if the column has no entry at its root.
        private boolean columnEntry(ColumnData cd)
        {
            ColumnMetadata column = cd.column();
            int id = columnIds.getValue(column.name);
            if (id < 0)
                throw new IllegalArgumentException(String.format("Column %s not present in row columns set.", column));

            if (column.isSimple())
            {
                Cell<?> cell = (Cell<?>) cd;
                if (deletes(rowDeletion, cell))
                    return false;
                truncatePath(rowPathLength);
                appendBytes(ByteSource.variableLengthUnsignedInteger(id));
                entryContent = cell;
                return entry();
            }

            ComplexColumnData ccd = (ComplexColumnData) cd;
            truncatePath(rowPathLength);
            appendBytes(ByteSource.variableLengthUnsignedInteger(id));
            complexPathLength = pathLength;
            complexColumn = column;
            complexCellDeletion = complexCellDeletion(ccd);
            complexCells = forward ? ccd.iterator() : ccd.reverseIterator();
            rowStage = RowStage.COMPLEX_CELLS;

            TrieTombstoneMarker.Covering complexDeletion = covering(ccd.complexDeletion(), TrieTombstoneMarker.Kind.COLUMN);
            TrieTombstoneMarker.Covering complexEffectiveDeletion = effective(rowEffectiveDeletion, complexDeletion);
            TrieTombstoneMarker startMarker = boundary(rowEffectiveDeletion, complexEffectiveDeletion, null);
            TrieTombstoneMarker endMarker = boundary(complexEffectiveDeletion, rowEffectiveDeletion, null);
            complexEndMarker = forward ? endMarker : startMarker;
            Object content = hasSurvivingCells(ccd, complexCellDeletion) ? TrieBackedRow.COMPLEX_COLUMN_MARKER : null;
            TrieTombstoneMarker deletion = forward ? startMarker : endMarker;
            if (content == null && deletion == null)
                return false;
            entryContent = content;
            entryDeletion = deletion;
            return entry();
        }
    }
}
