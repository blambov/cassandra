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
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Predicate;

import com.google.common.base.Predicates;
import com.google.common.collect.SortedSetMultimap;

import org.agrona.collections.Object2IntHashMap;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.ColumnSubselection;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryRangeTrie;
import org.apache.cassandra.db.tries.RangeState;
import org.apache.cassandra.db.tries.RangeTrie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public class TrieColumnFilter
{
    static abstract class FilterState implements RangeState<FilterState>
    {
        abstract boolean included();

        abstract boolean dropsValue();

        abstract TrieTombstoneMarker.Covering applicableDeletion();

        abstract FilterState mergeWith(FilterState existing);
    }

    static class Covering extends FilterState
    {
        final boolean included;
        final boolean dropsValue;
        final TrieTombstoneMarker.Covering applicableDeletion;

        Covering(boolean included, boolean dropsValue, TrieTombstoneMarker.Covering applicableDeletion)
        {
            this.included = included;
            this.dropsValue = dropsValue;
            this.applicableDeletion = applicableDeletion;
        }

        @Override
        boolean included()
        {
            return included;
        }

        @Override
        boolean dropsValue()
        {
            return dropsValue;
        }

        @Override
        TrieTombstoneMarker.Covering applicableDeletion()
        {
            return applicableDeletion;
        }

        @Override
        FilterState mergeWith(FilterState existing)
        {
            if (existing == null)
                return this;
            if (existing instanceof Header)
                return new Boundary((Header) existing, this, this, this);
            if (existing instanceof Boundary)
                return existing.mergeWith(this);
            return mergeWithCovering((Covering) existing);
        }

        private Covering mergeWithCovering(Covering other)
        {
            if (other == null)
                return this;
            TrieTombstoneMarker.Covering newDeletion = TrieTombstoneMarker.combine(applicableDeletion, other.applicableDeletion);
            return new Covering(included | other.included, dropsValue | other.dropsValue, newDeletion);
        }

        @Override
        public boolean isBoundary()
        {
            return false;
        }

        @Override
        public Covering precedingState(Direction direction)
        {
            return this;
        }

        @Override
        public Covering succedingState(Direction direction)
        {
            return this;
        }

        @Override
        public FilterState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            throw new AssertionError();
        }

        @Override
        public FilterState asBoundary(Direction direction)
        {
            return direction.isForward() ? new Boundary(null, this, this) : new Boundary(this, null, this);
        }

        @Override
        public String toString()
        {
            if (applicableDeletion != null)
                return (dropsValue ? "D " : (included ? "I " : "")) + applicableDeletion;
            if (dropsValue)
                return "DROPPED_VALUE";
            if (included)
                return "INCLUDED";
            return "UNEFFECTIVE";
        }
    }

    static class Header extends FilterState
    {
        @Override
        boolean included()
        {
            return false;
        }

        @Override
        boolean dropsValue()
        {
            return false;
        }

        @Override
        TrieTombstoneMarker.Covering applicableDeletion()
        {
            return null;
        }

        @Override
        FilterState mergeWith(FilterState existing)
        {
            if (existing == null)
                return this;
            return existing.mergeWith(this);
        }

        @Override
        public boolean isBoundary()
        {
            return true;
        }

        @Override
        public FilterState precedingState(Direction direction)
        {
            return null;
        }

        @Override
        public FilterState succedingState(Direction direction)
        {
            return null;
        }

        @Override
        public FilterState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            return this;
        }

        @Override
        public FilterState asBoundary(Direction direction)
        {
            throw new AssertionError();
        }

        @Override
        public String toString()
        {
            return "Header";
        }
    }

    static class Boundary extends FilterState
    {
        final Header header;
        final Covering left;
        final Covering right;
        final Covering appliesToPoint;

        Boundary(Covering left, Covering right, Covering appliesToPoint)
        {
            this(null, left, right, appliesToPoint);
        }

        Boundary(Header header, Covering left, Covering right, Covering appliesToPoint)
        {
            this.header = header;
            this.left = left;
            this.right = right;
            this.appliesToPoint = appliesToPoint;
            assert appliesToPoint != null;
            assert appliesToPoint == left || appliesToPoint == right;
        }

        @Override
        boolean included()
        {
            return appliesToPoint.included();
        }

        @Override
        boolean dropsValue()
        {
            return appliesToPoint.dropsValue();
        }

        @Override
        TrieTombstoneMarker.Covering applicableDeletion()
        {
            return appliesToPoint.applicableDeletion();
        }

        @Override
        FilterState mergeWith(FilterState existing)
        {
            if (existing == null)
                return this;
            if (existing instanceof Header)
            {
                Header other = (Header) existing;
                if (this.header == other)
                    return this;
                else
                    return new Boundary(other, left, right, appliesToPoint);
            }
            if (existing instanceof Covering)
            {
                Covering other = (Covering) existing;
                Covering l = other.mergeWithCovering(left);
                Covering r = other.mergeWithCovering(right);
                if (l == left && r == right)
                    return this;
                if (l == r)
                    return header;
                return new Boundary(header, l, r, left == appliesToPoint ? l : r);
            }
            Boundary other = (Boundary) existing;
            Covering l = left != null ? left.mergeWithCovering(other.left) : other.left;
            Covering r = right != null ? right.mergeWithCovering(other.right) : other.right;
            Header h = header != null ? header : other.header;
            if (l == r)
                return h;
            if (l == left && r == right && h == header)
                return this;
            return new Boundary(header, l, r, left == appliesToPoint ? l : r);
        }

        @Override
        public boolean isBoundary()
        {
            return true;
        }

        @Override
        public FilterState precedingState(Direction direction)
        {
            return direction.select(left, right);
        }

        @Override
        public FilterState succedingState(Direction direction)
        {
            return direction.select(right, left);
        }

        @Override
        public FilterState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            Covering l = applicableBefore ? left : null;
            Covering r = applicableAfter ? right : null;
            if (l == null && r == null)
                return header;
            if (l == left && r == right)
                return this;
            return new Boundary(header, l, r, left == appliesToPoint ? l : r);
        }

        @Override
        public FilterState asBoundary(Direction direction)
        {
            throw new AssertionError();
        }

        @Override
        public String toString()
        {
            return "Boundary{" +
                   (header != null ? "Header + " : "") +
                   left + "->" + right + '}';
        }
    }

    static final Covering INCLUDED = new Covering(true, false, null);
    static final Boundary INCLUDED_START = new Boundary(null, INCLUDED, INCLUDED);
    static final Boundary INCLUDED_END = new Boundary(INCLUDED, null, INCLUDED);
    static final Covering DROP_VALUE = new Covering(true, true, null);
    static final Boundary DROP_VALUE_START = new Boundary(null, DROP_VALUE, DROP_VALUE);
    static final Boundary DROP_VALUE_END = new Boundary(DROP_VALUE, null, DROP_VALUE);

    static final Header HEADER = new Header();

    static Covering deletion(long timestamp, long localDeletionTime)
    {
        return new Covering(false, false, TrieTombstoneMarker.covering(timestamp, localDeletionTime, TrieTombstoneMarker.Kind.COLUMN));
    }

    static FilterState addFilterState(FilterState existing, FilterState update)
    {
        return update.mergeWith(existing);
    }

    static FilterState addNew(FilterState existing, FilterState update)
    {
        assert existing == null;
        return update;
    }

    static boolean included(FilterState state)
    {
        return state != null && state.included();
    }

    static boolean included(TrieTombstoneMarker marker)
    {
        return marker != null;
    }

    static TrieTombstoneMarker resolve(TrieTombstoneMarker marker, FilterState state)
    {
        if (marker == null || state == null)
            return null;

        // If a deletion is in force, drop anything shadowed; otherwise, leave markers we happen upon unchanged
        TrieTombstoneMarker.Covering deletion = state.applicableDeletion();
        if (deletion != null)
            return marker.dropShadowed(deletion);
        else
            return marker;
    }

    static TrieTombstoneMarker resolveAndSetRowDeletion(TrieTombstoneMarker marker, FilterState state)
    {
        if (marker == null || state == null)
            return null;

        TrieTombstoneMarker.Covering deletion = state.applicableDeletion();
        if (deletion != null)
        {
            if (marker.hasLevelMarker(TrieTombstoneMarker.LevelMarker.ROW))
                return marker.mergeWith(deletion);
            else
                return marker.dropShadowed(deletion);
        }
        else
            return marker;
    }

    static Object resolve(Object data, FilterState state)
    {
        if (data == null || data == TrieBackedRow.COMPLEX_COLUMN_MARKER)
            return data;
        if (data instanceof LivenessInfo)
        {
            if (state == null)
                return data;
            TrieTombstoneMarker.Covering deletion = state.applicableDeletion();
            LivenessInfo li = (LivenessInfo) data;
            if (deletion == null || !deletion.deletes(li))
                return li;
            else
                return LivenessInfo.EMPTY;
        }
        if (state == null || !state.included())
            return null;

        TrieTombstoneMarker.Covering deletion = state.applicableDeletion();
        CellData<?, ?> cell = (CellData<?, ?>) data;
        if (deletion != null && deletion.deletes(cell))
            return null;
        if (state.dropsValue())
            cell = cell.withSkippedValue();
        return cell;
    }

    final InMemoryRangeTrie<FilterState> trie = InMemoryRangeTrie.shortLived(TrieBackedPartition.BYTE_COMPARABLE_VERSION);
    final InMemoryRangeTrie<FilterState>.Mutator<FilterState> mutator = trie.mutator(TrieColumnFilter::addFilterState, Predicates.alwaysFalse());
    final Columns columns;
    final Object2IntHashMap<ColumnIdentifier> columnIds;

    public TrieColumnFilter(Columns columns, Object2IntHashMap<ColumnIdentifier> columnIds, ColumnFilter filter, boolean isStatic, boolean mayFilterColumns) throws TrieSpaceExhaustedException
    {
        this.columns = columns;
        this.columnIds = columnIds;

        if (mayFilterColumns)
        {
            applyColumnFilter(filter, isStatic);

            // Add row-level before and after markers to preserve row-level deletion and row markers.
            trie.putRecursive(ByteComparable.EMPTY, HEADER, false, TrieColumnFilter::addNew);
            trie.putRecursive(ByteComparable.EMPTY, HEADER, true, TrieColumnFilter::addNew);
        }
        else
        {
            // include all
            trie.putRecursive(ByteComparable.EMPTY, INCLUDED_START, false, TrieColumnFilter::addNew);
            trie.putRecursive(ByteComparable.EMPTY, INCLUDED_END, true, TrieColumnFilter::addNew);
        }
    }

    public TrieColumnFilter(Columns columns, Object2IntHashMap<ColumnIdentifier> columnIds, Columns queriedColumns) throws TrieSpaceExhaustedException
    {
        this.columns = columns;
        this.columnIds = columnIds;

        for (ColumnMetadata column : queriedColumns)
        {
            int id = columnIds.get(column.name);
            if (id == TrieBackedRow.COLUMN_NOT_PRESENT)
                continue;
            ByteComparable columnKey = TrieBackedRow.encodeUnsignedInt(id);

            trie.putRecursive(columnKey, INCLUDED_START, false, TrieColumnFilter::addNew);
            trie.putRecursive(columnKey, INCLUDED_END, true, TrieColumnFilter::addNew);
        }

        // Add row-level before and after markers to preserve row-level deletion and row markers.
        trie.putRecursive(ByteComparable.EMPTY, HEADER, false, TrieColumnFilter::addNew);
        trie.putRecursive(ByteComparable.EMPTY, HEADER, true, TrieColumnFilter::addNew);
    }

    public void applyColumnFilter(ColumnFilter filter, boolean isStatic) throws TrieSpaceExhaustedException
    {
        Columns columns = filter.fetchedColumns().columns(isStatic);
        Predicate<ColumnMetadata> queriedByUserTester = filter.queriedColumns().columns(isStatic).inOrderInclusionTester();
        SortedSetMultimap<ColumnIdentifier, ColumnSubselection> subselections = filter.subSelections();

        for (ColumnMetadata column : columns)
        {
            int id = TrieBackedRow.columnId(columnIds, column);
            if (id == TrieBackedRow.COLUMN_NOT_PRESENT)
                continue;
            ByteComparable columnKey = TrieBackedRow.encodeUnsignedInt(id);

            if (queriedByUserTester.test(column))
            {
                SortedSet<ColumnSubselection> subselection = subselections != null ? subselections.get(column.name) : null;
                if (subselection != null && !subselection.isEmpty())
                {
                    for (ColumnSubselection ss : subselection)
                    {
                        trie.putRecursive(TrieBackedRow.cellKey(id, column, ss.startInclusive()), INCLUDED_START, false, TrieColumnFilter::addNew);
                        trie.putRecursive(TrieBackedRow.cellKey(id, column, ss.endInclusive()), INCLUDED_END, true, TrieColumnFilter::addNew);
                    }

                    // preserve column marker and deletion
                    trie.putRecursive(columnKey, HEADER, false, TrieColumnFilter::addNew);
                    trie.putRecursive(columnKey, HEADER, true, TrieColumnFilter::addNew);
                }
                else
                {
                    trie.putRecursive(columnKey, INCLUDED_START, false, TrieColumnFilter::addNew);
                    trie.putRecursive(columnKey, INCLUDED_END, true, TrieColumnFilter::addNew);
                }
            }
            else
            {
                trie.putRecursive(columnKey, DROP_VALUE_START, false, TrieColumnFilter::addNew);
                trie.putRecursive(columnKey, DROP_VALUE_END, true, TrieColumnFilter::addNew);
            }
        }
    }

    public void applyDroppedColumns(Map<ByteBuffer, DroppedColumn> droppedColumns, Columns fetchedColumns) throws TrieSpaceExhaustedException
    {
        // Filter dropped columns by adding a deletion with the drop time, so that data before the drop time is not
        // returned.
        for (ColumnMetadata c : fetchedColumns)
        {
            DroppedColumn dropped = droppedColumns.get(c.name.bytes);
            if (dropped != null)
            {
                ByteComparable columnKey = TrieBackedRow.columnKey(columnIds, c);
                Covering deletion = deletion(dropped.droppedTime, 0);
                mutator.apply(RangeTrie.branch(columnKey, TrieBackedPartition.BYTE_COMPARABLE_VERSION, deletion));
            }
        }
    }

    public void applyDeletion(DeletionTime deletionTime) throws TrieSpaceExhaustedException
    {
        Covering deletion = deletion(deletionTime.markedForDeleteAt(), deletionTime.localDeletionTime());
        mutator.apply(RangeTrie.branch(ByteComparable.EMPTY, TrieBackedPartition.BYTE_COMPARABLE_VERSION, deletion));
    }


    DeletionAwareTrie<Object, TrieTombstoneMarker> apply(DeletionAwareTrie<Object, TrieTombstoneMarker> data, boolean setActiveDeletionToRow)
    {
        return data.intersectWith(trie,
                                  TrieColumnFilter::included,
                                  TrieColumnFilter::included,
                                  TrieColumnFilter::resolve,
                                  setActiveDeletionToRow ? TrieColumnFilter::resolveAndSetRowDeletion : TrieColumnFilter::resolve);
    }
}
