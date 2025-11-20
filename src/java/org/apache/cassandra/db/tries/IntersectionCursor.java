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

package org.apache.cassandra.db.tries;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// The implementation of the intersection of a trie with a set
abstract class IntersectionCursor<T, C extends Cursor<T>> implements Cursor<T>
{
    enum State
    {
        /// The exact position is inside the set, source and set cursors are at the same position.
        MATCHING,
        /// The set cursor is ahead; the current position, as well as any before the set cursor's are inside the set.
        SET_AHEAD
    }

    final C source;
    final TrieSetCursor set;
    State state;

    IntersectionCursor(C source, TrieSetCursor set)
    {
        this.source = source;
        this.set = set;
        setInitialState();
    }

    @Override
    public long encodedPosition()
    {
        return source.encodedPosition();
    }

    @Override
    public long advance()
    {
        if (state == State.SET_AHEAD)
            return advanceInCoveredBranch(set.encodedPosition(), source.advance());

        return advanceWhenMatching();
    }

    @Override
    public long advanceMultiple(Cursor.TransitionsReceiver receiver)
    {
        // We can only apply advanceMultiple if we are fully inside a covered branch.
        if (state == State.SET_AHEAD)
            return advanceInCoveredBranch(set.encodedPosition(), source.advanceMultiple(receiver));

        return advanceWhenMatching();
    }

    private long advanceWhenMatching()
    {
        // The set is assumed sparser, so we advance that first.
        long setPosition = set.advance();
        if (set.precedingIncluded())
            return advanceInCoveredBranch(setPosition, source.advance());
        else
            return advanceSourceToIntersection(setPosition);
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        if (state == State.SET_AHEAD)
            return advanceInCoveredBranch(set.encodedPosition(), source.skipTo(encodedSkipPosition));

        long setPosition = set.skipTo(encodedSkipPosition);
        if (set.precedingIncluded())
            return advanceInCoveredBranch(setPosition, source.skipTo(encodedSkipPosition));
        else
            return advanceSourceToIntersection(setPosition);
    }

    private long advanceInCoveredBranch(long setPosition, long sourcePosition)
    {
        // Check if the advanced source is still in the covered area.
        long cmp = Cursor.compare(sourcePosition, setPosition);
        if (cmp < 0)    // source is strictly before set position
            return coveredAreaWithSetAhead(sourcePosition);
        if (Cursor.isExhausted(sourcePosition))
            return exhausted(sourcePosition);

        if (cmp == 0)
            return matchingPosition(sourcePosition);

        // Source moved beyond the set position. Advance the set too.
        setPosition = set.skipTo(sourcePosition);
        if (Cursor.compare(setPosition, sourcePosition) == 0)
            return matchingPosition(sourcePosition);

        // At this point set is ahead. Check content to see if we are in a covered branch.
        // If not, we need to skip the source as well and repeat the process.
        if (set.precedingIncluded())
            return coveredAreaWithSetAhead(sourcePosition);
        else
            return advanceSourceToIntersection(setPosition);
    }

    private long advanceSourceToIntersection(long setPosition)
    {
        while (true)
        {
            // Set is ahead of source, but outside the covered area. Skip source to the set's position.
            long sourcePosition = source.skipTo(setPosition);
            if (Cursor.isExhausted(sourcePosition))
                return exhausted(sourcePosition);
            if (Cursor.compare(setPosition, sourcePosition) == 0)
                return matchingPosition(sourcePosition);

            // Source is now ahead of the set.
            setPosition = set.skipTo(sourcePosition);
            if (Cursor.compare(setPosition, sourcePosition) == 0)
                return matchingPosition(sourcePosition);

            // At this point set is ahead. Check content to see if we are in a covered branch.
            if (set.precedingIncluded())
                return coveredAreaWithSetAhead(sourcePosition);
        }
    }

    private long coveredAreaWithSetAhead(long encodedPosition)
    {
        state = State.SET_AHEAD;
        return encodedPosition;
    }

    long matchingPosition(long encodedPosition)
    {
        // If we are matching a boundary of the set, include all its children by using a set-ahead state, ensuring that
        // the set will only be advanced once the source ascends to its depth again.
        state = State.MATCHING;
        return encodedPosition;
    }

    void setInitialState()
    {
        matchingPosition(encodedPosition());
    }

    private long exhausted(long position)
    {
        state = State.MATCHING;
        return position;
    }

    @Override
    public T content()
    {
        return source.content();
    }

    @Override
    public Direction direction()
    {
        return source.direction();
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return source.byteComparableVersion();
    }

    /// A variation of the intersection cursor that supports boundary inclusivity control and does not report content
    /// in prefixes.
    ///
    /// Note: Exclusivity is ignored for empty bounds, i.e. if a boundary is empty, it is treated like null regardless
    /// of the inclusivity flag.
    abstract static class Slice<T, C extends Cursor<T>> extends IntersectionCursor<T, C>
    {
        Slice(C source, TrieSetCursor set)
        {
            super(source, set);
        }

        @Override
        public T content()
        {
            switch (state)
            {
                case SET_AHEAD:
                    return source.content();
                case MATCHING:
                    // If this is a prefix, report if it leads to an end bound.
                    if (set.state().isBoundary)
                        return set.state().applicableAfter ? source.content() : null;
                    return set.state().applicableBefore ? source.content() : null;
                default:
                    throw new AssertionError();
            }
        }
    }

    /// Intersection cursor for [Trie].
    static class Plain<T> extends IntersectionCursor<T, Cursor<T>>
    {
        public Plain(Cursor<T> source, TrieSetCursor set)
        {
            super(source, set);
        }

        @Override
        public Cursor<T> tailCursor(Direction direction)
        {
            switch (state)
            {
                case MATCHING:
                    return new Plain<>(source.tailCursor(direction), set.tailCursor(direction));
                case SET_AHEAD:
                    return source.tailCursor(direction);
                default:
                    throw new AssertionError();
            }
        }
    }

    /// Slice cursor for [Trie].
    static class PlainSlice<T> extends Slice<T, Cursor<T>>
    {
        public PlainSlice(Cursor<T> source, TrieSetCursor set)
        {
            super(source, set);
        }

        @Override
        public Cursor<T> tailCursor(Direction direction)
        {
            switch (state)
            {
                case MATCHING:
                    return new PlainSlice<>(source.tailCursor(direction), set.tailCursor(direction));
                case SET_AHEAD:
                    return source.tailCursor(direction);
                default:
                    throw new AssertionError();
            }
        }
    }

    static class DeletionAware<T, D extends RangeState<D>>
    extends IntersectionCursor<T, DeletionAwareCursor<T, D>>
    implements DeletionAwareCursor<T, D>
    {
        RangeCursor<D> applicableDeletionBranch;

        public DeletionAware(DeletionAwareCursor<T, D> source, TrieSetCursor set)
        {
            super(source, set);
            applicableDeletionBranch = null;
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            switch (state)
            {
                case MATCHING:
                    return new DeletionAware<>(source.tailCursor(direction), set.tailCursor(direction));
                case SET_AHEAD:
                    return source.tailCursor(direction);
                default:
                    throw new AssertionError();
            }
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            RangeCursor<D> deletions = source.deletionBranchCursor(direction);
            if (deletions == null)
                return null;

            switch (state)
            {
                case SET_AHEAD:
                    // Since the deletion branch cannot extend outside this branch, it is fully covered by the set.
                    return deletions;
                case MATCHING:
                    return new RangeIntersectionCursor<>(deletions,
                                                         set.tailCursor(direction));
                default:
                    throw new AssertionError();
            }
        }
    }
}
