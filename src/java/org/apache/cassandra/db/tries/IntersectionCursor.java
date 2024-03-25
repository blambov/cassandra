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

abstract class IntersectionCursor<C extends CursorWalkable.Cursor> implements CursorWalkable.Cursor
{
    enum State
    {
        /**
         * The exact position is outside the set, source and set cursors are at the same position.
         */
        OUTSIDE_MATCHING,
        /**
         * The exact position is inside the set, source and set cursors are at the same position.
         */
        INSIDE_MATCHING,
        /**
         * The set cursor is ahead; the current position, as well as any before the set cursor's are inside the set.
         */
        INSIDE_SET_AHEAD;
    }

    final C source;
    final TrieSetImpl.Cursor set;
    State state;

    public IntersectionCursor(C source, TrieSetImpl.Cursor set)
    {
        this.source = source;
        this.set = set;
        matchingPosition(depth());
    }

    public IntersectionCursor(IntersectionCursor<C> copyFrom, C withSource)
    {
        this.source = withSource;
        this.set = copyFrom.set.duplicate();
        this.state = copyFrom.state;
    }

    @Override
    public int depth()
    {
        return source.depth();
    }

    @Override
    public int incomingTransition()
    {
        return source.incomingTransition();
    }

    @Override
    public int advance()
    {
        if (state == State.INSIDE_SET_AHEAD)
            return advanceInCoveredBranch(set.depth(), source.advance());

        // The set is assumed sparser, so we advance that first.
        int setDepth = set.advance();
        if (inSetCoveredArea())
            return advanceInCoveredBranch(setDepth, source.advance());
        else
            return advanceSourceToIntersection(setDepth, set.incomingTransition());
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        if (state == State.INSIDE_SET_AHEAD)
            return advanceInCoveredBranch(set.depth(), source.skipTo(skipDepth, skipTransition));

        int setDepth = set.skipTo(skipDepth, skipTransition);
        if (inSetCoveredArea())
            return advanceInCoveredBranch(setDepth, source.skipTo(skipDepth, skipTransition));
        else
            return advanceSourceToIntersection(setDepth, set.incomingTransition());
    }

    @Override
    public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
    {
        if (state == State.INSIDE_SET_AHEAD)
            return advanceInCoveredBranch(set.depth(), source.advanceMultiple(receiver));

        int setDepth = set.advance();
        if (inSetCoveredArea())
            return advanceInCoveredBranch(setDepth, source.advance());
        else
            return advanceSourceToIntersection(setDepth, set.incomingTransition());
    }

    private int advanceInCoveredBranch(int setDepth, int sourceDepth)
    {
        // Check if the advanced source is still in the covered area.
        if (sourceDepth > setDepth) // most common fast path
            return coveredAreaWithSetAhead(sourceDepth);
        if (sourceDepth < 0)
            return exhausted();
        int sourceTransition = source.incomingTransition();
        if (sourceDepth == setDepth)
        {
            int setTransition = set.incomingTransition();
            if (sourceTransition < setTransition)
                return coveredAreaWithSetAhead(sourceDepth);
            if (sourceTransition == setTransition)
                return matchingPosition(sourceDepth);
        }

        // Source moved beyond the set position. Advance the set too.
        setDepth = set.skipTo(sourceDepth, sourceTransition);
        int setTransition = set.incomingTransition();
        if (setDepth == sourceDepth && setTransition == sourceTransition)
            return matchingPosition(sourceDepth);

        // At this point set is ahead. Check content to see if we are in a covered branch.
        // If not, we need to skip the source as well and repeat the process.
        if (inSetCoveredArea())
            return coveredAreaWithSetAhead(sourceDepth);
        else
            return advanceSourceToIntersection(setDepth, setTransition);
    }

    private int advanceSourceToIntersection(int setDepth, int setTransition)
    {
        while (true)
        {
            // Set is ahead of source, but outside the covered area. Skip source to the set's position.
            int sourceDepth = source.skipTo(setDepth, setTransition);
            int sourceTransition = source.incomingTransition();
            if (sourceDepth < 0)
                return exhausted();
            if (sourceDepth == setDepth && sourceTransition == setTransition)
                return matchingPosition(setDepth);

            // Source is now ahead of the set.
            setDepth = set.skipTo(sourceDepth, sourceTransition);
            setTransition = set.incomingTransition();
            if (setDepth == sourceDepth && setTransition == sourceTransition)
                return matchingPosition(setDepth);

            // At this point set is ahead. Check content to see if we are in a covered branch.
            if (inSetCoveredArea())
                return coveredAreaWithSetAhead(sourceDepth);
        }
    }

    private boolean inSetCoveredArea()
    {
        return set.state().lesserIncluded();
    }

    private int coveredAreaWithSetAhead(int depth)
    {
        state = State.INSIDE_SET_AHEAD;
        return depth;
    }

    private int matchingPosition(int depth)
    {
        state = set.state().matchingIncluded() ? State.INSIDE_MATCHING : State.OUTSIDE_MATCHING;
        return depth;
    }

    private int exhausted()
    {
        state = State.OUTSIDE_MATCHING;
        return -1;
    }

    static abstract class WithContent<T, C extends TrieImpl.Cursor<T>> extends IntersectionCursor<C> implements TrieImpl.Cursor<T>
    {
        public WithContent(C source, TrieSetImpl.Cursor set)
        {
            super(source, set);
        }

        public WithContent(IntersectionCursor<C> copyFrom, C withSource)
        {
            super(copyFrom, withSource);
        }

        @Override
        public T content()
        {
            return state != State.OUTSIDE_MATCHING
                   ? source.content()
                   : null;
        }
    }

    static class Deterministic<T> extends WithContent<T, TrieImpl.Cursor<T>>
    {
        public Deterministic(TrieImpl.Cursor<T> source, TrieSetImpl.Cursor set)
        {
            super(source, set);
        }

        public Deterministic(Deterministic<T> copyFrom)
        {
            super(copyFrom, copyFrom.source.duplicate());
        }

        @Override
        public Deterministic<T> duplicate()
        {
            return new Deterministic<>(this);
        }
    }


    static class NonDeterministic<T extends NonDeterministicTrie.Mergeable<T>>
    extends WithContent<T, NonDeterministicTrieImpl.Cursor<T>>
    implements NonDeterministicTrieImpl.Cursor<T>
    {
        public NonDeterministic(NonDeterministicTrieImpl.Cursor<T> source, TrieSetImpl.Cursor set)
        {
            super(source, set);
        }

        public NonDeterministic(NonDeterministic<T> copyFrom, NonDeterministicTrieImpl.Cursor<T> withSource)
        {
            super(copyFrom, withSource);
        }

        @Override
        public NonDeterministic<T> duplicate()
        {
            return new NonDeterministic<>(this, source.duplicate());
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> alternateBranch()
        {
            NonDeterministicTrieImpl.Cursor<T> alternate = source.alternateBranch();
            if (alternate == null)
                return null;
            return new NonDeterministic<>(this, alternate);
        }
    }

    static class DeletionAware<T, D extends RangeTrie.RangeMarker<D>>
    extends WithContent<T, DeletionAwareTrieImpl.Cursor<T, D>>
    implements DeletionAwareTrieImpl.Cursor<T, D>
    {
        RangeTrieImpl.Cursor<D> applicableDeletionBranch;

        public DeletionAware(DeletionAwareTrieImpl.Cursor<T, D> source, TrieSetImpl.Cursor set)
        {
            super(source, set);
            applicableDeletionBranch = null;
        }

        public DeletionAware(DeletionAware<T, D> copyFrom, DeletionAwareTrieImpl.Cursor<T, D> withSource)
        {
            super(copyFrom, withSource);
            applicableDeletionBranch = copyFrom.applicableDeletionBranch.duplicate();
        }

        @Override
        public DeletionAware<T, D> duplicate()
        {
            return new DeletionAware<>(this, source.duplicate());
        }

        @Override
        public RangeTrieImpl.Cursor<D> deletionBranch()
        {
            RangeTrieImpl.Cursor<D> deletions = source.deletionBranch();
            if (deletions == null)
                return null;
            return new RangeIntersectionCursor<>(RangeTrieImpl.rangeAndSetIntersectionController(), set.duplicate(), deletions);
        }
    }
}
