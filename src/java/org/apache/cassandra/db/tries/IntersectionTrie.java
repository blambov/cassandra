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

/**
 * Intersection implementation which works with {@code TrieSet} as the intersecting set.
 * <p>
 * Walks the two tries in parallel until it finds a matching position, or a skip to a position in the source takes us
 * to a region that is fully covered by the set. The trie set makes the latter known by returning true for
 * {@code contained().lesserInSet()} for a position. In that case we advance the source individually (also permitting
 * {@code advanceMultiple}) until the source reaches the set's position, after which we return to the parallel walk.
 * <p>
 * As it is expected that the structure of the set is going to be simpler, unless we know that we are inside a fully
 * covered branch we advance the set first, and skip in the source to the advanced position.
 */
public class IntersectionTrie<T> implements TrieWithImpl<T>
{
    final TrieWithImpl<T> trie;
    final TrieSetImpl set;

    public IntersectionTrie(TrieWithImpl<T> trie, TrieSetImpl set)
    {
        this.trie = trie;
        this.set = set;
    }

    @Override
    public Cursor<T> cursor()
    {
        return new IntersectionCursor<>(trie.cursor(), set.cursor());
    }

    enum State
    {
        /** The exact position is outside the set, source and set cursors are at the same position. */
        OUTSIDE_MATCHING,
        /** The exact position is inside the set, source and set cursors are at the same position. */
        INSIDE_MATCHING,
        /** The set cursor is ahead; the current position, as well as any before the set cursor's are inside the set. */
        INSIDE_SET_AHEAD;
    }

    static class IntersectionCursor<T> implements Cursor<T>
    {
        final Cursor<T> source;
        final TrieSetImpl.Cursor set;
        State state;

        public IntersectionCursor(Cursor<T> source, TrieSetImpl.Cursor set)
        {
            this.source = source;
            this.set = set;
            matchingPosition(depth());
        }

        public IntersectionCursor(IntersectionCursor<T> copyFrom, Cursor<T> withSource)
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
        public T content()
        {
            return state != State.OUTSIDE_MATCHING
                   ? source.content()
                   : null;
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
        public int advanceMultiple(TransitionsReceiver receiver)
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
            return set.contained().lesserInSet();
        }

        private int coveredAreaWithSetAhead(int depth)
        {
            state = State.INSIDE_SET_AHEAD;
            return depth;
        }

        private int matchingPosition(int depth)
        {
            state = set.contained().isInSet() ? State.INSIDE_MATCHING : State.OUTSIDE_MATCHING;
            return depth;
        }

        private int exhausted()
        {
            state = State.OUTSIDE_MATCHING;
            return -1;
        }

        @Override
        public Cursor<T> alternateBranch()
        {
            Cursor<T> alternate = source.alternateBranch();
            if (alternate == null)
                return null;
            return new IntersectionCursor<>(this, alternate);
        }

        @Override
        public Cursor<T> duplicate()
        {
            return new IntersectionCursor<>(this, source.duplicate());
        }
    }
}
