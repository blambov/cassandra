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
 * Intersection implementation which works with {@code Trie<Contained>} as the intersecting set.
 * <p>
 * Implemented as a parallel walk over the two tries, skipping over the parts which are not in the set, and skipping
 * advancing in the set while we know branches are fully contained within the set (i.e. when its {@code content()}
 * returns FULLY). To do the latter we always examing the set's content, and on receiving FULLY we store the set's
 * current depth as the lowest depth we need to see on a source advance to be outside of the covered branch. This
 * allows us to implement {@code advanceMultiple}, and to have a very low overhead on all advances inside the covered
 * branch.
 * <p>
 * As it is expected that the structure of the set is going to be simpler, unless we know that we are inside a fully
 * covered branch we advance the set first, and skip in the source to the advanced position.
 */
public class IntersectionTrie<T> extends Trie<T>
{
    final Trie<T> trie;
    final Trie<Contained> set;

    public IntersectionTrie(Trie<T> trie, Trie<Contained> set)
    {
        this.trie = trie;
        this.set = set;
    }

    @Override
    protected Cursor<T> cursor()
    {
        return new IntersectionCursor<>(trie.cursor(), set.cursor());
    }

    enum State
    {
        OUTSIDE_MATCHING,
        INSIDE_MATCHING,
        INSIDE_SET_AHEAD;
    }

    static class IntersectionCursor<T> implements Cursor<T>
    {
        final Cursor<T> source;
        final Cursor<Contained> set;
        State state;

        public IntersectionCursor(Cursor<T> source, Cursor<Contained> set)
        {
            this.source = source;
            this.set = set;
            onMatchingPosition(depth());
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

        private int advanceInCoveredBranch(int setDepth, int advancedSourceDepth)
        {
            // Check if the advanced source is still in the covered area.
            if (advancedSourceDepth > setDepth) // most common fast path
                return onCoveredAreaWithSetAhead(advancedSourceDepth);
            if (advancedSourceDepth < 0)
                return advancedSourceDepth; // exhausted
            int sourceTransition = source.incomingTransition();
            if (advancedSourceDepth == setDepth)
            {
                int setTransition = set.incomingTransition();
                if (sourceTransition < setTransition)
                    return onCoveredAreaWithSetAhead(advancedSourceDepth);
                if (sourceTransition == setTransition)
                    return onMatchingPosition(advancedSourceDepth);
            }

            // Source moved beyond the set position. Advance the set too.
            setDepth = set.skipTo(advancedSourceDepth, sourceTransition);
            int setTransition = set.incomingTransition();
            if (setDepth == advancedSourceDepth && setTransition == sourceTransition)
                return onMatchingPosition(advancedSourceDepth);

            // At this point set is ahead. Check content to see if we are in a covered branch.
            // If not, we need to skip the source as well and repeat the process.
            if (inSetCoveredArea())
                return onCoveredAreaWithSetAhead(advancedSourceDepth);
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
                    return sourceDepth;
                if (sourceDepth == setDepth && sourceTransition == setTransition)
                    return onMatchingPosition(setDepth);

                // Source is now ahead of the set.
                setDepth = set.skipTo(sourceDepth, sourceTransition);
                setTransition = set.incomingTransition();
                if (setDepth == sourceDepth && setTransition == sourceTransition)
                    return onMatchingPosition(setDepth);

                // At this point set is ahead. Check content to see if we are in a covered branch.
                if (inSetCoveredArea())
                    return onCoveredAreaWithSetAhead(sourceDepth);
            }
        }

        private boolean inSetCoveredArea()
        {
            Contained contained = set.content();
            return (contained == Contained.INSIDE_PREFIX || contained == Contained.END);
        }

        private int onCoveredAreaWithSetAhead(int depth)
        {
            state = State.INSIDE_SET_AHEAD;
            return depth;
        }

        private int onMatchingPosition(int depth)
        {
            Contained contained = set.content();
            if (contained == Contained.START || contained == Contained.INSIDE_PREFIX)
                state = State.INSIDE_MATCHING;
            else
                state = State.OUTSIDE_MATCHING;
            return depth;
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
