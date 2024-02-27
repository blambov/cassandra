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

public class DeletionAwareIntersectionTrie<T, D extends T> implements DeletionAwareTrieImpl<T, D>
{
    final DeletionAwareTrieImpl<T, D> trie;
    final TrieSetImpl set;

    public DeletionAwareIntersectionTrie(DeletionAwareTrieImpl<T, D> trie, TrieSetImpl set)
    {
        // If the source is already an intersection, intersect the sets. This easier to do than handling skipTo calls
        // in this cursor.
        if (trie instanceof DeletionAwareIntersectionTrie)
        {
            DeletionAwareIntersectionTrie<T, D> other = (DeletionAwareIntersectionTrie<T, D>) trie;
            trie = other.trie;
            set = new IntersectionTrieSet(set, other.set);
        }

        this.trie = trie;
        this.set = set;
    }

    @Override
    public Cursor<T> cursor()
    {
        return new DeletionAwareIntersectionCursor<>(trie.cursor(), set.cursor(), trie.deletionHandler());
    }

    @Override
    public DeletionHandler<T, D> deletionHandler()
    {
        return trie.deletionHandler();
    }

    static class DeletionAwareIntersectionCursor<T, D extends T> extends IntersectionTrie.IntersectionCursor<T>
    {
        final DeletionHandler<T, D> handler;

        public DeletionAwareIntersectionCursor(Cursor<T> source, TrieSetImpl.Cursor set, DeletionHandler<T, D> handler)
        {
            super(source, set);
            this.handler = handler;
        }

        public Cursor<T> alternateBranch()
        {
            Cursor<T> alternate = source.alternateBranch();
            if (alternate == null)
                return null;
            return new DeletionCursor<>(alternate, set.duplicate(), handler);
        }
    }

    enum CursorState
    {
        MATCHING_POSITION,
        SET_AHEAD,
        SOURCE_AHEAD
    }

    /**
     * In the alternate branch we must present covering deletions in the returned branch, at the boundaries defined by
     * the set.
     * We do this by checking if a (set-driven) skip jumps over the target position in the source, and if so look at
     * the first content value reachable from the source's state (using a duplicate of the cursor to be able to
     * correctly descend into it if any following set iteration is a match). If this content is a marker with a BEFORE
     * side, we return this (as AFTER side) at the left bound of the current branch of the set. We also keep track of
     * the active range deletion (i.e. for every marker we encounter during interation, we take its AFTER side) and
     * produce it (as a BEFORE side) when we encounter a set branch's right bound.
     * <p>
     * If we are inside the set, our view of it must be one step ahead, so that we can report its end correctly.
     */
    static class DeletionCursor<T, D extends T> implements Cursor<T>
    {
        private final Cursor<T> source;
        private final TrieSetImpl.Cursor set;
        final DeletionHandler<T, D> handler;

        CursorState state;
        int currentDepth;
        int currentTransition;
        D currentContent;

        D activeDeletion;  // set when we encounter a deletion marker and produced at the end of the set branch
        D coveringDeletion;  // set when source skips over a set branch, from the BEFORE side of the next marker


        public DeletionCursor(Cursor<T> source, TrieSetImpl.Cursor set, DeletionHandler<T, D> handler)
        {
            this.source = source;
            this.set = set;
            this.handler = handler;
            this.activeDeletion = null;
            this.coveringDeletion = null;
            assert set.depth() == source.depth() && set.incomingTransition() == source.incomingTransition();
            matchingPosition(source.depth(), source.incomingTransition());
        }

        DeletionCursor(DeletionCursor<T, D> copyFrom)
        {
            this.source = copyFrom.source.duplicate();
            this.set = copyFrom.set.duplicate();
            this.activeDeletion = copyFrom.activeDeletion;
            this.coveringDeletion = copyFrom.coveringDeletion;
            this.handler = copyFrom.handler;
            this.currentDepth = copyFrom.currentDepth;
            this.currentTransition = copyFrom.currentTransition;
            this.currentContent = copyFrom.currentContent;
            this.state = copyFrom.state;
        }

        @Override
        public int depth()
        {
            return currentDepth;
        }

        @Override
        public int incomingTransition()
        {
            return currentTransition;
        }

        @Override
        public D content()
        {
            return currentContent;
        }

        @Override
        public int advance()
        {
            switch (state)
            {
                case MATCHING_POSITION:
                {
                    // assume set is more restrictive
                    int setDepth = set.advance();
                    if (set.contained().lesserInSet())
                        return advanceInCoveredBranch(setDepth, source.advance());
                    else
                        return advanceSourceToIntersection(setDepth, set.incomingTransition());
                }
                case SET_AHEAD:
                    return advanceInCoveredBranch(set.depth(), source.advance());
                case SOURCE_AHEAD:
                    return advanceWithCoveringDeletion(set.advance());
                default:
                    throw new AssertionError();
            }
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            switch (state)
            {
                case MATCHING_POSITION:
                    return advance();
                case SET_AHEAD:
                    return advanceInCoveredBranch(set.depth(), source.advanceMultiple(receiver));
                case SOURCE_AHEAD:
                    return advanceWithCoveringDeletion(set.advance());  // TODO: maybe introduce advanceMultiple for set?
                default:
                    throw new AssertionError();
            }
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            throw new AssertionError("Intersections of intersections should be handled by set operations.");
        }

        private int advanceInCoveredBranch(int setDepth, int sourceDepth)
        {
            if (sourceDepth > setDepth) // most common fast path
                return atSourceSetAhead(sourceDepth, source.incomingTransition());
            if (sourceDepth < 0)
                return exhausted();
            int sourceTransition = source.incomingTransition();
            int setTransition = set.incomingTransition();
            if (sourceDepth == setDepth)
            {
                if (sourceTransition < setTransition)
                    return atSourceSetAhead(sourceDepth, sourceTransition);
                if (sourceTransition == setTransition)
                    return matchingPosition(sourceDepth, sourceTransition);
            }

            // Source moved ahead of the set. Before we can jump to that position, we must close any open deletion
            // at the next set END (and maybe reintroduce it at any following START).
            if (activeDeletion != null)
            {
                coveringDeletion = activeDeletion;
                return atSetSourceAhead(setDepth, setTransition);
            }

            // When there is no active deletion
            setDepth = set.skipTo(sourceDepth, sourceTransition);
            setTransition = set.incomingTransition();
            if (setDepth == sourceDepth && setTransition == sourceTransition)
                return matchingPosition(sourceDepth, sourceTransition);
            // set is ahead
            if (set.contained().lesserInSet())
                return atSourceSetAhead(sourceDepth, sourceTransition);
            else
                return advanceSourceToIntersection(setDepth, setTransition);
        }

        private int advanceWithCoveringDeletion(int setDepth)
        {
            assert coveringDeletion != null;
            int sourceDepth = source.depth();
            int setTransition = set.incomingTransition();
            if (setDepth > sourceDepth)
                return atSetSourceAhead(setDepth, setTransition);
            int sourceTransition = source.incomingTransition();
            if (setDepth == sourceDepth)
            {
                if (setTransition < sourceTransition)
                    return atSetSourceAhead(setDepth, setTransition);
                if (setTransition == sourceTransition)
                    return matchingPosition(sourceDepth, sourceTransition);
            }

            // Set has moved ahead of source.
            if (set.contained().lesserInSet())
                return atSourceSetAhead(sourceDepth, source.incomingTransition());
            else
                return advanceSourceToIntersection(setDepth, setTransition);
        }

        private int advanceSourceToIntersection(int setDepth, int setTransition)
        {
            assert activeDeletion == null : "Invalid state, active deletion should have been closed at range end.";
            while (true)
            {
                int sourceDepth = source.skipTo(setDepth, setTransition);
                if (sourceDepth < 0)
                    return exhausted();
                int sourceTransition = source.incomingTransition();
                if (sourceDepth == setDepth && sourceTransition == setTransition)
                    return matchingPosition(sourceDepth, sourceTransition); // easy case, source has a matching position

                // Source is ahead. We now need to check if a deletion is open at the nearest source position and, if so,
                // report it as covering.
                final D nextMarker = getClosestDeletion(source);
                if (nextMarker == null)
                    return exhausted();
                coveringDeletion = handler.asBound(nextMarker, BoundSide.BEFORE, BoundSide.AFTER);
                if (coveringDeletion != null)
                    return atSetSourceAhead(setDepth, setTransition);

                // No covering deletion. Advance set to match.
                setDepth = set.skipTo(sourceDepth, sourceTransition);
                setTransition = set.incomingTransition();
                if (setDepth == sourceDepth && setTransition == sourceTransition)
                    return matchingPosition(sourceDepth, sourceTransition);
                if (set.contained().lesserInSet())
                    return atSourceSetAhead(sourceDepth, sourceTransition);
                // else set is again ahead, repeat the process
            }
        }

        private D getClosestDeletion(Cursor<T> source)
        {
            D content = (D) source.content();
            if (content != null)
                return content;

            Cursor<T> cursorToMarker = source.duplicate();
            return (D) cursorToMarker.advanceToContent(null);
        }

        private int atSourceSetAhead(int sourceDepth, int sourceTransition)
        {
            currentTransition = sourceTransition;
            currentDepth = sourceDepth;
            currentContent = applyCoveredContent();
            state = CursorState.SET_AHEAD;
            return sourceDepth;
        }

        private int atSetSourceAhead(int setDepth, int setTransition)
        {
            currentTransition = setTransition;
            currentDepth = setDepth;
            currentContent = applyCoveringDeletion();
            state = CursorState.SOURCE_AHEAD;
            return setDepth;
        }

        private int matchingPosition(int sourceDepth, int sourceTransition)
        {
            currentTransition = sourceTransition;
            currentDepth = sourceDepth;
            currentContent = applyMatchingContent(set.contained());
            state = CursorState.MATCHING_POSITION;
            return sourceDepth;
        }

        private int exhausted()
        {
            assert activeDeletion == null : "Cursor exhausted with active deletion still set.";
            currentDepth = -1;
            currentTransition = 0;
            currentContent = null;
            state = CursorState.MATCHING_POSITION;
            return -1;
        }

        private D applyMatchingContent(TrieSetImpl.Contained contained)
        {
            if (contained == TrieSetImpl.Contained.OUTSIDE_PREFIX)
                return null;
            D deletion = (D) source.content();
            if (deletion == null)
                return null;

            switch (contained)
            {
                case START:
                    assert activeDeletion == null;
                    activeDeletion = deletion = handler.asBound(deletion, BoundSide.AFTER, BoundSide.AFTER);
                    return deletion;
                case END:
                    assert handler.closes(deletion, activeDeletion);
                    deletion = handler.asBound(deletion, BoundSide.BEFORE, BoundSide.BEFORE);
                    activeDeletion = null;
                    return deletion;
                case INSIDE_PREFIX:
                    assert handler.closes(deletion, activeDeletion);
                    activeDeletion = handler.asBound(deletion, BoundSide.AFTER, BoundSide.AFTER);
                    return deletion;
                default:
                    throw new AssertionError();
            }
        }

        private D applyCoveredContent()
        {
            D deletion = (D) source.content();
            if (deletion == null)
                return null;
            assert handler.closes(deletion, activeDeletion);
            activeDeletion = handler.asBound(deletion, BoundSide.AFTER, BoundSide.AFTER);   // take only the AFTER side
            return deletion;
        }

        public D applyCoveringDeletion()
        {
            switch (set.contained())
            {
                case START:
                    assert activeDeletion == null : "Deletion still open at range start";
                    activeDeletion = coveringDeletion;
                    return coveringDeletion;
                case END:
                    assert activeDeletion == coveringDeletion : "Active deletion does not match covering when reporting covered range";
                    D result = handler.asBound(activeDeletion, BoundSide.AFTER, BoundSide.BEFORE);
                    activeDeletion = null;
                    return result;
                default:
                    return null;
            }
        }

        @Override
        public DeletionCursor<T, D> duplicate()
        {
            return new DeletionCursor<>(this);
        }
    }
}
