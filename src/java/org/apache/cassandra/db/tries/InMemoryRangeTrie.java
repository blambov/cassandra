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

import java.util.function.Function;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class InMemoryRangeTrie<M extends RangeTrie.RangeMarker<M>> extends InMemoryTrie<M> implements RangeTrieWithImpl<M>
{
    public InMemoryRangeTrie(MemoryAllocationStrategy strategy)
    {
        super(strategy);
    }

    public static <M extends RangeTrie.RangeMarker<M>> InMemoryRangeTrie<M> shortLived()
    {
        return new InMemoryRangeTrie<M>(shortLivedStrategy());
    }

    public static <M extends RangeTrie.RangeMarker<M>> InMemoryRangeTrie<M> longLived(OpOrder opOrder)
    {
        return new InMemoryRangeTrie<M>(longLivedStrategy(opOrder));
    }

    public static <M extends RangeTrie.RangeMarker<M>> InMemoryRangeTrie<M> longLived(BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryRangeTrie<M>(longLivedStrategy(bufferType, opOrder));
    }

    @Override
    public Cursor<M> makeCursor(Direction direction)
    {
        return new RangeCursor<>(this, direction, root, -1, -1);
    }


    /**
     *
     * @param <M>
     * @param <Q> Type of the underlying trie. Made generic to accommodate deletion-aware tries where the in-memory
     *            trie is not of the same type as the deletion branches.
     */
    static class RangeCursor<M extends RangeTrie.RangeMarker<M>, Q> extends MemtableCursor<Q> implements RangeTrieImpl.Cursor<M>
    {
        boolean activeIsSet;
        M activeRange;  // only non-null if activeIsSet
        M prevContent;  // can only be non-null if activeIsSet

        RangeCursor(InMemoryReadTrie<Q> trie, Direction direction, int root, int depth, int incomingTransition)
        {
            super(trie, direction, root, depth, incomingTransition);
            activeIsSet = true;
            activeRange = null;
            prevContent = null;
        }

        RangeCursor(RangeCursor<M, Q> copyFrom)
        {
            super(copyFrom);
            this.activeRange = copyFrom.activeRange;
            this.activeIsSet = copyFrom.activeIsSet;
            this.prevContent = copyFrom.prevContent;
        }

        @SuppressWarnings("unchecked")
        @Override
        public M content()
        {
            return (M) content;
        }

        @Override
        public int advance()
        {
            return updateActiveAndReturn(super.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            return updateActiveAndReturn(super.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            activeIsSet = false;    // since we are skipping, we have no idea where we will end up
            activeRange = null;
            prevContent = null;
            return updateActiveAndReturn(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public M coveringState()
        {
            if (!activeIsSet)
                setActiveState();
            return activeRange;
        }

        private int updateActiveAndReturn(int depth)
        {
            if (depth < 0)
            {
                activeIsSet = true;
                activeRange = null;
                prevContent = null;
                return depth;
            }

            // Always check if we are seeing new content; if we do, that's an easy state update.
            M content = content();
            if (content != null)
            {
                activeRange = content.asCoveringState(direction);
                prevContent = content;
                activeIsSet = true;
            }
            else if (prevContent != null)
            {
                // If the previous state was exact, its right side is what we now have.
                activeRange = prevContent.asCoveringState(direction.opposite());
                prevContent = null;
                assert activeIsSet;
            }
            // otherwise the active state is either not set or still valid.
            return depth;
        }

        private void setActiveState()
        {
            assert content() == null;
            M nearestContent = duplicate().advanceToContent(null);
            activeRange = nearestContent != null ? nearestContent.asCoveringState(direction) : null;
            prevContent = null;
            activeIsSet = true;
        }

        @Override
        public RangeCursor<M, Q> duplicate()
        {
            return new RangeCursor<>(this);
        }

        @Override
        public RangeCursor<M, Q> tailCursor(Direction direction)
        {
            RangeCursor<M, Q> cursor = new RangeCursor<>(trie, direction, currentNodeWithPrefixes, -1, -1);
            cursor.activeIsSet = false; // TODO: check this suffices
            return cursor;
        }
    }


    /**
     * Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
     * with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
     * @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
     * different than the element type for this memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value. Applied even if there's no pre-existing value in the memtable trie.
     */
    public <U extends RangeMarker<U>> void apply(RangeTrie<U> mutation, final UpsertTransformer<M, U> transformer) throws TrieSpaceExhaustedException
    {
        RangeTrieImpl.Cursor<U> mutationCursor = RangeTrieImpl.impl(mutation).cursor(Direction.FORWARD);
        assert mutationCursor.depth() == 0 : "Unexpected non-fresh cursor.";
        ApplyState state = applyState.start();
        assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        applyRanges(state, mutationCursor, transformer);
        assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        // TODO
        state.attachRoot(Integer.MAX_VALUE);
    }

    static <M extends RangeMarker<M>, N extends RangeMarker<N>>
    void applyRanges(InMemoryTrie<M>.ApplyState state,
                     RangeTrieImpl.Cursor<N> mutationCursor,
                     final UpsertTransformer<M, N> transformer)
    throws TrieSpaceExhaustedException
    {
        // While activeDeletion is not set, follow the mutation trie.
        // When a deletion is found, get existing covering state, combine and apply/store.
        // Get rightSideAsCovering and walk the full existing trie to apply, advancing mutation cursor in parallel
        // until we see another entry in mutation trie.
        // Repeat until mutation trie is exhausted.
        int prevAscendDepth = state.setAscendLimit(state.currentDepth);
        while (true)
        {
            N content = mutationCursor.content();
            if (content != null)
            {
                final M existingCoveringState = getExistingCoveringState(state);
                applyContent(state, transformer, existingCoveringState, content);
                N mutationCoveringState = content.asCoveringState(Direction.REVERSE);
                // Several cases:
                // - New deletion is point deletion: Apply it and move on to next mutation branch.
                // - New deletion starts range and there is no existing or it beats the existing: Walk both tries in
                //   parallel to apply deletion and adjust on any change.
                // - New deletion starts range and existing beats it: We still have to walk both tries in parallel,
                //   because existing deletion may end before the newly introduced one, and we want to apply that when
                //   it does.
                if (mutationCoveringState != null)
                {
                    boolean done = applyDeletionRange(state, mutationCursor, transformer, rightSideAsCovering(existingCoveringState), mutationCoveringState);
                    if (done)
                        break;
                }
            }

            int depth = mutationCursor.advance();
            // Descend but do not modify anything yet.
            // TODO
            if (state.advanceTo(depth, mutationCursor.incomingTransition(), Integer.MAX_VALUE))
                break;
            assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
        }
        state.setAscendLimit(prevAscendDepth);
    }

    static <M extends RangeMarker<M>> M rightSideAsCovering(M rangeMarker)
    {
        if (rangeMarker == null)
            return null;
        return rangeMarker.asCoveringState(Direction.REVERSE);
    }

    private static <M extends RangeMarker<M>, N extends RangeMarker<N>>
    void applyContent(InMemoryTrie<M>.ApplyState state, UpsertTransformer<M, N> transformer, M existingState, N mutationState) throws TrieSpaceExhaustedException
    {
        M combined = transformer.apply(existingState, mutationState);
        if (combined != null)
            combined = combined.toContent();
        // TODO
        state.setContent(combined, false); // can be null
    }

    static <M extends RangeMarker<M>>
    M getExistingCoveringState(InMemoryTrie<M>.ApplyState state)
    {
        M existingCoveringState = state.getContent();
        if (existingCoveringState == null)
        {
            existingCoveringState = state.getNearestContent();    // without advancing, just get
            if (existingCoveringState != null)
                existingCoveringState = existingCoveringState.asCoveringState(Direction.FORWARD);
        }
        return existingCoveringState;
    }

    static <M extends RangeMarker<M>, N extends RangeMarker<N>>
    boolean applyDeletionRange(InMemoryTrie<M>.ApplyState state,
                               Cursor<N> mutationCursor,
                               UpsertTransformer<M,N> transformer,
                               M existingCoveringState,
                               N mutationCoveringState)
    throws TrieSpaceExhaustedException
    {
        boolean atMutation = true;
        int depth = mutationCursor.depth();
        int transition = mutationCursor.incomingTransition();
        // We are walking both tries in parallel.
        while (true)
        {
            if (atMutation)
            {
                depth = mutationCursor.advance();
                transition = mutationCursor.incomingTransition();
            }
            // TODO
            atMutation = state.advanceToNextExistingOr(depth, transition, Integer.MAX_VALUE);
            if (atMutation && depth == -1)
                return true;

            M existingContent = state.getContent();
            N mutationContent = atMutation ? mutationCursor.content() : null;
            if (existingContent != null || mutationContent != null)
            {
                // TODO: maybe assert correct closing of ranges
                if (existingContent == null)
                    existingContent = existingCoveringState;
                if (mutationContent == null)
                    mutationContent = mutationCoveringState;
                applyContent(state, transformer, existingContent, mutationContent);
                mutationCoveringState = mutationContent.asCoveringState(Direction.REVERSE);
                existingCoveringState = rightSideAsCovering(existingContent);
                if (mutationCoveringState == null)
                {
                    assert atMutation; // mutation covering state can only change when mutation content is present
                    return false; // mutation deletion range was closed, we can continue normal mutation cursor iteration
                }
            }
        }
    }


    /**
     * Override of dump to provide more detailed printout that includes the type of each node in the trie.
     * We do this via a wrapping cursor that returns a content string for the type of node for every node we return.
     */
    public String dump(Function<M, String> contentToString)
    {
        return dump(contentToString, root);
    }
}
