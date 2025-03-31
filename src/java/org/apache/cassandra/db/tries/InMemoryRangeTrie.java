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

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class InMemoryRangeTrie<M extends RangeMarker<M>> extends InMemoryBaseTrie<M> implements RangeTrie<M>
{
    InMemoryRangeTrie(ByteComparable.Version byteComparableVersion, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(byteComparableVersion, bufferType, lifetime, opOrder);
    }

    public static <M extends RangeMarker<M>> InMemoryRangeTrie<M> shortLived(ByteComparable.Version byteComparableVersion)
    {
        return new InMemoryRangeTrie<>(byteComparableVersion, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    public static <M extends RangeMarker<M>> InMemoryRangeTrie<M> shortLived(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryRangeTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.SHORT, null);
    }

    public static <M extends RangeMarker<M>> InMemoryRangeTrie<M> longLived(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLived(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    public static <M extends RangeMarker<M>> InMemoryRangeTrie<M> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryRangeTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.LONG, opOrder);
    }

    public InMemoryRangeCursor makeCursor(Direction direction)
    {
        return new InMemoryRangeCursor(direction, root, 0, -1);
    }


    class InMemoryRangeCursor extends InMemoryCursor implements RangeCursor<M>
    {
        boolean activeIsSet;
        M activeRange;  // only non-null if activeIsSet
        M prevContent;  // can only be non-null if activeIsSet

        InMemoryRangeCursor(Direction direction, int root, int depth, int incomingTransition)
        {
            super(direction, root, depth, incomingTransition);
            activeIsSet = true;
            activeRange = null;
            prevContent = null;
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
        public M state()
        {
            if (!activeIsSet)
                setActiveState();
            return activeRange;
        }

        @Override
        public M precedingState()
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
                activeRange = content.precedingState(direction);
                prevContent = content;
                activeIsSet = true;
            }
            else if (prevContent != null)
            {
                // If the previous state was exact, its right side is what we now have.
                activeRange = prevContent.precedingState(direction.opposite());
                prevContent = null;
                assert activeIsSet;
            }
            // otherwise the active state is either not set or still valid.
            return depth;
        }

        private void setActiveState()
        {
            assert content() == null;
            M nearestContent = getFirstContent(currentFullNode);
            activeRange = nearestContent != null ? nearestContent.precedingState(direction) : null;
            prevContent = null;
            activeIsSet = true;
        }

        @Override
        public InMemoryRangeCursor tailCursor(Direction direction)
        {
            InMemoryRangeCursor cursor = new InMemoryRangeCursor(direction, currentFullNode, -1, -1);
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
        RangeCursor<U> mutationCursor = mutation.cursor(Direction.FORWARD);
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
                     RangeCursor<N> mutationCursor,
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
                N mutationCoveringState = content.precedingState(Direction.REVERSE);
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
        return rangeMarker.precedingState(Direction.REVERSE);
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
                existingCoveringState = existingCoveringState.precedingState(Direction.FORWARD);
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
                mutationCoveringState = mutationContent.precedingState(Direction.REVERSE);
                existingCoveringState = rightSideAsCovering(existingContent);
                if (mutationCoveringState == null)
                {
                    assert atMutation; // mutation covering state can only change when mutation content is present
                    return false; // mutation deletion range was closed, we can continue normal mutation cursor iteration
                }
            }
        }
    }
}
