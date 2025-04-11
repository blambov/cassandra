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

import java.util.function.Predicate;

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
        int prevDepth;
        int coveredBranchDepth;

        InMemoryRangeCursor(Direction direction, int root, int depth, int incomingTransition)
        {
            super(direction, root, depth, incomingTransition);
            activeIsSet = true;
            activeRange = null;
            prevContent = null;
            prevDepth = Integer.MIN_VALUE;
            coveredBranchDepth = Integer.MIN_VALUE;
        }

        @Override
        public int advance()
        {
            prevDepth = depth;
            return updateActiveAndReturn(super.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            prevDepth = depth;
            return updateActiveAndReturn(super.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            activeIsSet = false;    // since we are skipping, we have no idea where we will end up
            activeRange = null;
            prevContent = null;
            prevDepth = Integer.MIN_VALUE;
            // TODO: We need to not redo finding child if we are doing multiple skipTos in a row.
            return updateActiveAndReturn(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public M state()
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
                activeRange = content;
                prevContent = content;
                activeIsSet = true;
            }
            else
            {
                if (depth < coveredBranchDepth)
                {
                    // We are ascending through a covered branch. We need to switch to that node's following state.
                    // We don't currently store that content anywhere so we need to rebuild the active state when we
                    // are asked.
                    // TODO: Check if it's a better idea to keep a stack of the active state to switch to.
                    activeIsSet = false;
                    coveredBranchDepth = Integer.MIN_VALUE;
                }
                else if (prevContent != null)
                {
                    if (depth == prevDepth + 1)
                    {
                        activeRange = prevContent.branchState();
                        prevContent = null;
                        coveredBranchDepth = prevDepth;
                    }
                    else
                    {
                        // If the previous state was exact, its right side is what we now have.
                        activeRange = prevContent.precedingState(direction.opposite());
                        prevContent = null;
                        assert activeIsSet;
                    }
                }
            }

            // otherwise the active state is either not set or still valid.
            return depth;
        }

        private void setActiveState()
        {
            assert content() == null;
            M nearestContent = getNearestContent();
            // Note: the nearest content may change between the time we fetch it and when we reach that node, e.g.
            // if someone deletes ab-cd where there existed an abc-acd deletion, and we fetched the latter while at "a".
            // This, though, should only be possible of the preceding state of the nearest content is null
            // (or the same parent's if we permit nested deletions).
            activeRange = nearestContent != null ? nearestContent.precedingState(direction) : null;
            prevContent = null;
            activeIsSet = true;
        }

        private M getNearestContent()
        {
            // Walk a copy of this cursor (non-range because we are only not doing anything smart with it) to find the
            // nearest child content in the direction of the cursor.
            return new InMemoryCursor(direction, currentNode, 0, -1).advanceToContent(null);
        }

        @Override
        public InMemoryRangeCursor tailCursor(Direction direction)
        {
            InMemoryRangeCursor cursor = new InMemoryRangeCursor(direction, currentFullNode, 0, -1);
            if (activeIsSet)
            {
                // Copy the state we have already compiled to the child cursor.
                cursor.activeIsSet = true;
                cursor.activeRange = activeRange;
            }
            else
                cursor.activeIsSet = false;

            return cursor;
        }
    }

    // Range tries have the possibility of "spooky action at a distance", i.e. be suddenly covered by a new range
    // deletion, effectively changing the current active range.
    // To avoid this causing problems, forced copying must always be done on the entirety of any deleted range.
    // FIXME: how do we enforce this? E.g. if have data for the branch at bc and a cursor is in that branch, a
    // deletion of aaaa-c should force copy the root to avoid affecting bc.
    // In this case "a" has a state with precedingAffected(REVERSE), thus we must force copy its parent.
    static class Mutation<M extends RangeMarker<M>, U extends RangeMarker<U>> extends InMemoryBaseTrie.Mutation<M, U, RangeCursor<U>>
    {
        Mutation(UpsertTransformerWithKeyProducer<M, U> transformer, Predicate<NodeFeatures<U>> needsForcedCopy, RangeCursor<U> source, InMemoryRangeTrie<M>.ApplyState state)
        {
            super(transformer, needsForcedCopy, source, state);
        }

        @Override
        void apply() throws TrieSpaceExhaustedException
        {
            applyRanges();
            assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        }

        void applyContent(M existingState, U mutationState) throws TrieSpaceExhaustedException
        {
            M combined = transformer.apply(existingState, mutationState, state);
            if (combined != null)
                combined = combined.toContent();
            state.setContent(combined, // can be null
                             state.currentDepth >= forcedCopyDepth); // this is called at the start of processing
        }


        void applyRanges()
        throws TrieSpaceExhaustedException
        {
            // While activeDeletion is not set, follow the mutation trie.
            // When a deletion is found, get existing covering state, combine and apply/store.
            // Get rightSideAsCovering and walk the full existing trie to apply, advancing mutation cursor in parallel
            // until we see another entry in mutation trie.
            // Repeat until mutation trie is exhausted.
            int depth = state.currentDepth;
            int prevAscendDepth = state.setAscendLimit(depth);
            while (true)
            {
                if (depth <= forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                U content = mutationCursor.content();

                // Keep the mutation cursor advanced to be able to know if it descends.
                mutationCursor.advance();

                if (content != null)
                {
                    final M existingCoveringState = getExistingCoveringState(); // TODO: Maybe track instead of looking up here? (Note: needs a stack)
                    applyContent(existingCoveringState, content);

                    // If branch and following state differ, we need to process them separately.
                    // - We need to pre-advance the mutation cursor to check if it descends.
                    // - It may also be the case that we have different branch and following.

                    // We now need to check:
                    // - If this introduces a new branch deletion.
                    // - If the mutation cursor descends into the branch.

                    // Several cases:
                    // - New deletion is point deletion.
                    // - New deletion starts range and there is no existing or it beats the existing: Walk both tries in
                    //   parallel to apply deletion and adjust on any change.
                    // - New deletion starts range and existing beats it: We still have to walk both tries in parallel,
                    //   because existing deletion may end before the newly introduced one, and we want to apply that when
                    //   it does.

                    if (content.hasSeparateBranchState(Direction.REVERSE))
                        applyDeletionRange(branchAsCovering(existingCoveringState), content.branchState(), depth);

                    U mutationFollowingState = content.precedingState(Direction.REVERSE);
                    if (mutationFollowingState != null)
                        applyDeletionRange(rightSideAsCovering(existingCoveringState), mutationFollowingState, state.ascendLimit);
                }

                depth = mutationCursor.depth();

                // Descend but do not modify anything yet.
                if (state.advanceTo(depth, mutationCursor.incomingTransition(), forcedCopyDepth))
                    break;
                assert depth == state.currentDepth : "Unexpected change to applyState. Concurrent trie modification?";
            }
            state.setAscendLimit(prevAscendDepth);
        }

        void applyDeletionRange(M existingCoveringState,
                                U mutationCoveringState,
                                int ascendLimit)
        throws TrieSpaceExhaustedException
        {
            int depth = mutationCursor.depth();
            int transition = mutationCursor.incomingTransition();
            // We are walking both tries in parallel.
            while (true)
            {
                AdvanceResult advanceResult = state.advanceToNextExistingOr(depth, transition, ascendLimit, forcedCopyDepth);
                if (advanceResult == AdvanceResult.ASCEND_LIMIT)
                    return;
                boolean atMutation = advanceResult == AdvanceResult.POSITION_LIMIT;

                M existingContent = state.getContent();
                U mutationContent = atMutation ? mutationCursor.content() : null;
                if (existingContent != null || mutationContent != null)
                {
                    if (existingContent == null)
                        existingContent = existingCoveringState;
                    if (mutationContent == null)
                        mutationContent = mutationCoveringState;
                    applyContent(existingContent, mutationContent);
                    mutationCoveringState = mutationContent.precedingState(Direction.REVERSE);
                    existingCoveringState = rightSideAsCovering(existingContent);
                }

                if (atMutation)
                {
                    depth = mutationCursor.advance();
                    transition = mutationCursor.incomingTransition();

                    if (depth <= forcedCopyDepth)
                        forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                    if (mutationCoveringState == null)
                        return; // mutation deletion range was closed, we can continue normal mutation cursor iteration
                }
                else
                    assert mutationCoveringState != null; // mutation covering state can only change when mutation content is present
            }
        }

        static <M extends RangeMarker<M>> M branchAsCovering(M rangeMarker)
        {
            if (rangeMarker == null)
                return null;
            return rangeMarker.branchState();
        }

        static <M extends RangeMarker<M>> M rightSideAsCovering(M rangeMarker)
        {
            if (rangeMarker == null)
                return null;
            return rangeMarker.precedingState(Direction.REVERSE);
        }

        M getExistingCoveringState()
        {
            // If the current node has content, use it.
            M existingCoveringState = state.getContent();
            if (existingCoveringState != null)
                return existingCoveringState;

            // Otherwise, we must have a descendant that will have the active state as its preceding.
            existingCoveringState = state.getNearestChildContent();
            if (existingCoveringState != null)
                return existingCoveringState.precedingState(Direction.FORWARD);

            // Otherwise, check if we are in a covered branch.
            existingCoveringState = state.getNearestParentContent();
            if (existingCoveringState != null)
                return existingCoveringState.branchState();

            return null;
        }

    }


    /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
    /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
    /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
    /// different than the element type for this memtable trie.
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    public <U extends RangeMarker<U>> void apply(RangeTrie<U> mutation,
                                                 final UpsertTransformerWithKeyProducer<M, U> transformer,
                                                 Predicate<NodeFeatures<U>> needsForcedCopy) throws TrieSpaceExhaustedException
    {
        try
        {
            Mutation<M, U> m = new Mutation<>(transformer,
                                              needsForcedCopy,
                                              mutation.cursor(Direction.FORWARD),
                                              applyState.start());
            m.apply();
            m.complete();
            completeMutation();
        }
        catch (Throwable t)
        {
            abortMutation();
            throw t;
        }
    }

    /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
    /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
    /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
    /// different than the element type for this memtable trie.
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    public <U extends RangeMarker<U>> void apply(RangeTrie<U> mutation,
                                                 final UpsertTransformer<M, U> transformer,
                                                 Predicate<NodeFeatures<U>> needsForcedCopy) throws TrieSpaceExhaustedException
    {
        apply(mutation, (UpsertTransformerWithKeyProducer<M, U>) transformer, needsForcedCopy);
    }
}
