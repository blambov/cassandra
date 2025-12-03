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
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class InMemoryRangeTrie<S extends RangeState<S>> extends InMemoryBaseTrie<S> implements RangeTrie<S>
{
    // constants for space calculations
    private static final long EMPTY_SIZE_ON_HEAP;
    private static final long EMPTY_SIZE_OFF_HEAP;
    static
    {
        // Measuring the empty size of long-lived tries, because these are the ones for which we want to track size.
        InMemoryBaseTrie<?> empty = new InMemoryRangeTrie<>(ByteComparable.Version.OSS50, BufferType.ON_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty);
        empty = new InMemoryRangeTrie<>(ByteComparable.Version.OSS50, BufferType.OFF_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty);
    }

    InMemoryRangeTrie(ByteComparable.Version byteComparableVersion, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(byteComparableVersion, bufferType, lifetime, opOrder, true);
    }

    public static <S extends RangeState<S>> InMemoryRangeTrie<S> shortLived(ByteComparable.Version byteComparableVersion)
    {
        return new InMemoryRangeTrie<>(byteComparableVersion, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    public static <S extends RangeState<S>> InMemoryRangeTrie<S> shortLived(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryRangeTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.SHORT, null);
    }

    public static <S extends RangeState<S>> InMemoryRangeTrie<S> longLived(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLived(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    public static <S extends RangeState<S>> InMemoryRangeTrie<S> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryRangeTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.LONG, opOrder);
    }

    public InMemoryRangeCursor<S> makeCursor(Direction direction)
    {
        return new InMemoryRangeCursor<>(this, direction, root);
    }

    protected long emptySizeOnHeap()
    {
        return bufferType == BufferType.ON_HEAP ? EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP;
    }

    static class InMemoryRangeCursor<S extends RangeState<S>> extends InMemoryCursor<S> implements RangeCursor<S>
    {
        boolean activeIsSet;
        S activeRange;  // only non-null if activeIsSet
        S prevContent;  // can only be non-null if activeIsSet

        InMemoryRangeCursor(InMemoryReadTrie<S> trie, Direction direction, int root)
        {
            super(trie, direction, root, true);
            activeIsSet = true;
            activeRange = null;
            prevContent = null;
            updateActiveAndReturn(encodedPosition());
        }

        @Override
        public long advance()
        {
            return updateActiveAndReturn(super.advance());
        }

        @Override
        public long advanceMultiple(TransitionsReceiver receiver)
        {
            return updateActiveAndReturn(super.advanceMultiple(receiver));
        }

        /// Range tries may have two content values. Handle this possibility here.
        @Override
        void setCurrentNodeAndApplyPrefixes(int node, int depth, int transition)
        {
            super.setCurrentNodeAndApplyPrefixes(node, depth, transition);

            if (isNullOrLeaf(node) || offset(node) != PREFIX_OFFSET)
                return;

            int extraContent = trie.getIntVolatile(node + PREFIX_ALTERNATE_OFFSET);
            assert isNullOrLeaf(extraContent);
            if (!isNull(extraContent))
            {
                if (shouldPresentOnTheReturnPath(extraContent))
                {
                    // this content needs to be presented on the return path
                    assert content != null || isNull(trie.getIntVolatile(node + PREFIX_CONTENT_OFFSET))
                        : "Prefix node with incompatible content pair"; // TODO: remove
                    addBacktrack(extraContent, transition, depth - 1);
                }
                else
                {
                    assert content == null : "Prefix node with incompatible content pair";
                    content = trie.getContent(extraContent);
                }
            }
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            activeIsSet = false;    // since we are skipping, we have no idea where we will end up
            activeRange = null;
            prevContent = null;
            return updateActiveAndReturn(super.skipTo(encodedSkipPosition));
        }

        @Override
        public S state()
        {
            if (!activeIsSet)
                setActiveState();
            return activeRange;
        }

        private long updateActiveAndReturn(long position)
        {
            if (!Cursor.isExhausted(position))
            {
                // Always check if we are seeing new content; if we do, that's an easy state update.
                S content = content();
                if (content != null)
                {
                    activeRange = content;
                    prevContent = content;
                    activeIsSet = true;
                }
                else if (prevContent != null)
                {
                    // If the previous state was exact, its right side is what we now have.
                    activeRange = prevContent.succedingState(direction);
                    prevContent = null;
                    assert activeIsSet;
                }
                // otherwise the active state is either not set or still valid.
            }
            else
            {
                // exhausted
                activeIsSet = true;
                activeRange = null;
                prevContent = null;
            }
            return position;
        }

        private void setActiveState()
        {
            assert content() == null;
            S nearestContent = getNearestContent();
            // Note: the nearest content may change between the time we fetch it and when we reach that node, e.g.
            // if someone deletes aa-cd where there existed an abc-acd deletion, and we fetched the latter while at "a".
            // This, though, should only be possible if the preceding state of the nearest content is null.
            activeRange = nearestContent != null ? nearestContent.precedingState(direction) : null;
            prevContent = null;
            activeIsSet = true;
        }

        private S getNearestContent()
        {
            // Walk a copy of this cursor to find the nearest child content in the direction of the cursor.
            // (Note: we can't use a non-range cursor because that does not use secondary content in prefixes.)
            return new InMemoryRangeCursor<>(trie, direction, currentNode).advanceToContent(null);
        }

        @Override
        public InMemoryRangeCursor<S> tailCursor(Direction direction)
        {
            InMemoryRangeCursor<S> cursor = new InMemoryRangeCursor<>(trie, direction, currentFullNode);
            cursor.activeIsSet = activeIsSet;
            if (activeIsSet)
            {
                // Copy the state we have already compiled to the child cursor.
                cursor.activeRange = activeRange;
            }

            return cursor;
        }
    }

    final private ApplyState<S> applyState = new ApplyState<>(this);

    enum AdvanceResult
    {
        DESCENDED,
        NEEDS_ASCENT,
        AT_LIMIT
    }

    static class ApplyState<S extends RangeState<S>> extends InMemoryBaseTrie.ApplyState<S>
    {
        ApplyState(InMemoryBaseTrie<S> trie)
        {
            super(trie);
        }

        ApplyState<S> start()
        {
            return start(trie.root);
        }

        ApplyState<S> start(int root)
        {
            return (ApplyState<S>) super.start(root);
        }


        S getFirstChildContent(int node)
        {
            while (true)
            {
                int contentId = getDescentPathContentId(node);
                if (contentId != NONE)
                    return trie.getContent(contentId);

                int next = trie.getNextChild(node, 0);

                if (next == NONE)
                {
                    int returnPathContent = getReturnPathContentId(node);
                    assert returnPathContent != NONE;
                    return trie.getContent(returnPathContent);
                }
                node = next;
            }
        }

        S getNearestContent(boolean onReturnPath)
        {
            // 1. If not on the return path, and the node we are positioned on exists, descend until we find content. If we
            // can't descend any further, there must be return-side content there. We are done.
            int fullNode = existingFullNode();
            if (fullNode != NONE && !onReturnPath)
                return getFirstChildContent(fullNode);

            // 2. If the node we are positioned on did not exist, or we are looking for return-path data, ascend until we
            // find a node that exists.
            int stackPos = currentDepth - 1;
            int node = NONE;

            while (stackPos >= 0)
            {
                node = existingFullNodeAtDepth(stackPos);
                if (node != NONE)
                    break;
                --stackPos;
            }

            if (node == NONE)
                return null;

            while (true)
            {
                // 3. If that node has a child with a transition index greater than the one we took to descend, descend into
                // that child and perform 1.
                int child = trie.getNextChild(node, transitionAtDepth(stackPos) + 1);
                if (child != NONE)
                    return getFirstChildContent(child);
                // 4. If not, check return path content -- return if present.
                int returnPathId = getReturnPathContentId(node);
                if (returnPathId != NONE)
                    return trie.getContent(returnPathId);
                // 5. Otherwise, go up one level and back to 3.
                if (--stackPos < 0)
                    return null;
                node = existingFullNodeAtDepth(stackPos);
            }
        }

        boolean advanceTo(int depth, int transition, boolean isOnReturnPath, int forcedCopyDepth) throws TrieSpaceExhaustedException
        {
            while (currentDepth >= Math.max(depth, 1))
            {
                if (isOnReturnPath && depth == currentDepth && transition == transitionAtDepth(currentDepth - 1))
                    return true;

                // There are no more children. Ascend to the parent state to continue walk.
                attachAndMoveToParentState(forcedCopyDepth);
            }

            if (depth <= 0)
            {
                if (isOnReturnPath && depth == 0)
                    return true;  // TODO: test
                return false;
            }

            // We have a transition, get child to descend into
            descend(transition);
            return true;
        }

        AdvanceResult tryDescend(int limitDepth, int limitTransition, boolean limitOnReturnPath)
        {
            int currentTransition = transition();

            int nextTransition = trie.getNextTransition(updatedPostContentNode(), currentTransition + 1);
            if (currentDepth + 1 == limitDepth && (nextTransition > limitTransition || (nextTransition == limitTransition && !limitOnReturnPath)))
            {
                descend(limitTransition);
                return AdvanceResult.AT_LIMIT;
            }
            if (nextTransition <= 0xFF)
            {
                descend(nextTransition);
                return AdvanceResult.DESCENDED;
            }

            // With range tries we need to be able to ascend on the return path without going over the node.
            if (limitOnReturnPath && currentDepth == limitDepth && (limitDepth == 0 || transitionAtDepth(currentDepth - 1) == limitTransition))
                return AdvanceResult.AT_LIMIT;

            return AdvanceResult.NEEDS_ASCENT;
        }

        int getReturnPathContentId(int fullNode)
        {
            if (isLeaf(fullNode) && (fullNode & CONTENT_AFTER_BRANCH) != 0)
                return fullNode;
            else if (offset(fullNode) == PREFIX_OFFSET)
                return trie().getIntVolatile(fullNode + PREFIX_ALTERNATE_OFFSET);
            else
                return NONE;
        }

        int getDescentPathContentId(int fullNode)
        {
            if (isLeaf(fullNode) && (fullNode & CONTENT_AFTER_BRANCH) == 0)
                return fullNode;
            else if (offset(fullNode) == PREFIX_OFFSET)
                return trie().getIntVolatile(fullNode + PREFIX_CONTENT_OFFSET);
            else
                return NONE;
        }

        int getReturnPathContentId()
        {
            return getReturnPathContentId(existingFullNode());
        }

        @Override
        protected int applyContent(boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            int ascentPathContentId = getReturnPathContentId();
            return applyAscentPathContent(ascentPathContentId, forcedCopy);
        }

        /// After a node's children are processed, this is called to ascend from it. This means applying the collected
        /// content to the compiled `updatedPostContentNode` and creating a mapping in the parent to it (or updating if
        /// one already exists).
        void attachAndMoveToParentStateWithAscentPathContent(int ascentPathContentId, int forcedCopyDepth) throws TrieSpaceExhaustedException
        {
            attachBranchAndMoveToParentState(applyAscentPathContent(ascentPathContentId, currentDepth >= forcedCopyDepth),
                                             forcedCopyDepth);
        }

        @Override
        void attachBranchAndMoveToParentState(int updatedFullNode, int forcedCopyDepth) throws TrieSpaceExhaustedException
        {
            if (currentDepth > 0)
                super.attachBranchAndMoveToParentState(updatedFullNode, forcedCopyDepth);
            else
                attachRoot(updatedFullNode, forcedCopyDepth);
        }

        protected int applyAscentPathContent(int ascentPathContentId, boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            if (ascentPathContentId == NONE)
                return super.applyContent(forcedCopy);

            int descentPathContentId = descentPathContentId();
            final int updatedPostContentNode = updatedPostContentNode();
            final int existingPreContentNode = existingFullNode();
            final int existingPostContentNode = existingPostContentNode();

            if (descentPathContentId == NONE && isNull(updatedPostContentNode))
            {
                // return path content only with no child -- we can use a leaf to store it
                if (existingPreContentNode != existingPostContentNode
                    && !isNullOrLeaf(existingPreContentNode)
                    && !trie.isEmbeddedPrefixNode(existingPreContentNode))
                    trie.recycleCell(existingPreContentNode);
                return ascentPathContentId;
            }

            // If we only had a descent-path entry before, upgrade to prefix node
            if (isLeaf(existingPreContentNode))
                return trie.createPrefixNode(descentPathContentId, ascentPathContentId, updatedPostContentNode, true);

            return applyPrefixChange(updatedPostContentNode,
                                     existingPreContentNode,
                                     existingPostContentNode,
                                     descentPathContentId,
                                     ascentPathContentId,
                                     forcedCopy);
        }
    }

    static class Mutation<S extends RangeState<S>, U extends RangeState<U>> extends InMemoryBaseTrie.Mutation<S, U, RangeCursor<U>, ApplyState<S>>
    {
        Mutation(UpsertTransformerWithKeyProducer<S, U> transformer, Predicate<NodeFeatures<U>> needsForcedCopy, RangeCursor<U> source, ApplyState<S> state)
        {
            this(transformer, needsForcedCopy, source, state, Integer.MAX_VALUE);
        }

        Mutation(UpsertTransformerWithKeyProducer<S, U> transformer, Predicate<NodeFeatures<U>> needsForcedCopy, RangeCursor<U> source, ApplyState<S> state, int forcedCopyDepth)
        {
            super(transformer, needsForcedCopy, source, state);
            this.forcedCopyDepth = forcedCopyDepth;
        }

        @Override
        void apply() throws TrieSpaceExhaustedException
        {
            applyRanges();
            assert state.currentDepth == 0 || state.currentDepth == -1 : "Unexpected change to applyState. Concurrent trie modification?";
        }

        @Override
        void complete() throws TrieSpaceExhaustedException
        {
            if (state.currentDepth == 0)
                super.complete();
            // else we have already attached the root because of a return-path update to the root node
        }

        void applyContent(S existingState, U mutationState) throws TrieSpaceExhaustedException
        {
            S combined = transformer.apply(existingState, mutationState, state);
            if (combined != null)
                combined = combined.isBoundary() ? combined : null;
            state.setDescentPathContent(combined, // can be null
                                        state.currentDepth >= forcedCopyDepth); // this is called at the start of processing
        }


        void applyRanges() throws TrieSpaceExhaustedException
        {
            // While activeDeletion is not set, follow the mutation trie.
            // When a deletion is found, get existing covering state, combine and apply/store.
            // Get rightSideAsCovering and walk the full existing trie to apply, advancing mutation cursor in parallel
            // until we see another entry in mutation trie.
            // Repeat until mutation trie is exhausted.
            int depth = state.currentDepth;
            long position = mutationCursor.encodedPosition();
            assert !Cursor.isOnReturnPath(position) : "Cursor cannot start with position on return path.";
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                U content = mutationCursor.content();
                if (content != null && content.succedingState(Direction.FORWARD) != null)
                {
                    S existingCoveringState = getExistingCoveringState(Cursor.isOnReturnPath(position));
                    applyDeletionRange(rightSideAsCovering(existingCoveringState), position);
                }

                position = mutationCursor.advance();
                depth = Cursor.depth(position);
                // Descend but do not modify anything yet.
                if (!state.advanceTo(depth, Cursor.incomingTransition(position), Cursor.isOnReturnPath(position), forcedCopyDepth))
                    break;
                assert depth == state.currentDepth : "Unexpected change to applyState. Concurrent trie modification?";
            }
        }

        private void ascendWithNewReturnPathContent(int existingContentId, S existingState, U content, int depth) throws TrieSpaceExhaustedException
        {
            S combined = transformer.apply(existingState, content, state);
            if (combined != null)
                combined = combined.isBoundary() ? combined : null;
            int combinedId = state.combineContent(existingContentId, combined, true, forcedCopyDepth >= depth);
            state.attachAndMoveToParentStateWithAscentPathContent(combinedId, forcedCopyDepth);
        }

        void applyDeletionRange(S existingCoveringState, long position)
        throws TrieSpaceExhaustedException
        {
            AdvanceResult advance = AdvanceResult.AT_LIMIT;
            int limitDepth = Cursor.depth(position);
            int limitTransition = Cursor.incomingTransition(position);
            boolean limitOnReturnPath = Cursor.isOnReturnPath(position);
            U mutationCoveringState = null;

            // We are walking both tries in parallel.
            while (true)
            {
                // We need to force-copy every node we touch while applying ranges to ensure consistent ranges.
                forcedCopyDepth = Math.min(forcedCopyDepth, state.currentDepth);

                switch (advance)
                {
                    case AT_LIMIT:
                    {
                        U mutationContent = mutationCursor.content();

                        int existingContentId = limitOnReturnPath ? state.getReturnPathContentId() : state.descentPathContentId();
                        S existingContent = InMemoryReadTrie.isNull(existingContentId) ? null : state.trie.getContent(existingContentId);

                        if (existingContent != null || mutationContent != null)
                        {
                            if (existingContent == null)
                                existingContent = existingCoveringState;
                            if (mutationContent == null)
                                mutationContent = mutationCoveringState;

                            if (limitOnReturnPath)
                                ascendWithNewReturnPathContent(existingContentId, existingContent, mutationContent, limitDepth);
                            else
                                applyContent(existingContent, mutationContent);

                            mutationCoveringState = mutationContent.succedingState(Direction.FORWARD);
                            existingCoveringState = rightSideAsCovering(existingContent);
                            if (mutationCoveringState == null)
                                return; // mutation deletion range was closed, we can continue normal mutation cursor iteration
                        }

                        position = mutationCursor.advance();
                        limitDepth = Cursor.depth(position);
                        limitTransition = Cursor.incomingTransition(position);
                        limitOnReturnPath = Cursor.isOnReturnPath(position);
                        assert limitDepth >= 0 : "Unbounded range in mutation trie, state " + mutationCoveringState + " active when exhausted.";
                        break;
                    }
                    case DESCENDED:
                    {
                        S existingContent = state.getDescentPathContent();
                        if (existingContent != null)
                        {
                            applyContent(existingContent, mutationCoveringState);
                            existingCoveringState = existingContent.succedingState(Direction.FORWARD);
                        }
                        break;
                    }
                    case NEEDS_ASCENT:
                    {
                        int existingContentId = state.getReturnPathContentId();
                        if (existingContentId != NONE)
                        {
                            S existingContent = state.trie.getContent(existingContentId);
                            existingCoveringState = existingContent.succedingState(Direction.FORWARD);
                            ascendWithNewReturnPathContent(existingContentId,
                                                           existingContent,
                                                           mutationCoveringState,
                                                           forcedCopyDepth);
                        }
                        else
                            state.attachAndMoveToParentState(forcedCopyDepth);
                        break;
                    }
                    default:
                        throw new AssertionError();
                }

                advance = state.tryDescend(limitDepth, limitTransition, limitOnReturnPath);
            }
        }

        static <S extends RangeState<S>> S rightSideAsCovering(S rangeState)
        {
            if (rangeState == null)
                return null;
            return rangeState.succedingState(Direction.FORWARD);
        }

        S getExistingCoveringState(boolean onReturnPath)
        {
            S existingCoveringState = state.getNearestContent(onReturnPath);
            if (existingCoveringState != null)
                return existingCoveringState.precedingState(Direction.FORWARD);

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
    public <U extends RangeState<U>> void apply(RangeTrie<U> mutation,
                                                final UpsertTransformerWithKeyProducer<S, U> transformer,
                                                Predicate<NodeFeatures<U>> needsForcedCopy) throws TrieSpaceExhaustedException
    {
        try
        {
            Mutation<S, U> m = new Mutation<>(transformer,
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
    public <U extends RangeState<U>> void apply(RangeTrie<U> mutation,
                                                final UpsertTransformer<S, U> transformer,
                                                Predicate<NodeFeatures<U>> needsForcedCopy) throws TrieSpaceExhaustedException
    {
        apply(mutation, (UpsertTransformerWithKeyProducer<S, U>) transformer, needsForcedCopy);
    }
}
