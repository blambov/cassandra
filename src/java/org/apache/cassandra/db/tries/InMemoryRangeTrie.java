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

import java.util.function.Consumer;
import java.util.function.Predicate;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class InMemoryRangeTrie<S extends RangeState<S>> extends InMemoryBaseTrie<S, RangeCursor<S>, RangeTrie<S>> implements RangeTrie<S>
{
    // constants for space calculations
    private static final long EMPTY_SIZE_ON_HEAP;
    private static final long EMPTY_SIZE_OFF_HEAP;
    static
    {
        // Measuring the empty size of long-lived tries, because these are the ones for which we want to track size.
        InMemoryRangeTrie<?> empty = new InMemoryRangeTrie<>(ByteComparable.Version.OSS50, BufferType.ON_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty);
        empty = new InMemoryRangeTrie<>(ByteComparable.Version.OSS50, BufferType.OFF_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty);
    }

    InMemoryRangeTrie(ByteComparable.Version byteComparableVersion, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(byteComparableVersion, false, bufferType, lifetime, opOrder);
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

    /// Range tries can have boundaries that apply after a branch.
    @Override
    protected boolean allowsContentAfterBranch()
    {
        return true;
    }

    protected long emptySizeOnHeap()
    {
        return bufferManager.bufferType() == BufferType.ON_HEAP ? EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP;
    }

    static class InMemoryRangeCursor<S extends RangeState<S>> extends InMemoryCursor<S> implements RangeCursor<S>
    {
        boolean activeIsSet;
        S activeRange;  // only non-null if activeIsSet
        S prevContent;  // can only be non-null if activeIsSet

        InMemoryRangeCursor(InMemoryReadTrie<S> trie, Direction direction, int root)
        {
            // Range tries must always preserve the order of boundaries with respect to descendants.
            super(trie, direction, root, false);
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

        /// @inheritDoc
        /// Range tries may have two content values. Handle this possibility here.
        @Override
        S processPrefix(int node, int depth, int transition)
        {
            S content1 = processPrefixEntry(node, depth, transition, PREFIX_CONTENT_OFFSET);
            S content2 = processPrefixEntry(node, depth, transition, PREFIX_ALTERNATE_OFFSET);
            assert (content1 == null) || (content2 == null) : "Prefix node with incompatible content pair " + content1 + " and " + content2;
            // It's not okay to have two backtracks either, but this is not trivial to check.
            return content1 == null ? content2 : content1;
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

        long updateActiveAndReturn(long position)
        {
            if (!Cursor.isExhausted(position))
            {
                // Always check if we are seeing new content; if we do, that's an easy state update.
                S content = this.content;
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
            S nearestContent = getNearestContent(direction);
            // Note: the nearest content may change between the time we fetch it and when we reach that node, e.g.
            // if someone deletes aa-cd where there existed an abc-acd deletion, and we fetched the latter while at "a".
            // This, though, should only be possible if the preceding state of the nearest content is null.
            activeRange = nearestContent != null ? nearestContent.precedingState(direction) : null;
            prevContent = null;
            activeIsSet = true;
        }

        private S getNearestContent(Direction direction)
        {
            // Descend into the children of the node until content is found, making sure to ignore return-path content
            // for non-leaf nodes (because there must be a leaf that is before them in iteration order).

            // This should yield the same result as walking a copy of this cursor in the given direction until the first
            // content, i.e.
            //   new InMemoryRangeCursor<>(trie, direction, currentFullNode).advanceToContent(null);
            int node = currentFullNode;
            if (isNull(node))
                return null;
            while (true)
            {
                assert !isNull(node);
                if (isLeaf(node))
                {
                    // We have reached the bottom of the branch. Report regardless of direction as it's the closest
                    // content either way.
                    return trie.getContent(node);
                }

                if (offset(node) == PREFIX_OFFSET)
                {
                    int contentId = trie.getIntVolatile(node + direction.select(PREFIX_CONTENT_OFFSET, PREFIX_ALTERNATE_OFFSET));
                    if (isLeaf(contentId))
                        return trie.getContent(contentId);

                    node = trie.followPrefixTransition(node);
                    assert !isNull(node);
                }

                node = trie.getFirstChild(node, direction);
            }
        }

        @Override
        public InMemoryRangeCursor<S> tailCursor(Direction direction)
        {
            // Deletion ranges active at entry and exit must be presented by the tail at its root. To do this, get
            // the closest content in both forward and reverse direction and adjust the content that the tail reports
            // for them.
            S rootDescentContent = getTailRootContent(this.direction, content, activeIsSet, activeRange);
            S rootAscentContent = getTailRootContent(this.direction.opposite(), getAscentPathContent(), false, null);
            if (this.direction != direction)
            {
                S swap = rootDescentContent;
                rootDescentContent = rootAscentContent;
                rootAscentContent = swap;
            }

            if (rootAscentContent == null && rootDescentContent == null)
                return new InMemoryRangeCursor<>(trie, direction, currentNode);
            else
                return new InMemoryRangeBranchCursor<>(trie, direction, currentNode, rootDescentContent, rootAscentContent);
        }

        S getAscentPathContent()
        {
            if (backtrackDepth <= 0)
                return null;
            if (depth(backtrackDepth - 1) != depth - 1)
                return null;
            int contentId = node(backtrackDepth - 1);
            if (!isLeaf(contentId))
                return null;
            assert shouldPresentOnTheReturnPath(contentId);
            return trie.getContent(contentId);
        }

        S getTailRootContent(Direction direction, S contentAtRoot, boolean activeRangeKnown, S activeRange)
        {
            if (contentAtRoot != null)
                return contentAtRoot.restrict(!direction.isForward(), direction.isForward());
            if (!activeRangeKnown)
                activeRange = getNearestContent(direction);
            if (activeRange == null)
                return null;
            activeRange = activeRange.precedingState(direction);
            if (activeRange == null)
                return null;
            return activeRange.asBoundary(direction);
        }
    }

    /// Modified range cursor returning the given content at the root's descend and ascent positions.
    static class InMemoryRangeBranchCursor<S extends RangeState<S>> extends InMemoryRangeCursor<S>
    {
        final S rootAscentContent;

        InMemoryRangeBranchCursor(InMemoryReadTrie<S> trie, Direction direction, int root, S rootDescentContent, S rootAscentContent)
        {
            super(trie, direction, root);
            this.rootAscentContent = rootAscentContent;
            if (rootAscentContent != null)
                addBacktrack(NONE, 0, -1);
            assert currentFullNode == root && currentNode == root;
            long rootPos = encodedPosition();
            if (rootDescentContent != null)
                setNodeState(rootPos | MAY_HAVE_CONTENT_BIT, rootDescentContent, root, root);
            // The superclass constructor has already run updateActiveAndReturn, which may have left
            // prevContent set from the root's own content. Clear it so the re-run below, which sees
            // the branch content installed above, starts from the same state a fresh cursor would.
            prevContent = null;
            updateActiveAndReturn(rootPos);
        }

        @Override
        long advanceToNextChild(int node, int data)
        {
            if (isNull(node))
                return presentAscentPathContent();
            else
                return super.advanceToNextChild(node, data);
        }

        @Override
        long advanceToNextChildWithTarget(int node, int data, int transition)
        {
            if (isNull(node))
                return direction.le(transition, data) ? presentAscentPathContent()
                                                      : NOT_FOUND;
            else
                return super.advanceToNextChildWithTarget(node, data, transition);
        }

        long presentAscentPathContent()
        {
            return setNodeState(Cursor.encode(++depth, 0, direction) | MAY_HAVE_CONTENT_BIT | ON_RETURN_PATH_BIT,
                                rootAscentContent,
                                NONE,
                                NONE);
        }

        @Override
        S getAscentPathContent()
        {
            if (backtrackDepth == 0)
                return null;
            if (depth(backtrackDepth - 1) != depth - 1)
                return null;
            int contentId = node(backtrackDepth - 1);
            if (!isNullOrLeaf(contentId))
                return null;
            if (isNull(contentId))
                return rootAscentContent;
            assert shouldPresentOnTheReturnPath(contentId);
            return trie.getContent(contentId);
        }
    }

    /// Map-like put method, using a fast recursive implementation through the key bytes. May run into stack overflow if
    /// the trie becomes too deep. When the correct position in the trie has been reached, the value will be resolved
    /// with the given function before being placed in the trie (even if there's no pre-existing content in this trie).
    ///
    /// The caller is responsible for making sure that the range state requirements are satisfied (e.g. that all opened
    /// ranges are properly closed) after performing a sequence of recursive puts.
    ///
    /// @param key The trie path/key for the given value.
    /// @param value The value being put in the memtable trie. Note that it can be of type different than the element
    /// type for this memtable trie. It's up to the `transformer` to return the final value that will stay in
    /// the memtable trie.
    /// @param contentAfterBranch Whether the content should be placed after descendants in forward iteration order.
    /// @param transformer A function applied to the potentially pre-existing value for the given key, and the new
    /// value (of a potentially different type), returning the final value that will stay in the memtable trie. Applied
    /// even if there's no pre-existing value in the memtable trie.
    public <R> void putRecursive(ByteComparable key, R value, boolean contentAfterBranch, final UpsertTransformer<S, R> transformer)
    throws TrieSpaceExhaustedException
    {
        putRecursiveInternal(key, value, contentAfterBranch, transformer);
    }

    /// Reused storage for the state of application of mutations. This stores the backtracking path, including changes
    /// already applied (e.g. new version of a node that is not yet linked to the current trie) and some that are yet
    /// to be applied (e.g. updated content).
    ///
    /// Because in-memory tries are single-writer, we can reuse a single state array for all updates. The updates are
    /// serialized and thus no other thread can corrupt this state (note that this is not the factor enforcing the
    /// single writer policy, and since we are already bound to it there is cost involved in reusing this state array).
    final private ApplyState<S> applyState = new ApplyState<>(this);

    enum AdvanceResult
    {
        DESCENDED,
        NEEDS_ASCENT,
        AT_LIMIT
    }

    static class ApplyState<S extends RangeState<S>> extends InMemoryBaseTrie.ApplyState<S>
    {
        ApplyState(InMemoryBaseTrie<S, ?, ?> trie)
        {
            super(trie);
        }

        ApplyState<S> start()
        {
            return start(trie.root);
        }

        ApplyState<S> start(int root)
        {
            return (ApplyState<S>) super.start(root, droppedContentCallback);
        }

        private S getFirstChildContent(int node)
        {
            while (true)
            {
                int contentId = getDescentPathContentId(node);
                if (contentId != NONE)
                    return trie.getContent(contentId);

                int next = trie.getNextChild(node, 0);

                if (next == NONE)
                {
                    int returnPathContent = getAscentPathContentId(node);
                    assert returnPathContent != NONE;
                    return trie.getContent(returnPathContent);
                }
                node = next;
            }
        }

        /// Get the nearest content following the current position. This is used to establish the range that applies to
        /// the current position after we have followed a mutation path which is done by skipping over our boundaries.
        private S getNearestContent(boolean onReturnPath)
        {
            // 1. If not on the return path, and the node we are positioned on exists, descend until we find content.
            // If we can't descend any further, there must be return-side content there. We are done.
            int fullNode = existingFullNode();
            if (fullNode != NONE && !onReturnPath)
                return getFirstChildContent(fullNode);

            // 2. If the node we are positioned on did not exist, or we are looking for return-path data, ascend until
            // we find a node that exists.
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
                // 3. If that node has a child with a transition index greater than the one we took to descend, descend
                // into that child and perform 1.
                int child = trie.getNextChild(node, transitionAtDepth(stackPos) + 1);
                if (child != NONE)
                    return getFirstChildContent(child);
                // 4. If not, check return path content -- return if present.
                int returnPathId = getAscentPathContentId(node);
                if (returnPathId != NONE)
                    return trie.getContent(returnPathId);
                // 5. Otherwise, go up one level and back to 3.
                if (--stackPos < 0)
                    return null;
                node = existingFullNodeAtDepth(stackPos);
            }
        }

        boolean advanceToMutationPosition(int depth, int transition, boolean isOnReturnPath, int forcedCopyDepth)
        throws TrieSpaceExhaustedException
        {
            while (currentDepth >= Math.max(depth, 1))
            {
                if (isOnReturnPath && depth == currentDepth && transition == transitionAtDepth(currentDepth - 1))
                    return true;

                // There are no more children. Ascend to the parent state to continue walk.
                attachAndMoveToParentState(forcedCopyDepth);
            }

            if (depth <= 0) // Either exhausted or the root's return path position.
                return (isOnReturnPath && depth == 0);

            // We have a transition, get child to descend into
            descend(transition);
            return true;
        }

        AdvanceResult tryDescendInExisting(int limitDepth, int limitTransition, boolean limitOnReturnPath)
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

        int getDescentPathContentId(int fullNode)
        {
            if (isNull(fullNode))
                return NONE;
            if (isLeaf(fullNode))
                return !trie.shouldPresentAfterBranch(fullNode) ? fullNode : NONE;
            if (offset(fullNode) == PREFIX_OFFSET)
                return trie().getIntVolatile(fullNode + PREFIX_CONTENT_OFFSET);

            return NONE;
        }

        int getAscentPathContentId(int fullNode)
        {
            if (isNull(fullNode))
                return NONE;
            if (isLeaf(fullNode))
                return trie.shouldPresentAfterBranch(fullNode) ? fullNode : NONE;
            if (offset(fullNode) == PREFIX_OFFSET)
                return trie().getIntVolatile(fullNode + PREFIX_ALTERNATE_OFFSET);

            return NONE;
        }

        int getAscentPathContentId()
        {
            return getAscentPathContentId(existingFullNode());
        }

        @Override
        protected int applyContent(boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            int ascentPathContentId = getAscentPathContentId();
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
            {
                // we need to update the root -- leave that job to complete() and abuse existingFullNode to tell it
                // what value to use
                setExistingFullNode(updatedFullNode);
                --currentDepth;
            }
        }

        protected int applyAscentPathContent(int ascentPathContentId, boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            if (ascentPathContentId == NONE)
                return super.applyContent(forcedCopy);

            int descentPathContentId = descentPathContentId();
            final int updatedPostContentNode = updatedPostContentNode();
            if (isNull(updatedPostContentNode))
            {
                if (!isNull(ascentPathContentId) && !trie.shouldPreserveWithoutChildren(ascentPathContentId))
                {
                    trie.releaseContent(ascentPathContentId);
                    return super.applyContent(forcedCopy);
                }
                if (!isNull(descentPathContentId) && !trie.shouldPreserveWithoutChildren(descentPathContentId))
                {
                    trie.releaseContent(descentPathContentId);
                    descentPathContentId = NONE;
                }
            }

            final int existingPreContentNode = existingFullNode();
            final int existingPostContentNode = existingPostContentNode();

            if (isNull(descentPathContentId) && isNull(updatedPostContentNode))
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

        void attachPreparedRoot(int updatedFullNode)
        {
            if (updatedFullNode != trie.root)
            {
                // Only write to root if they are different (value doesn't change, but
                // we don't want to invalidate the value in other cores' caches unnecessarily).
                trie.root = updatedFullNode;
            }
        }

    }

    /// Applies a sequence of range boundaries given in order, e.g. the content of a [BasicRangeCursor], to an in-memory
    /// range trie or to a deletion branch of an in-memory deletion-aware trie. The caller moves the applier to each
    /// position of the mutation with [#moveTo] and gives it the mutation's boundary there, if any, with [#applyAt].
    ///
    /// While a range of the mutation is open, i.e. between a boundary that starts a range and the one that ends it,
    /// [#moveTo] also visits the existing boundaries on the way to apply the range to them.
    static class RangeApplier<S extends RangeState<S>, U extends RangeState<U>> implements NodeFeatures<U>
    {
        final ApplyState<S> state;
        final UpsertTransformer<S, U> transformer;
        final Predicate<NodeFeatures<U>> needsForcedCopy;
        final Consumer<? super S> droppedContentCallback;

        /// The depth of the root of the applied trie in the positions given to [#moveTo].
        int rootDepth;
        int forcedCopyDepth;
        /// The boundary of the mutation at the current position, null if none.
        U mutationContent;
        /// The range of the mutation that covers what follows the current position, null if no range is open.
        U mutationCoveringState;
        /// The range of the existing trie that covers what follows the current position, null if none. Only meaningful
        /// while `existingCoveringStateIsValid`: it is tracked while a range of the mutation is open, and remains valid
        /// afterwards for as long as the walk cannot have passed any existing boundaries, i.e. while it moves inside
        /// branches that do not exist in the trie. When not valid, it is found by a search (see
        /// [#existingCoveringState]).
        S existingCoveringState;
        boolean existingCoveringStateIsValid;

        RangeApplier(ApplyState<S> state,
                     UpsertTransformer<S, U> transformer,
                     Predicate<NodeFeatures<U>> needsForcedCopy,
                     Consumer<? super S> droppedContentCallback)
        {
            this.state = state;
            this.transformer = transformer;
            this.needsForcedCopy = needsForcedCopy;
            this.droppedContentCallback = droppedContentCallback;
        }

        /// Prepare to apply a mutation to the trie with the given root, which the mutation presents at the given depth.
        void start(int root, int initialForcedCopyDepth, int rootDepth)
        {
            state.start(root, droppedContentCallback);
            this.rootDepth = rootDepth;
            forcedCopyDepth = initialForcedCopyDepth;
            mutationContent = null;
            mutationCoveringState = null;
            // No range can be open before the root.
            setExistingCoveringState(null);
        }

        @Override
        public U content()
        {
            return mutationContent;
        }

        /// Move to the given position of the mutation. If a range of the mutation is open, apply it to the existing
        /// boundaries on the way.
        ///
        /// @return false if the mutation is exhausted. The state is then at the root, or above it if the content on
        /// the root's return path was changed; see [#completeBranch].
        boolean moveTo(long position) throws TrieSpaceExhaustedException
        {
            int depth = Cursor.depth(position) - rootDepth;
            int transition = Cursor.incomingTransition(position);
            boolean onReturnPath = Cursor.isOnReturnPath(position);

            if (mutationCoveringState == null)
            {
                // The move skips the existing boundaries between the two positions. There are none if it descends
                // from the current node and the node has no existing children before the target, or if it is made
                // from a node that does not exist in the trie.
                boolean skipsNoChildren = existingCoveringStateIsValid
                                          && !onReturnPath
                                          && depth == state.currentDepth + 1
                                          && state.trie.getNextTransition(state.updatedPostContentNode(), state.transition() + 1) >= transition;
                if (!state.advanceToMutationPosition(depth, transition, onReturnPath, forcedCopyDepth))
                    return false;
                existingCoveringStateIsValid &= skipsNoChildren
                                                || isNull(state.existingFullNodeAtDepth(onReturnPath ? depth : depth - 1));
                return true;
            }

            assert depth >= 0 : "Unbounded range in mutation trie, state " + mutationCoveringState + " active when exhausted.";
            while (true)
            {
                AdvanceResult advance = state.tryDescendInExisting(depth, transition, onReturnPath);
                // We need to force-copy every node we touch while applying ranges to ensure consistent ranges.
                forcedCopyDepth = Math.min(forcedCopyDepth, state.currentDepth);

                switch (advance)
                {
                    case AT_LIMIT:
                        return true;
                    case DESCENDED:
                    {
                        // An existing position covered by the mutation's range. Apply the range to its boundary.
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
                        // There are no more existing children. Ascend, applying the range to a boundary on the return
                        // path of the node.
                        int existingContentId = state.getAscentPathContentId();
                        if (existingContentId != NONE)
                        {
                            S existingContent = state.trie.getContent(existingContentId);
                            existingCoveringState = existingContent.succedingState(Direction.FORWARD);
                            ascendWithUpdatedReturnPathContent(existingContentId, existingContent, mutationCoveringState);
                        }
                        else
                            state.attachAndMoveToParentState(forcedCopyDepth);
                        break;
                    }
                    default:
                        throw new AssertionError();
                }
            }
        }

        /// Apply the boundary that the mutation has at the current position (null if none), which must have been
        /// reached with [#moveTo].
        ///
        /// If the position is on the return path and its content changes, this also ascends to the parent of the node.
        void applyAt(U mutationContent, boolean onReturnPath) throws TrieSpaceExhaustedException
        {
            this.mutationContent = mutationContent;
            if (state.currentDepth < forcedCopyDepth)
                forcedCopyDepth = needsForcedCopy.test(this) ? state.currentDepth : Integer.MAX_VALUE;

            int existingContentId = onReturnPath ? state.getAscentPathContentId() : state.descentPathContentId();
            S existingContent = isNull(existingContentId) ? null : state.trie.getContent(existingContentId);
            if (mutationContent == null && mutationCoveringState == null)
            {
                // Nothing to apply. An existing boundary tells us the state that follows.
                if (existingContent != null)
                    setExistingCoveringState(existingContent.succedingState(Direction.FORWARD));
                return;
            }

            // We need to force-copy every node we touch while applying ranges to ensure consistent ranges.
            forcedCopyDepth = Math.min(forcedCopyDepth, state.currentDepth);
            if (existingContent == null && mutationContent == null)
                return;

            S existing = existingContent != null ? existingContent : existingCoveringState(onReturnPath);
            U mutation = mutationContent != null ? mutationContent : mutationCoveringState;
            if (onReturnPath)
                ascendWithUpdatedReturnPathContent(existingContentId, existing, mutation);
            else
                applyContent(existing, mutation);

            mutationCoveringState = mutation.succedingState(Direction.FORWARD);
            setExistingCoveringState(rightSideAsCovering(existing));
        }

        /// The state of the existing trie at the current position, which must not be on the return path, before
        /// anything is applied there: its boundary if it has one, otherwise the range that covers it.
        S existingStateAtCurrentPosition()
        {
            S content = state.getDescentPathContent();
            return content != null ? content : existingCoveringState(false);
        }

        /// The range of the existing trie that covers the current position, which must have no existing boundary.
        private S existingCoveringState(boolean onReturnPath)
        {
            if (!existingCoveringStateIsValid)
            {
                // The walk may have skipped boundaries. Take the state from the next one.
                S nextBoundary = state.getNearestContent(onReturnPath);
                setExistingCoveringState(nextBoundary != null ? nextBoundary.precedingState(Direction.FORWARD) : null);
            }
            return existingCoveringState;
        }

        private void setExistingCoveringState(S coveringState)
        {
            existingCoveringState = coveringState;
            existingCoveringStateIsValid = true;
        }

        /// Complete the application after the mutation is exhausted or has left the branch: ascend to the root, apply
        /// the changes to it and return the updated root.
        int completeBranch() throws TrieSpaceExhaustedException
        {
            assert mutationCoveringState == null : "Unbounded range in mutation trie, state " + mutationCoveringState + " active when exhausted.";
            while (state.currentDepth > 0)
                state.attachAndMoveToParentState(forcedCopyDepth);

            if (state.currentDepth == 0)
                return state.applyContent(0 >= forcedCopyDepth);
            else if (state.currentDepth == -1) // root already prepared because of return-path update to the root node
                return state.existingFullNodeAtDepth(0);
            else
                throw new AssertionError("Unexpected depth value " + state.currentDepth);
        }

        void applyContent(S existingState, U mutationState) throws TrieSpaceExhaustedException
        {
            S combined = transformer.apply(existingState, mutationState);
            S existing = existingState;
            if (combined != null && !combined.isBoundary())
                combined = null;
            if (existing != null && !existing.isBoundary())
                existing = null;
            if (combined != existing)
                state.setDescentPathContent(combined, // can be null
                                            state.currentDepth >= forcedCopyDepth); // this is called at the start of processing
        }

        private void ascendWithUpdatedReturnPathContent(int existingContentId, S existingState, U content) throws TrieSpaceExhaustedException
        {
            S combined = transformer.apply(existingState, content);
            S existing = existingState;
            if (combined != null && !combined.isBoundary())
                combined = null;
            if (existing != null && !existing.isBoundary())
                existing = null;
            int combinedId = combined != existing
                             ? state.combineContent(existingContentId,
                                                    combined,
                                                    true,
                                                    state.currentDepth >= forcedCopyDepth)
                             : existingContentId;
            state.attachAndMoveToParentStateWithAscentPathContent(combinedId, forcedCopyDepth);
        }

        static <S extends RangeState<S>> S rightSideAsCovering(S rangeState)
        {
            if (rangeState == null)
                return null;
            return rangeState.succedingState(Direction.FORWARD);
        }
    }

    /// Range trie mutator, binding the trie with merge configuration (i.e. transformer and predicates).
    /// Can be used to apply multiple modifications to the trie using [#apply(BasicRangeTrie)].
    public class Mutator<U extends RangeState<U>> extends InMemoryBaseTrie.Mutator<S, U, BasicRangeCursor<U>, ApplyState<S>>
    {
        final RangeApplier<S, U> applier;

        /// See [InMemoryTrie#mutator(UpsertTransformer, Predicate)] for the meaning of the
        /// parameters.
        Mutator(UpsertTransformer<S, U> transformer, Predicate<NodeFeatures<U>> needsForcedCopy, Consumer<? super S> droppedContentCallback)
        {
            super(transformer, needsForcedCopy, droppedContentCallback, applyState);
            this.applier = new RangeApplier<>(applyState, transformer, needsForcedCopy, droppedContentCallback);
        }

        @Override
        Mutator<U> start(int root, BasicRangeCursor<U> mutationCursor, int initialForcedCopyDepth)
        {
            super.start(root, mutationCursor, initialForcedCopyDepth);
            applier.start(root, initialForcedCopyDepth, 0);
            return this;
        }

        @Override
        Mutator<U> apply() throws TrieSpaceExhaustedException
        {
            long position = mutationCursor.encodedPosition();
            do
            {
                applier.applyAt(BasicCursor.content(mutationCursor, position), Cursor.isOnReturnPath(position));
                position = mutationCursor.advance();
            }
            while (applier.moveTo(position));
            return this;
        }

        @Override
        void complete() throws TrieSpaceExhaustedException
        {
            state.attachPreparedRoot(applier.completeBranch());
        }

        /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
        /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
        /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
        /// different than the element type for this memtable trie.
        public void apply(BasicRangeTrie<U> mutation) throws TrieSpaceExhaustedException
        {
            try
            {
                start(mutation.basicCursor(Direction.FORWARD)).apply().complete();
                completeMutation();
            }
            catch (Throwable t)
            {
                abortMutation();
                throw t;
            }
            finally
            {
                releaseMutationCursor();
            }
        }
    }


    /// Creates a trie mutator that can be used to apply multiple modifications to the trie.
    ///
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    public <U extends RangeState<U>> Mutator<U> mutator(final UpsertTransformer<S, U> transformer,
                                                        Predicate<NodeFeatures<U>> needsForcedCopy)
    {
        return new Mutator<>(transformer, needsForcedCopy, null);
    }

    /// Creates a trie mutator that can be used to apply multiple modifications to the trie.
    ///
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. See NodeFeatures for details.
    /// @param droppedContentCallback function to call when childless content is dropped because
    /// [ContentManager#shouldPreserveWithoutChildren] returned false.
    public <U extends RangeState<U>> Mutator<U> mutator(final UpsertTransformer<S, U> transformer,
                                                        Predicate<NodeFeatures<U>> needsForcedCopy,
                                                        Consumer<? super S> droppedContentCallback)
    {
        return new Mutator<>(transformer, needsForcedCopy, droppedContentCallback);
    }


    /// Modify this trie to apply the mutation given in the form of a range trie. Any content in the mutation will be
    /// resolved with the given function before being placed in this trie (even if there's no pre-existing content in
    /// this trie). For any range that the new mutation introduces, the transformer function will be applied to all
    /// existing content that falls in the range; this may result in the deletion of existing boundaries or their
    /// modification.
    /// @param mutation the mutation to be applied, given in the form of a range trie. Note that its content can be of
    /// type different than the element type for this memtable trie.
    /// @param transformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value, as well as to pre-existing content that falls under a range in the mutation.
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
    /// concurrent readers. Note that this only applies to separate ranges in the mutation. Whenever a mutation range is
    /// applied, the covered content is copied to ensure that consumer cannot see unclosed ranges due to intermediate
    /// state. See [NodeFeatures] for more details.
    public <U extends RangeState<U>> void apply(BasicRangeTrie<U> mutation,
                                                final UpsertTransformer<S, U> transformer,
                                                Predicate<NodeFeatures<U>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        mutator(transformer, needsForcedCopy).apply(mutation);
    }
}
