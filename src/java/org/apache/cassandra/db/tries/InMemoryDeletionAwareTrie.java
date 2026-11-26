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

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

/// In-memory implementation of deletion-aware tries with concurrent access support.
///
/// This class provides a concrete implementation of [DeletionAwareTrie] that stores both live data
/// and deletion information in a unified in-memory structure. It extends [InMemoryBaseTrie] to
/// inherit the efficient cell-based memory management, concurrent access patterns, and performance
/// optimizations of the base trie implementation.
///
/// This class stores deletion branches in the "alternate branch" field of prefix nodes. All the
/// machinery to support this is already provided by [InMemoryBaseTrie]. This class implements the
/// relevant cursor and mutation methods.
///
/// See [InMemoryTrie] for information on the consistency model.
///
/// @param <T> The content type for live data stored in the trie
/// @param <D> The deletion marker type, must extend [RangeState] for range operations
public class InMemoryDeletionAwareTrie<T, D extends RangeState<D>>
extends InMemoryBaseTrie<T, DeletionAwareCursor<T, D>, DeletionAwareTrie<T, D>> implements DeletionAwareTrie<T, D>
{
    // constants for space calculations
    private static final long EMPTY_SIZE_ON_HEAP;
    private static final long EMPTY_SIZE_OFF_HEAP;
    static
    {
        // Measuring the empty size of long-lived tries, because these are the ones for which we want to track size.
        InMemoryDeletionAwareTrie<?, ?> empty = new InMemoryDeletionAwareTrie<>(ByteComparable.Version.OSS50, null, BufferType.ON_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty);
        empty = new InMemoryDeletionAwareTrie<>(ByteComparable.Version.OSS50, null, BufferType.OFF_HEAP, ExpectedLifetime.LONG, null);
        EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty);
    }

    InMemoryDeletionAwareTrie(ByteComparable.Version byteComparableVersion, Predicate<T> shouldPreserveContentWithoutChildren, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(byteComparableVersion, true, shouldPreserveContentWithoutChildren, bufferType, lifetime, opOrder);
    }

    InMemoryDeletionAwareTrie(ByteComparable.Version version, BufferManager bufferManager, ContentManager<T> contentManager)
    {
        super(version, true, bufferManager, contentManager);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, null, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    /// Create a short-lived on-heap in-memory deletion-aware trie, where content that has no children and fails the
    /// `shouldPreserveContentWithoutChildren` predicate is removed.
    /// This is used to clean up dangling metadata that has no meaning when its branch is empty.
    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion, Predicate<T> shouldPreserveContentWithoutChildren)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, shouldPreserveContentWithoutChildren, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, null, bufferType, ExpectedLifetime.SHORT, null);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLived(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, null, bufferType, ExpectedLifetime.LONG, opOrder);
    }

    /// Create a long-lived in-memory deletion-aware trie, where data is stored in trie cells using the given
    /// `contentSerializer`.
    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder, ContentSerializer<T> contentSerializer)
    {
        BufferManagerMultibuf bufferManager = new BufferManagerMultibuf(bufferType, ExpectedLifetime.LONG, opOrder);
        ContentManager<T> contentManager = new ContentManagerBytes<>(contentSerializer, bufferManager);
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, bufferManager, contentManager);
    }

    static class DeletionAwareInMemoryCursor<T, D extends RangeState<D>>
    extends InMemoryCursor<T> implements DeletionAwareCursor<T, D>
    {
        DeletionAwareInMemoryCursor(InMemoryReadTrie<T> trie, Direction direction, int root)
        {
            super(trie, direction, root);
        }

        @Override
        public T content()
        {
            return content;
        }

        @Override
        T processPrefix(int node, int depth, int transition)
        {
            T content = super.processPrefix(node, depth, transition);
            if (trie.getIntVolatile(node + PREFIX_ALTERNATE_OFFSET) != NONE)
                currentPosition |= MAY_HAVE_DELETION_BRANCH_BIT;
            return content;
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            int alternateBranch = trie.getAlternateBranch(currentFullNode);
            return ((InMemoryDeletionAwareTrie<T, D>) trie).makeRangeCursor(direction, alternateBranch);
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            return new DeletionAwareInMemoryCursor<>((InMemoryDeletionAwareTrie<T, D>) trie,
                                                     direction,
                                                     currentFullNode);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private RangeCursor<D> makeRangeCursor(Direction direction, int alternateBranch) {
        return isNull(alternateBranch)
                ? null
                : new InMemoryRangeTrie.InMemoryRangeCursor<>((InMemoryReadTrie) this, direction, alternateBranch);
    }

    //noinspection ClassEscapesDefinedScope
    @Override
    public DeletionAwareInMemoryCursor<T, D> makeCursor(Direction direction)
    {
        return new DeletionAwareInMemoryCursor<>(this, direction, root);
    }

    protected long emptySizeOnHeap()
    {
        return bufferManager.bufferType() == BufferType.ON_HEAP ? EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP;
    }

    @Override
    public RangeTrie<D> deletionBranchAtRoot()
    {
        // The root is read when a cursor is opened, as everywhere else, so that the returned trie is a live view.
        return dir -> {
            int db = getAlternateBranch(root);
            if (isNull(db))
                return RangeCursor.empty(dir, byteComparableVersion);
            else
                return new InMemoryRangeTrie.InMemoryRangeCursor<>((InMemoryReadTrie<D>) this, dir, db);
        };
    }

    @Override
    public D deletionAtRoot()
    {
        // Both calls handle NONE gracefully.
        return (D) getNodeContent(getAlternateBranch(root));
    }

    static class ApplyState<T, D extends RangeState<D>> extends InMemoryBaseTrie.ApplyState<T>
    {
        int alternateBranchToAttach;

        ApplyState(InMemoryDeletionAwareTrie<T, D> trie)
        {
            super(trie);
        }

        @Override
        ApplyState<T, D> start(int root, Consumer<? super T> droppedContentCallback)
        {
            alternateBranchToAttach = NONE;
            return (ApplyState<T, D>) super.start(root, droppedContentCallback);
        }

        public int alternateBranch()
        {
            return trie.getAlternateBranch(existingFullNode());
        }

        @Override
        protected int applyContent(boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            if (alternateBranchToAttach != NONE)
            {
                int alternateBranch = alternateBranchToAttach;
                alternateBranchToAttach = NONE;
                return applyContentWithAlternateBranch(alternateBranch, forcedCopy);
            }
            else
                return super.applyContent(forcedCopy);
        }

        /// Apply the collected content and alternate branch to a node, when it is known that the node contains an
        /// alternate branch. This will create or update a prefix node to reflect the new alternate branch pointer.
        int applyContentWithAlternateBranch(int alternateBranch, boolean forcedCopy) throws TrieSpaceExhaustedException
        {
            int contentId = descentPathContentId();
            final int updatedPostContentNode = updatedPostContentNode();
            final int existingPreContentNode = existingFullNode();
            final int existingPostContentNode = existingPostContentNode();
            if (isNull(updatedPostContentNode) && !isNull(contentId) && !trie.shouldPreserveWithoutChildren(contentId))
            {
                trie.releaseContent(contentId);
                contentId = NONE;
            }

            // applyPrefixChange does not understand leaf nodes, handle upgrade from one explicitly.
            if (isLeaf(existingPreContentNode))
                return trie.createPrefixNode(contentId, alternateBranch, updatedPostContentNode, true);

            return applyPrefixChange(updatedPostContentNode,
                                     existingPreContentNode,
                                     existingPostContentNode,
                                     contentId,
                                     alternateBranch,
                                     forcedCopy);
        }
    }

    /// Reused storage for the state of application of mutations. This stores the backtracking path, including changes
    /// already applied (e.g. new version of a node that is not yet linked to the current trie) and some that are yet
    /// to be applied (e.g. updated content).
    ///
    /// This state is used for the live part of the trie (i.e. excluding deletion branches). This includes machinery to
    /// attach deletion branches to nodes in the live trie.
    ///
    /// Because in-memory tries are single-writer, we can reuse a single state array for all updates. The updates are
    /// serialized and thus no other thread can corrupt this state (note that this is not the factor enforcing the
    /// single writer policy, and since we are already bound to it there is cost involved in reusing this state array).
    final private ApplyState<T, D> applyState = new ApplyState<>(this);

    /// Reused storage for the state of application of deletions (i.e. merging deletion branches into this trie).
    /// Mutations switch to using this state object (leaving the [#applyState] to store the state leading up to the
    /// deletion branch) whenever we switch to using [InMemoryRangeTrie] methods to apply deletions.
    ///
    /// This treats this data buffers as a range trie and uses an unchecked cast to treat the deletion branches as
    /// containing only deletion states of type `D`.
    @SuppressWarnings("unchecked")
    final InMemoryRangeTrie.ApplyState<D> deletionState = new InMemoryRangeTrie.ApplyState<>((InMemoryBaseTrie<D, ?, ?>) this);

    /// Deletion-aware trie mutator, binding the trie with a merge configuration (i.e. transformers and predicates).
    /// Can be used to apply multiple modifications to the trie using [#apply(BasicDeletionAwareTrie)].
    ///
    /// The mutation is given as a [BasicDeletionAwareCursor], i.e. with its live content and deletions in one walk,
    /// and is applied in one pass in which the live trie and the relevant deletion branch of this trie are walked
    /// together with it:
    /// - Outside deletion branches the walk follows the mutation in the live trie and merges in its content.
    /// - A deletion branch is entered at a node of this trie that has one, or at a position that the mutation marks as
    ///   a deletion branch root. In the latter case a new branch is started, after hoisting the deletion branches this
    ///   trie has below that point (which can only exist if deletions are not at fixed points). The branch is attached
    ///   to its node when the walk leaves it.
    /// - Inside a deletion branch the mutation's deletions are merged into the branch, and applied to the existing live
    ///   content that they cover: while a deletion of the mutation is open, the walk also visits everything that this
    ///   trie has between the mutation's positions. The mutation's live content is merged after applying this trie's
    ///   deletions to it.
    ///
    /// Inside a deletion branch the transformers are called in the order `deletionTransformer`, `existingDeleter`,
    /// `insertedDeleter`, `liveTransformer`, both at a position of the mutation and for what this trie has between two
    /// positions: deletions are processed first, so that they only apply to pre-existing content.
    ///
    /// The exception is live content at the root of a deletion branch, which is taken to describe the branch (e.g. the
    /// metadata of a partition) and is processed before it, so that the other transformers can rely on it: it is
    /// merged, without calling `insertedDeleter`, and then given to `existingDeleter` if the mutation has a deletion at
    /// the root.
    public class Mutator<V, E extends RangeState<E>>
    extends InMemoryBaseTrie.Mutator<T, V, BasicDeletionAwareCursor<V, E>, ApplyState<T, D>>
    {
        final UpsertTransformer<D, E> deletionTransformer;
        final UpsertTransformer<T, E> existingDeleter;
        final BiFunction<D, V, V> insertedDeleter;
        final boolean deletionsAtFixedPoints;
        final InMemoryRangeTrie.RangeApplier<D, E> deletionApplier;

        /// The depth of the root of the deletion branch that the walk is in, -1 if it is not in one.
        int deletionBranchDepth;
        /// Whether the deletion branch that the walk is in was made by hoisting branches from below its root.
        boolean hoisted;
        /// True while the deletion transformer is called, when the key methods describe the deletion's position.
        boolean atDeletion;

        /// See [InMemoryDeletionAwareTrie#mutator(UpsertTransformer, UpsertTransformer, UpsertTransformer, BiFunction, boolean, Predicate, Predicate)]
        /// for the meaning of the parameters.
        @SuppressWarnings("unchecked")
        Mutator(UpsertTransformer<T, V> liveTransformer,
                UpsertTransformer<D, E> deletionTransformer,
                UpsertTransformer<T, E> existingDeleter,
                BiFunction<D, V, V> insertedDeleter,
                Predicate<NodeFeatures<V>> needsForcedCopyInLiveTrie,
                Predicate<NodeFeatures<E>> needsForcedCopyInDeletionBranch,
                Consumer<? super T> droppedContentCallback,
                boolean deletionsAtFixedPoints)
        {
            super(liveTransformer, needsForcedCopyInLiveTrie, droppedContentCallback, applyState);
            this.deletionTransformer = deletionTransformer;
            this.deletionApplier = new InMemoryRangeTrie.RangeApplier<>(deletionState,
                                                                         this::mergeDeletion,
                                                                         needsForcedCopyInDeletionBranch,
                                                                         (Consumer<D>) droppedContentCallback);
            this.existingDeleter = existingDeleter;
            this.insertedDeleter = insertedDeleter;
            this.deletionsAtFixedPoints = deletionsAtFixedPoints;
        }

        @Override
        Mutator<V, E> apply() throws TrieSpaceExhaustedException
        {
            deletionBranchDepth = -1;
            atDeletion = false;
            long position = mutationCursor.encodedPosition();
            do
            {
                applyPosition(position);
                position = mutationCursor.advance();
            }
            while (moveTo(position));
            return this;
        }

        /// Apply what the mutation has at the given position, which the walk has just reached.
        private void applyPosition(long position) throws TrieSpaceExhaustedException
        {
            V content = BasicCursor.content(mutationCursor, position);
            E deletion = BasicDeletionAwareCursor.deletion(mutationCursor, position);
            if (Cursor.isOnReturnPath(position))
            {
                // Only deletions can be placed after a branch, and we can only get here inside a deletion branch.
                if (content != null)
                    throw new IllegalArgumentException("Live content on the return path at " + Cursor.toString(position));
                deletionApplier.applyAt(deletion, true);
                return;
            }

            int depth = state.currentDepth;
            assert depth == Cursor.depth(position) : "Unexpected change to applyState. Concurrent trie modification?";
            if (depth < forcedCopyDepth)
                forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

            if (deletionBranchDepth < 0)
            {
                // If a deletion branch is rooted here, the live content is processed before the branch.
                applyContent(content);
                if (enterDeletionBranch(position))
                {
                    if (deletion != null)
                        deleteExistingContent(deletion);
                    deletionApplier.applyAt(deletion, false);
                }
                else
                    assert deletion == null : "Deletion outside a deletion branch at " + Cursor.toString(position);
                return;
            }
            assert state.alternateBranch() == NONE : "Deletion branch found inside another deletion branch. Are deletions really at fixed points?";

            // The existing deletion that applies to the incoming content must be taken before the incoming deletion
            // changes it.
            D existingDeletion = content != null ? deletionApplier.existingStateAtCurrentPosition() : null;

            E applicableDeletion = deletion != null ? deletion : deletionApplier.mutationCoveringState;
            deletionApplier.applyAt(deletion, false);
            if (applicableDeletion != null)
                deleteExistingContent(applicableDeletion);

            if (existingDeletion != null)
                content = insertedDeleter.apply(existingDeletion, content);
            applyContent(content);
        }

        /// Enter the deletion branch rooted at the current position, if this trie has one there or the mutation marks
        /// the position as a deletion branch root.
        private boolean enterDeletionBranch(long position) throws TrieSpaceExhaustedException
        {
            int branch = state.alternateBranch();
            hoisted = false;
            if (branch == NONE)
            {
                if (!BasicDeletionAwareCursor.isDeletionBranchRoot(position))
                    return false;

                // The mutation's deletions are to be rooted here. Any deletion branches we have below this point must
                // be moved up to be merged with them.
                if (!deletionsAtFixedPoints && !isNull(state.existingFullNode()))
                {
                    branch = hoistOurDeletionBranches();
                    hoisted = branch != NONE;
                }
            }

            deletionBranchDepth = state.currentDepth;
            // If forced copying is in force, apply it to the deletion branch too.
            deletionApplier.start(branch, forcedCopyDepth <= deletionBranchDepth ? 0 : Integer.MAX_VALUE, deletionBranchDepth);
            return true;
        }

        /// Complete the deletion branch that the walk is leaving, and have it attached to its node.
        private void exitDeletionBranch() throws TrieSpaceExhaustedException
        {
            while (state.currentDepth > deletionBranchDepth)
                state.attachAndMoveToParentState(forcedCopyDepth);
            state.alternateBranchToAttach = deletionApplier.completeBranch();

            // A node that is given hoisted deletion branches has lost them from its descendants. Readers must see both
            // changes at once, i.e. the node must be copied rather than changed in place, where the links to its child
            // and its deletion branch are written separately.
            if (hoisted)
                forcedCopyDepth = Math.min(forcedCopyDepth, deletionBranchDepth);
            deletionBranchDepth = -1;
        }

        /// Move the walk to the given position of the mutation.
        ///
        /// @return false if the mutation is exhausted.
        private boolean moveTo(long position) throws TrieSpaceExhaustedException
        {
            int depth = Cursor.depth(position);
            int transition = Cursor.incomingTransition(position);
            boolean onReturnPath = Cursor.isOnReturnPath(position);

            if (deletionBranchDepth >= 0)
            {
                if (depth > deletionBranchDepth || depth == deletionBranchDepth && onReturnPath)
                {
                    deletionApplier.moveTo(position);
                    applyDeletionToExistingLiveData(depth, transition, onReturnPath);
                    return true;
                }
                exitDeletionBranch();
            }

            rejectReturnPath(position);
            return state.advanceTo(depth, transition, forcedCopyDepth);
        }

        /// Move the walk of the live trie to the given position inside the deletion branch. If a deletion of the
        /// mutation is open, apply it to the existing live content on the way.
        private void applyDeletionToExistingLiveData(int depth, int transition, boolean onReturnPath) throws TrieSpaceExhaustedException
        {
            E deletion = deletionApplier.mutationCoveringState;
            if (deletion != null)
            {
                while (state.advanceToNextExistingOr(depth, transition, onReturnPath, forcedCopyDepth, deletionBranchDepth))
                    deleteExistingContent(deletion);
            }
            else if (onReturnPath)
                state.advanceToReturnPath(depth, transition, forcedCopyDepth, deletionBranchDepth);
            else
                state.advanceTo(depth, transition, forcedCopyDepth, deletionBranchDepth);
        }

        private void deleteExistingContent(E deletion) throws TrieSpaceExhaustedException
        {
            T existingContent = state.getDescentPathContent();
            if (existingContent != null)
            {
                T remainingContent = existingDeleter.apply(existingContent, deletion);
                if (remainingContent != existingContent)
                    state.setDescentPathContent(remainingContent, // can be null
                                                state.currentDepth >= forcedCopyDepth);
            }
        }

        private D mergeDeletion(D existing, E update)
        {
            atDeletion = true;
            D merged = deletionTransformer.apply(existing, update);
            atDeletion = false;
            return merged;
        }

        private int hoistOurDeletionBranches() throws TrieSpaceExhaustedException
        {
            // Walk all of our live branch and build new branches corresponding to it. When we reach a deletion
            // branch, link it. If a branch is walked without finding a deletion branch, the returned NONEs should
            // propagate up.
            // We need to walk both the deletion-aware/live trie, as well as the deletion branch being built, so that
            // the existing deletion branch mappings can be removed.
            // The process must be atomic to make sure readers can't lose a deletion branch that has already been added,
            // thus we force all modifications to `state` to be done with force-copying, which will also result in
            // copying the leading node's child if anything was modified. The leading node itself is changed when the
            // hoisted branch is attached to it, which must also copy it (see [#exitDeletionBranch]).
            deletionState.start(NONE);
            final int initialDepth = state.currentDepth;
            final int targetForcedCopyDepth = Integer.MAX_VALUE; // never force-copy in the new branch, these are completely new nodes
            final int sourceForcedCopyDepth = initialDepth; // always force-copy in the source so that hoisting can be atomic
            while (true)
            {
                int existingAlternateBranch = state.alternateBranch();
                if (existingAlternateBranch != NONE)
                {
                    deletionState.attachBranchAndMoveToParentState(existingAlternateBranch, targetForcedCopyDepth);
                    // Drop the existing alternate branch from the main state and ascend.
                    // The normal applyContent() method uses alternate branch value of NONE.
                    state.attachAndMoveToParentState(sourceForcedCopyDepth);
                }

                if (!state.advanceToNextExisting(sourceForcedCopyDepth, initialDepth))
                    break;
                int depth = state.currentDepth;
                deletionState.advanceTo(depth - initialDepth, state.incomingTransition(), targetForcedCopyDepth);
            }
            if (deletionState.currentDepth > 0)
                deletionState.advanceTo(-1, -1, targetForcedCopyDepth);

            // Make sure the walks over the live branch that follow use the updated branch.
            state.prepareToWalkBranchAgain();
            return deletionState.applyContent(false);
        }

        /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
        /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
        /// All of the deletions in the given mutation trie will be applied, removing any content and trie paths that become
        /// empty as a result of the deletions and releasing any of the trie cells that they occupied. The deletions of
        /// the mutation will be combined with the deletion branches of this trie.
        ///
        /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
        /// different than the element type for this memtable trie.
        /// @see DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints
        public
        void apply(BasicDeletionAwareTrie<V, E> mutation)
        throws TrieSpaceExhaustedException
        {
            // TODO: track hasDeletions and do plain Trie merges if neither this nor mutation has deletions.
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

        /// Modify this trie to apply the given deletion branch. This has the same effect as applying a deletion-aware
        /// trie that contains only the given range trie as a deletion branch at its root.
        /// All deletions in the given mutation trie will be applied, removing any content and trie paths that become
        /// empty as a result of the deletions and releasing any of the trie cells that they occupied. The deletion
        /// branches of the trie will be combined with the incoming deletions, hoisting the deletion branch to the root
        /// of the trie if necessary.
        ///
        /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
        /// different than the element type for this memtable trie.
        public
        void delete(RangeTrie<E> mutation)
        throws TrieSpaceExhaustedException
        {
            apply(DeletionAwareTrie.deletionBranch(ByteComparable.EMPTY, byteComparableVersion, mutation));
        }

        // The methods below describe the position that a transformer is called for. For `deletionTransformer` this is
        // the position of the deletion, whose path continues from the root of the deletion branch in `deletionState`.

        @Override
        public int currentDepth()
        {
            return atDeletion ? deletionBranchDepth + deletionState.currentDepth : state.currentDepth;
        }

        @Override
        public byte[] getCurrentKeyBytes(int startDepth)
        {
            if (!atDeletion)
                return state.getBytes(startDepth);

            int depth = currentDepth();
            Preconditions.checkArgument(startDepth >= 0 && startDepth <= depth);
            byte[] bytes = new byte[depth - startDepth];
            int liveLength = Math.max(deletionBranchDepth - startDepth, 0);
            state.copyBytes(startDepth, startDepth + liveLength, bytes, 0);
            deletionState.copyBytes(startDepth + liveLength - deletionBranchDepth, deletionState.currentDepth, bytes, liveLength);
            return bytes;
        }

        /// @inheritDoc
        /// For a deletion, only its ancestors down to the root of the deletion branch are considered.
        @Override
        public int getNearestAncestorDepthSatisfying(Predicate<T> shouldStop)
        {
            return atDeletion
                   ? state.getNearestAncestorDepthSatisfying(shouldStop, Math.min(deletionBranchDepth, currentDepth() - 1))
                   : state.getNearestAncestorDepthSatisfying(shouldStop);
        }

        /// Get the bytes of the current key below the root of the deletion branch. The returned array can be safely
        /// modified and/or stored.
        ///
        /// This method may only be called by `deletionTransformer`.
        public byte[] getDeletionBranchKeyBytes()
        {
            return deletionState.getBytes(0);
        }
    }

    /// Creates a trie mutator that can be used to apply multiple modifications to the trie.
    ///
    /// @param liveTransformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie. The transformer can return null
    /// if the entry should not be added or preserved.
    /// @param deletionTransformer a function applied to combine overlapping deletions into a consistent view. Called
    /// even if there is no pre-existing deletion to convert the marker type. The transformer can return null if
    /// deletions cancel out or should not be preserved.
    /// @param existingDeleter a function used to apply a deletion marker to potentially delete live data. This is
    /// only called if there is both content and deletion at a given covered point. It should return null if the entry
    /// is to be deleted.
    /// @param insertedDeleter a function used to filter incoming entries that are covered by existing deletions
    /// in this trie, called only if both an entry and a deletion apply to a given point. This function is not provided
    /// with a path to the modified data.
    /// @param deletionsAtFixedPoints True if deletion branches are at predetermined positions. See
    /// [DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints].
    /// @param needsForcedCopyInLiveTrie a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in live branches. See [NodeFeatures] for details.
    /// @param needsForcedCopyInDeletions a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in deletion branches. See [NodeFeatures] for details.
    /// @param droppedContentCallback function to call when childless content is dropped because
    /// [ContentManager#shouldPreserveWithoutChildren] returned false.
    public <V, E extends RangeState<E>>
    Mutator<V, E> mutator(final UpsertTransformer<T, V> liveTransformer,
                          final UpsertTransformer<D, E> deletionTransformer,
                          final UpsertTransformer<T, E> existingDeleter,
                          final BiFunction<D, V, V> insertedDeleter,
                          boolean deletionsAtFixedPoints,
                          Predicate<NodeFeatures<V>> needsForcedCopyInLiveTrie,
                          Predicate<NodeFeatures<E>> needsForcedCopyInDeletions,
                          Consumer<? super T> droppedContentCallback)
    {
        return new Mutator<>(liveTransformer,
                             deletionTransformer,
                             existingDeleter,
                             insertedDeleter,
                             needsForcedCopyInLiveTrie,
                             needsForcedCopyInDeletions,
                             droppedContentCallback,
                             deletionsAtFixedPoints);
    }


    /// Creates a trie mutator that can be used to apply multiple modifications to the trie.
    ///
    /// @param liveTransformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie. The transformer can return null
    /// if the entry should not be added or preserved.
    /// @param deletionTransformer a function applied to combine overlapping deletions into a consistent view. Called
    /// even if there is no pre-existing deletion to convert the marker type. The transformer can return null if
    /// deletions cancel out or should not be preserved.
    /// @param existingDeleter a function used to apply a deletion marker to potentially delete live data. This is
    /// only called if there is both content and deletion at a given covered point. It should return null if the entry
    /// is to be deleted.
    /// @param insertedDeleter a function used to filter incoming entries that are covered by existing deletions
    /// in this trie, called only if both an entry and a deletion apply to a given point. This function is not provided
    /// with a path to the modified data.
    /// @param deletionsAtFixedPoints True if deletion branches are at predetermined positions. See
    /// [DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints].
    /// @param needsForcedCopyInLiveTrie a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in live branches. See [NodeFeatures] for details.
    /// @param needsForcedCopyInDeletions a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in deletion branches. See [NodeFeatures] for details.
    public <V, E extends RangeState<E>>
    Mutator<V, E> mutator(final UpsertTransformer<T, V> liveTransformer,
                          final UpsertTransformer<D, E> deletionTransformer,
                          final UpsertTransformer<T, E> existingDeleter,
                          final BiFunction<D, V, V> insertedDeleter,
                          boolean deletionsAtFixedPoints,
                          Predicate<NodeFeatures<V>> needsForcedCopyInLiveTrie,
                          Predicate<NodeFeatures<E>> needsForcedCopyInDeletions)
    {
        return new Mutator<>(liveTransformer,
                             deletionTransformer,
                             existingDeleter,
                             insertedDeleter,
                             needsForcedCopyInLiveTrie,
                             needsForcedCopyInDeletions,
                             null,
                             deletionsAtFixedPoints);
    }


    /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
    /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
    /// All of the deletions in the given mutation trie will be applied, removing any content and trie paths that become
    /// empty as a result of the deletions and releasing any of the trie cells that they occupied. The deletion branches
    /// of the trie will be combined with the incoming deletions.
    ///
    /// @param liveTransformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie. The transformer can return null
    /// if the entry should not be added or preserved.
    /// @param deletionTransformer a function applied to combine overlapping deletions into a consistent view. Called
    /// even if there is no pre-existing deletion to convert the marker type. The transformer can return null if
    /// deletions cancel out or should not be preserved.
    /// @param existingDeleter a function used to apply a deletion marker to potentially delete live data. This is
    /// only called if there is both content and deletion at a given covered point. It should return null if the entry
    /// is to be deleted.
    /// @param insertedDeleter a function used to filter incoming entries that are covered by existing deletions
    /// in this trie, called only if both an entry and a deletion apply to a given point. This function is not provided
    /// with a path to the modified data.
    /// @param deletionsAtFixedPoints True if deletion branches are at predetermined positions. See
    /// [DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints].
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in both live and deletion branches. See [NodeFeatures] for details.
    @SuppressWarnings({"rawtypes", "unchecked"})
    public <V, E extends RangeState<E>>
    Mutator<V, E> mutator(final UpsertTransformer<T, V> liveTransformer,
                          final UpsertTransformer<D, E> deletionTransformer,
                          final UpsertTransformer<T, E> existingDeleter,
                          final BiFunction<D, V, V> insertedDeleter,
                          boolean deletionsAtFixedPoints,
                          Predicate<NodeFeatures<?>> needsForcedCopy)
    {
        return mutator(liveTransformer,
                       deletionTransformer,
                       existingDeleter,
                       insertedDeleter,
                       deletionsAtFixedPoints,
                       (Predicate) needsForcedCopy,
                       (Predicate) needsForcedCopy);
    }

    /// Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
    /// with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
    /// All of the deletions in the given mutation trie will be applied, removing any content and trie paths that become
    /// empty as a result of the deletions and releasing any of the trie cells that they occupied. The deletion branches
    /// of the trie will be combined with the incoming deletions.
    ///
    /// @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
    /// different than the element type for this memtable trie.
    /// @param liveTransformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie. The transformer can return null
    /// if the entry should not be added or preserved.
    /// @param deletionTransformer a function applied to combine overlapping deletions into a consistent view. Called
    /// even if there is no pre-existing deletion to convert the marker type. The transformer can return null if
    /// deletions cancel out or should not be preserved.
    /// @param existingDeleter a function used to apply a deletion marker to potentially delete live data. This is
    /// only called if there is both content and deletion at a given covered point. It should return null if the entry
    /// is to be deleted.
    /// @param insertedDeleter a function used to filter incoming entries that are covered by existing deletions
    /// in this trie, called only if both an entry and a deletion apply to a given point. This function is not provided
    /// with a path to the modified data.
    /// @param liveTransformer a function applied to the potentially pre-existing value for the given key, and the new
    /// value. Applied even if there's no pre-existing value in the memtable trie. The transformer can return null
    /// if the entry should not be added or preserved.
    /// @param deletionTransformer a function applied to combine overlapping deletions into a consistent view. Called
    /// even if there is no pre-existing deletion to convert the marker type. The transformer can return null if
    /// deletions cancel out or should not be preserved.
    /// @param existingDeleter a function used to apply a deletion marker to potentially delete live data. This is
    /// only called if there is both content and deletion at a given covered point. It should return null if the entry
    /// is to be deleted.
    /// @param insertedDeleter a function used to filter incoming entries that are covered by existing deletions
    /// in this trie, called only if both an entry and a deletion apply to a given point. This function is not provided
    /// with a path to the modified data.
    /// @param deletionsAtFixedPoints True if deletion branches are at predetermined positions. See
    /// [DeletionAwareTrie.MergeResolver#deletionsAtFixedPoints].
    /// @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity
    /// guarantees to concurrent readers, applied in both live and deletion branches. See [NodeFeatures] for details.
    public <V, E extends RangeState<E>>
    void apply(BasicDeletionAwareTrie<V, E> mutation,
               final UpsertTransformer<T, V> liveTransformer,
               final UpsertTransformer<D, E> deletionTransformer,
               final UpsertTransformer<T, E> existingDeleter,
               final BiFunction<D, V, V> insertedDeleter,
               boolean deletionsAtFixedPoints,
               Predicate<NodeFeatures<?>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        // TODO: track hasDeletions and do plain Trie merges if neither this nor mutation has deletions.
        mutator(liveTransformer,
                deletionTransformer,
                existingDeleter,
                insertedDeleter,
                deletionsAtFixedPoints,
                needsForcedCopy)
        .apply(mutation);
    }

    private class DumpCursor extends InMemoryReadTrie<T>.DumpCursor<DeletionAwareInMemoryCursor<T, D>> implements DeletionAwareCursor<String, D>
    {
        DumpCursor(DeletionAwareInMemoryCursor<T, D> source, Function<T, String> contentToString)
        {
            super(source, contentToString);
        }


        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return source.deletionBranchCursor(direction);
        }

        @Override
        public DumpCursor tailCursor(Direction direction)
        {
            // `DumpCursor` is only created by the dump method to be used immediately, and that use should never call
            // `tailCursor`.
            throw new AssertionError();
        }
    }

    public String dump(Function<T, String> contentToString)
    {
        return dump(contentToString, Object::toString);
    }

    /// Override of dump to provide more detailed printout that includes the type of each node in the trie.
    /// We do this via a wrapping cursor that returns a content string for the type of node for every node we return.
    public String dump(Function<T, String> contentToString, Function<D, String> rangeToString)
    {
        return new DumpCursor(makeCursor(Direction.FORWARD), contentToString).process(new TrieDumper.DeletionAware<>(Function.identity(), rangeToString));
    }

    /// Dump the branch rooted at the given node. To be used for debugging only.
    @SuppressWarnings("unused")
    private String dumpBranch(int branchRoot)
    {
        return new DumpCursor(new DeletionAwareInMemoryCursor<>(this, Direction.FORWARD, branchRoot), Object::toString)
               .process(new TrieDumper.DeletionAware<>(Function.identity(), Object::toString));
    }
}
