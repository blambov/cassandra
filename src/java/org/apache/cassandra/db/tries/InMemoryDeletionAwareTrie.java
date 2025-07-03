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
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * @param <T>
 * @param <D> Must be a subtype of T.
 */
public class InMemoryDeletionAwareTrie<T, D extends RangeState<D>>
extends InMemoryBaseTrie<T> implements DeletionAwareTrie<T, D>
{
    public InMemoryDeletionAwareTrie(ByteComparable.Version byteComparableVersion, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(byteComparableVersion, bufferType, lifetime, opOrder);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.SHORT, null);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLived(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    public static <T, D extends RangeState<D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.LONG, opOrder);
    }


    static class DeletionAwareInMemoryCursor<T, D extends RangeState<D>>
    extends InMemoryCursor<T> implements DeletionAwareCursor<T, D>
    {
        DeletionAwareInMemoryCursor(InMemoryReadTrie<T> trie, Direction direction, int root, int depth, int incomingTransition)
        {
            super(trie, direction, root, depth, incomingTransition);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T content()
        {
            return (T) content;
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
            return new DeletionAwareInMemoryCursor<>(trie, direction, currentFullNode, 0, -1);
        }
    }

    @SuppressWarnings("rawtypes")
    private RangeCursor<D> makeRangeCursor(Direction direction, int alternateBranch) {
        return isNull(alternateBranch)
                ? null
                : new InMemoryRangeTrie.InMemoryRangeCursor<>((InMemoryReadTrie) this, direction, alternateBranch, 0, -1);
    }

    @Override
    public DeletionAwareInMemoryCursor<T, D> makeCursor(Direction direction)
    {
        return new DeletionAwareInMemoryCursor<>(this, direction, root, 0, -1);
    }

    @SuppressWarnings("unchecked")
    InMemoryTrie<D>.ApplyState deletionState = (InMemoryTrie<D>.ApplyState) new ApplyState();

    static class Mutation<T, D extends RangeState<D>, V, E extends RangeState<E>>
    extends InMemoryBaseTrie.Mutation<T, V, DeletionAwareMergeSource<V, E, D>>
    {
        final UpsertTransformerWithKeyProducer<D, E> deletionTransformer;
        final UpsertTransformerWithKeyProducer<T, E> deleter;
        final boolean deletionsAtFixedPoints;
        final InMemoryTrie<D>.ApplyState deletionState;

        Mutation(UpsertTransformerWithKeyProducer<T, V> dataTransformer,
                 UpsertTransformerWithKeyProducer<D, E> deletionTransformer,
                 UpsertTransformerWithKeyProducer<T, E> existingDeleter,
                 BiFunction<D, V, V> insertedDeleter,
                 Predicate<NodeFeatures<V>> needsForcedCopy,
                 boolean deletionsAtFixedPoints,
                 DeletionAwareCursor<V, E> mutationCursor,
                 InMemoryBaseTrie<T>.ApplyState state,
                 InMemoryBaseTrie<D>.ApplyState deletionState)
        {
            super(dataTransformer, needsForcedCopy, new DeletionAwareMergeSource<>(insertedDeleter, mutationCursor), state);
            this.deletionTransformer = deletionTransformer;
            this.deleter = existingDeleter;
            this.deletionsAtFixedPoints = deletionsAtFixedPoints;
            this.deletionState = deletionState;

            // pain points:
            // - Deletion introduction may be at a different level. If this happens, we need to uplift the other branch
            //   to that level. This means doing DeletionBranchCursor, which has to walk every data point (this may be
            //   _very_ expensive; note that no nodes will be created).
            // - The above also happens if we don't have an existing deletion at all.
            // - Assuming uplift only happens once, this is not fatal.
            // - Probably sensible to apply deletions on the branch first to reduce its size.

            // plan:
            // - Add a flag "deletionsAtKnownPositions" that guarantees that if one merge source has deletion branch
            //   at some position, the other cannot have a deletion branch below or above this position.
            //   This lets us avoid creating DeletionBranchCursors when one source is missing.
            // if incoming and existing deletions match, or flag above is in force, apply:
            // - incoming deletion branch to our data using DeleteMutation
            // - incoming deletion branch to our deletion branch using InMemoryRangeTrie.Mutation
            // - incoming data, with applied existing deletion branch (using DeletionAwareSource) to our data
            // if they don't match, make DeletionBranchCursor on the other to turn it into a deletion branch.
            // - this is likely wasteful
            // TODO: add deletionsAtKnownPositions flag to MergeCursor too;
            //   - possibly a property of trie / cursor
            //   - maybe a DeletionAware subclass
            // TODO: track if in-memory trie has any deletions and accept a "mayHaveDeletions" flag to apply()
            //   - simplify apply() for the resulting special cases

            // What if we walk all four sources in parallel? With several methods that we switch between:
            // - deletion-aware on both sides, no deletion branch yet
            // - data + deletion cursor on both sides, building data and deletion
            // - data only on both sides (use base class methods)
            // - deletion only on both sides (range class methods)
            // - deletion-aware on one side, data + deletion on the other (only for !deletionsAtFixedPoints)
            // --- two versions
            // --- need to go down all the way to look for deletion branches
        }

        @Override
        void apply() throws TrieSpaceExhaustedException
        {
            int depth = state.currentDepth;
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                applyContent();

                int existingAlternateBranch = state.alternateBranch();
                RangeCursor<E> incomingAlternateBranch = mutationCursor.deletionBranchCursor(Direction.FORWARD);
                if (incomingAlternateBranch != null || existingAlternateBranch != NONE)
                {
                    int updatedAlternateBranch = existingAlternateBranch;
                    RangeCursor<D> ourDeletionBranch;
                    if (!deletionsAtFixedPoints && existingAlternateBranch == NONE && state.existingFullNode() != NONE)
                    {
                        // TODO: track hasDeletions and skip this if !hasDeletions
                        // Move any covered deletion branches up to this depth so that we can correctly merge the
                        // incoming deletions.
                        updatedAlternateBranch = hoistOurDeletionBranches();
                    }
                    ourDeletionBranch = ((InMemoryDeletionAwareTrie<T, D>) state.trie()).makeRangeCursor(Direction.FORWARD, updatedAlternateBranch);

                    if (!deletionsAtFixedPoints && incomingAlternateBranch == null)
                    {
                        // The incoming cursor has no deletions here, but it may have some below this point.
                        // Switch to deletion branch to transform them to be rooted here.
                        // (Note: this will cause a lot of processing of unproductive branches.)
                        incomingAlternateBranch = new DeletionAwareCursor.DeletionsTrieCursor<>(mutationCursor.tailCursor(Direction.FORWARD));
                    }

                    if (incomingAlternateBranch != null)
                    {
                        // duplicate cursor as we need it for both deletion and data branches
                        RangeCursor<E> deletionBranch = incomingAlternateBranch.tailCursor(Direction.FORWARD);

                        // Delete data that is covered by the new deletions.
                        applyDeletions(incomingAlternateBranch);

                        // Merge the deletions into our deletion branch.
                        updatedAlternateBranch = mergeDeletionBranch(updatedAlternateBranch, deletionBranch);
                    }

                    // Continue processing to also insert the incoming data at this branch.
                    applyDataUnderDeletion(ourDeletionBranch);

                    // ascend and apply alternate branch
                    state.alternateBranchToAttach = updatedAlternateBranch;
                    if (state.currentDepth == 0)
                        break; // to be attached to root by complete()
                    state.attachAndMoveToParentState(forcedCopyDepth);
                    depth = mutationCursor.depth();
                }
                else
                    depth = mutationCursor.advance();

                if (!state.advanceTo(depth, mutationCursor.incomingTransition(), forcedCopyDepth))
                    break;
                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
            }
        }

        private void applyDataUnderDeletion(RangeCursor<D> ourDeletionBranch) throws TrieSpaceExhaustedException
        {
            // Add to DeletionAwareMergeSource
            if (ourDeletionBranch != null)
                mutationCursor.addDeletions(ourDeletionBranch);
            int initialDepth = state.currentDepth;

            // Below is the same as the normal path, but ignores deletion branches.
            int depth = state.currentDepth;
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                applyContent();

                depth = mutationCursor.advance();
                if (!state.advanceTo(depth, mutationCursor.incomingTransition(), forcedCopyDepth, initialDepth))
                    break;
                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
            }
            assert state.currentDepth == initialDepth;
        }

        private void applyDeletions(RangeCursor<E> incomingAlternateBranch) throws TrieSpaceExhaustedException
        {
            // Apply the deletion branch to our data.
            InMemoryTrie.DeleteMutation<T, E, RangeCursor<E>> deleteMutation = new InMemoryTrie.DeleteMutation<>(
                    deleter,
                    (Predicate<NodeFeatures<E>>) (Predicate) needsForcedCopy,
                    incomingAlternateBranch,
                    state);
            deleteMutation.apply();

            // Make sure the next data pass below walks the updated branch.
            state.prepareToWalkBranchAgain(forcedCopyDepth);
        }

        private int mergeDeletionBranch(int existingAlternateBranch, RangeCursor<E> deletionBranch) throws TrieSpaceExhaustedException
        {
            // Merge the deletion branch into our deletion branch.
            InMemoryRangeTrie.Mutation<D, E> rangeMutation = new InMemoryRangeTrie.Mutation<>(
                    deletionTransformer,
                    (Predicate<NodeFeatures<E>>) (Predicate) needsForcedCopy,
                    deletionBranch,
                    deletionState.start(existingAlternateBranch));
            rangeMutation.apply();
            return deletionState.completeBranch(forcedCopyDepth);
        }

        private int hoistOurDeletionBranches() throws TrieSpaceExhaustedException {
            // Walk all of our data branch and build new branches corresponding to it. When we reach a deletion
            // branch, link it. If a branch is walked without finding a deletion branch, the returned NONEs should
            // propagate up.
            // We need to walk both the deletion-aware/data trie, as well as the deletion branch being built, so that
            // the existing deletion branch mappings can be removed.
            deletionState.start(NONE);
            int initialDepth = state.currentDepth;

            int depth = state.currentDepth;
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                int existingAlternateBranch = state.alternateBranch();
                if (existingAlternateBranch != NONE)
                {
                    deletionState.attachBranchAndMoveToParentState(existingAlternateBranch, forcedCopyDepth);
                    // Drop the existing alternate branch from the main state and ascend.
                    // The normal applyContent() method uses alternate branch value of NONE.
                    state.attachAndMoveToParentState(forcedCopyDepth);
                }

                if (!state.advanceToNextExisting(forcedCopyDepth, initialDepth))
                    break;
                depth = state.currentDepth;
                deletionState.advanceTo(depth - initialDepth, state.incomingTransition(), forcedCopyDepth - initialDepth);
            }
            if (deletionState.currentDepth > 0)
                deletionState.advanceTo(-1, -1, forcedCopyDepth - initialDepth);

            // Make sure next walks over the data branch use the updated branch.
            state.prepareToWalkBranchAgain(forcedCopyDepth);
            return deletionState.completeBranch(forcedCopyDepth - initialDepth);
        }
    }



    /**
     * Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
     * with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
     * @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
     * different than the element type for this memtable trie.
     * @param dataTransformer a function applied to the potentially pre-existing value for the given key, and the new
     * value. Applied even if there's no pre-existing value in the memtable trie.
     */
    public <V, E extends RangeState<E>>
    void apply(DeletionAwareTrie<V, E> mutation,
               final UpsertTransformerWithKeyProducer<T, V> dataTransformer,
               final UpsertTransformerWithKeyProducer<D, E> deletionTransformer,
               final UpsertTransformerWithKeyProducer<T, E> existingDeleter,
               final BiFunction<D, V, V> insertedDeleter,
               boolean deletionsAtFixedPoints,
               Predicate<NodeFeatures<V>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        try
        {
            Mutation<T, D, V, E> m = new Mutation<>(dataTransformer,
                    deletionTransformer,
                    existingDeleter,
                    insertedDeleter,
                    needsForcedCopy,
                    deletionsAtFixedPoints,
                    mutation.cursor(Direction.FORWARD),
                    applyState.start(),
                    deletionState);
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

    public <V, E extends RangeState<E>>
    void apply(DeletionAwareTrie<V, E> mutation,
               final UpsertTransformer<T, V> dataTransformer,
               final UpsertTransformer<D, E> deletionTransformer,
               final UpsertTransformer<T, E> existingDeleter,
               final BiFunction<D, V, V> insertedDeleter,
               boolean deletionsAtFixedPoints,
               Predicate<NodeFeatures<V>> needsForcedCopy)
            throws TrieSpaceExhaustedException
    {
        apply(mutation,
                (UpsertTransformerWithKeyProducer<T, V>) dataTransformer,
                deletionTransformer, existingDeleter, insertedDeleter, deletionsAtFixedPoints, needsForcedCopy);
    }

    class DumpCursor extends InMemoryReadTrie<T>.DumpCursor<DeletionAwareInMemoryCursor<T, D>> implements DeletionAwareCursor<String, D>
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

    private String dumpBranch(int branchRoot)
    {
        return new DumpCursor(new DeletionAwareInMemoryCursor<>(this, Direction.FORWARD, branchRoot, 0, -1), Object::toString)
               .process(new TrieDumper.DeletionAware<>(Function.identity(), Object::toString));
    }
}
