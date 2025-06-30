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
            return new DeletionAwareInMemoryCursor<>(trie, direction, currentFullNode, -1, -1);
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
        return new DeletionAwareInMemoryCursor<>(this, direction, root, -1, -1);
    }

    static class Mutation<T, D extends RangeState<D>, V, E extends RangeState<E>>
    extends InMemoryBaseTrie.Mutation<T, V, DeletionAwareMergeSource<V, E>>
    {
        final UpsertTransformerWithKeyProducer<D, E> deletionTransformer;
        final UpsertTransformerWithKeyProducer<E, T> deleter;
        final boolean deletionsAtFixedPoints;

        Mutation(UpsertTransformerWithKeyProducer<T, V> dataTransformer,
                 UpsertTransformerWithKeyProducer<D, E> deletionTransformer,
                 UpsertTransformerWithKeyProducer<E, T> existingDeleter,
                 BiFunction<D, V, V> insertedDeleter,
                 Predicate<NodeFeatures<T>> needsForcedCopy,
                 boolean deletionsAtFixedPoints,
                 DeletionAwareCursor<V, E> mutationCursor,
                 InMemoryBaseTrie<T>.ApplyState state)
        {
            super(dataTransformer, needsForcedCopy, new DeletionAwareMergeSource<>(insertedDeleter, mutationCursor), state);
            this.deletionTransformer = deletionTransformer;
            this.deleter = existingDeleter;
            this.deletionsAtFixedPoints = deletionsAtFixedPoints;

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
        }

        @Override
        void apply() throws TrieSpaceExhaustedException
        {
            int depth = state.currentDepth;
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                int existingAlternateBranch = state.alternateBranch();
                int updatedAlternateBranch = existingAlternateBranch;
                RangeCursor<D> incomingAlternateBranch = mutationCursor.deletionBranchCursor(Direction.FORWARD);
                if (incomingAlternateBranch != null || existingAlternateBranch != NONE)
                {
                    if (!deletionsAtFixedPoints && incomingAlternateBranch == null)
                    {
                        // The incoming cursor has no deletions here, but it may have some below this point.
                        // Switch to deletion branch to transform them to be rooted here.
                        // (Note: this will cause a lot of processing of unproductive branches.)
                        incomingAlternateBranch = new DeletionAwareCursor.DeletionsTrieCursor<>(mutationCursor.tailCursor(Direction.FORWARD));
                    }

                    RangeCursor<D> ourDeletionBranch;
                    if (!deletionsAtFixedPoints && existingAlternateBranch == NONE && state.existingFullNode() != NONE)
                    {
                        // We may have alternate branches below this point. If so, we need to delete these branches, but
                        // take them into account for the deletion branch we are now building.
                        DeletionAwareCursor<T, D> ourBranch = new DeletionAwareInMemoryCursor<>(state.trie(), Direction.FORWARD, state.existingFullNode(), 0, -1);
                        ourDeletionBranch = new DeletionAwareCursor.DeletionsTrieCursor<>(ourBranch);
                    }
                    else
                        ourDeletionBranch = ((InMemoryDeletionAwareTrie<T, D>) state.trie()).makeRangeCursor(Direction.FORWARD, existingAlternateBranch);

                    // stop checking alternateBranch below this point

                    if (incomingAlternateBranch != null)
                    {
                        // duplicate cursor as we need it for both deletion and data branches
                        RangeCursor<D> deletionBranch = incomingAlternateBranch.tailCursor(Direction.FORWARD);
                        applyDeletions(incomingAlternateBranch);
                        updatedAlternateBranch = mergeDeletionBranch(existingAlternateBranch, deletionBranch);
                    }

                    // Continue processing to also insert the incoming data at this branch. We need to attach the updated alternate branch
                    applyDataUnderDeletion(ourDeletionBranch);
                }

                applyContentAndAlternateBranch(updatedAlternateBranch);

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

            // Below is the same as the normal path, but ignores deletion branches.
            int depth = state.currentDepth;
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                applyContent();

                depth = mutationCursor.advance();
                if (!state.advanceTo(depth, mutationCursor.incomingTransition(), forcedCopyDepth))
                    break;
                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
            }
        }

        private void applyDeletions(RangeCursor<D> incomingAlternateBranch) throws TrieSpaceExhaustedException
        {
            // Apply the deletion branch to our data.
            // This needs to remove any lower-level deletion branches.
            state.setDepthCorrection(-state.currentDepth);
            InMemoryTrie.DeleteMutation<T, D, RangeCursor<D>> deleteMutation = new InMemoryTrie.DeleteMutation<>(deleter, needsForcedCopy, incomingAlternateBranch, state);
            deleteMutation.apply();
        }

        private int mergeDeletionBranch(int existingAlternateBranch, RangeCursor<D> deletionBranch) throws TrieSpaceExhaustedException
        {
            // Merge the deletion branch into our deletion branch.
            // This needs to release any dropped cells.
            state.descendIntoAlternate(existingAlternateBranch);
            state.setDepthCorrection(-state.currentDepth);
            InMemoryRangeTrie.Mutation<D, D> rangeMutation = new InMemoryRangeTrie.Mutation<>(deletionTransformer,
                                                                                              (Predicate<NodeFeatures<D>>) (Predicate) needsForcedCopy,
                                                                                              deletionBranch,
                                                                                              state);
            rangeMutation.apply();
            state.attachAndMoveToParentState(forcedCopyDepth);
            return rangeMutation.state.updatedPostContentNode();
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
               final UpsertTransformer<T, V> dataTransformer,
               final UpsertTransformer<D, E> deletionTransformer,
               final UpsertTransformer<T, E> deleter,
               boolean deletionsAtFixedPoints)
    throws TrieSpaceExhaustedException
    {
        try
        {
            Mutation<T, D, V, E> m = new Mutation<>(dataTransformer, deletionTransformer, deleter, deletionsAtFixedPoints, mutation.cursor(Direction.FORWARD), applyState.start());
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
}
