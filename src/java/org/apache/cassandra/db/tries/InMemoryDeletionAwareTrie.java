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

/**
 * @param <T>
 * @param <D> Must be a subtype of T.
 */
public class InMemoryDeletionAwareTrie<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
extends InMemoryBaseTrie<T> implements DeletionAwareTrie<T, D>
{
    public InMemoryDeletionAwareTrie(ByteComparable.Version byteComparableVersion, BufferType bufferType, ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(byteComparableVersion, bufferType, lifetime, opOrder);
    }

    public static <T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
    }

    public static <T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    InMemoryDeletionAwareTrie<T, D> shortLived(ByteComparable.Version byteComparableVersion, BufferType bufferType)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.SHORT, null);
    }

    public static <T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, OpOrder opOrder)
    {
        return longLived(byteComparableVersion, BufferType.OFF_HEAP, opOrder);
    }

    public static <T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    InMemoryDeletionAwareTrie<T, D> longLived(ByteComparable.Version byteComparableVersion, BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryDeletionAwareTrie<>(byteComparableVersion, bufferType, ExpectedLifetime.LONG, opOrder);
    }


    static class DeletionAwareInMemoryCursor<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
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
            return isNull(alternateBranch)
                   ? null
                   : new InMemoryRangeTrie.InMemoryRangeCursor<>((InMemoryReadTrie) trie, direction, alternateBranch, depth() - 1, incomingTransition());
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            return new DeletionAwareInMemoryCursor<>(trie, direction, currentFullNode, -1, -1);
        }
    }

    @Override
    public DeletionAwareInMemoryCursor<T, D> makeCursor(Direction direction)
    {
        return new DeletionAwareInMemoryCursor<>(this, direction, root, -1, -1);
    }

    static class Mutation<T extends Deletable, D extends DeletionMarker<T, D>,
                          C extends DeletionAwareCursor<T, D>>
    extends InMemoryBaseTrie.Mutation<T, T, C>
    {
        final UpsertTransformerWithKeyProducer<D, D> deletionTransformer;
        final UpsertTransformerWithKeyProducer<D, T> deleter;

        Mutation(UpsertTransformerWithKeyProducer<T, T> dataTransformer,
                 UpsertTransformerWithKeyProducer<D, D> deletionTransformer,
                 UpsertTransformerWithKeyProducer<D, T> deleter,
                 Predicate<NodeFeatures<T>> needsForcedCopy,
                 C mutationCursor,
                 InMemoryBaseTrie<T>.ApplyState state)
        {
            super(dataTransformer, needsForcedCopy, mutationCursor, state);
            this.deletionTransformer = deletionTransformer;
            this.deleter = deleter;

            // pain points:
            // - Deletion introduction may be at a different level. If this happens, we need to uplift the other branch
            //   to that level. This means doing DeletionBranchCursor, which has to walk every data point (this may be
            //   _very_ expensive; note that no nodes will be created).
            // - The above also happens if we don't have an existing deletion at all.
            // - Assuming uplift only happens once, this is not fatal.
            // - Probably sensible to apply deletions on the branch first to reduce its size.
            // - LATER: We'd prefer to never do it; i.e. to always have a deletion branch (with some special empty designation)
            //   for every partition.
            // - LATER: We want a static "partition-level marker" content value that we can use for that.

            // plan:
            // if incoming and existing deletions match, apply:
            // - incoming deletion branch to our data using DeleteMutation
            // - incoming deletion branch to our deletion branch using InMemoryRangeTrie.Mutation
            // - incoming data, with applied existing deletion branch (using DeletionAwareSource) to our data
            // if they don't match, make DeletionBranchCursor on the other to turn it into a deletion branch.
            // - this is likely wasteful
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
                RangeCursor<D> incomingAlternateBranch = mutationCursor.deletionBranchCursor(Direction.FORWARD);
                if (incomingAlternateBranch != null || existingAlternateBranch != NONE)
                {
                    if (incomingAlternateBranch == null)
                    {
                        // The incoming cursor has no deletions here, but it may have some below this point.
                        // Switch to deletion branch to transform them to be rooted here.
                        // (Note: this can cause a lot of processing of unproductive branches.)
                        incomingAlternateBranch = new DeletionAwareCursor.DeletionsTrieCursor<>(mutationCursor.tailCursor(Direction.FORWARD));
                    }
                    // duplicate cursor as we need it for both deletion and data branches
                    RangeCursor<D> deletionBranch = incomingAlternateBranch.tailCursor(Direction.FORWARD);

                    RangeCursor<D> ourDeletionBranch;
                    if (existingAlternateBranch == NONE)
                    {
                        // We may have alternate branches below this point. If so, we need to delete these branches, but
                        // take them into account for the deletion branch we are now building.
                        DeletionAwareCursor<T, D> ourBranch = new DeletionAwareInMemoryCursor<>(state.trie(), Direction.FORWARD, state.existingPostContentNode(), 0, -1);
                        ourDeletionBranch = new DeletionAwareCursor.DeletionsTrieCursor<>(ourBranch);


                        deletionBranch = new MergeCursor.Range<>((x, y) -> deletionTransformer.apply(x, y, null), deletionBranch, ourDeletionBranch.tailCursor(Direction.FORWARD));
                    }
                    else
                        ourDeletionBranch = new InMemoryRangeTrie.InMemoryRangeCursor<>(state.trie(), Direction.FORWARD, existingAlternateBranch, 0, -1);

                    int updatedAlternateBranch = mergeDeletionBranch(existingAlternateBranch, deletionBranch);
                    applyDeletions(incomingAlternateBranch);

                    // Continue processing to also insert the incoming data at this branch. We need to attach the updated alternate branch
                    applyDataUnderDeletion(ourDeletionBranch);
                }

                applyContent();

                depth = mutationCursor.advance();
                if (!state.advanceTo(depth, mutationCursor.incomingTransition(), forcedCopyDepth))
                    break;
                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
            }
        }

        private void applyDataUnderDeletion(RangeCursor<D> ourDeletionBranch) throws TrieSpaceExhaustedException
        {
            // Add to DeletionAwareMergeSource
            mutationCursor.addDeletions(ourDeletionBranch, currentDepth);

            // Below is the same as the normal path, but also deletes/releases existing alternate branches
            int depth = state.currentDepth;
            while (true)
            {
                if (depth < forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                int existingAlternateBranch = state.alternateBranch();
                if (existingAlternateBranch != NONE)
                {
                    // release the existing alternate branch (on the way up, incoming data must still be filtered by it)
                }

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
            InMemoryTrie.DeleteMutation<T, E, RangeCursor<E>> deleteMutation = new InMemoryTrie.DeleteMutation<>(deleter, needsForcedCopy, incomingAlternateBranch, state);
            deleteMutation.apply();
        }

        private int mergeDeletionBranch(int existingAlternateBranch, RangeCursor<D> deletionBranch) throws TrieSpaceExhaustedException
        {
            // Merge the deletion branch into our deletion branch.
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
    public <V extends Deletable, E extends DeletionMarker<V, E>>
    void apply(DeletionAwareTrie<V, E> mutation,
               final UpsertTransformer<T, V> dataTransformer,
               final UpsertTransformer<D, E> deletionTransformer,
               final UpsertTransformer<T, E> deleter)
    throws TrieSpaceExhaustedException
    {
        DeletionAwareCursor<V, E> mutationCursor = mutation.cursor(Direction.FORWARD);
        assert mutationCursor.depth() == 0 : "Unexpected non-fresh cursor.";
        ApplyState state = applyState.start();
        assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        apply(state, mutationCursor, dataTransformer, deletionTransformer, deleter);
        assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        // TODO
        state.attachRoot(Integer.MAX_VALUE);
    }

    @SuppressWarnings("unchecked")
    private static <U extends Deletable, T extends U, D extends DeletionMarker<T, D>, V extends Deletable, E extends DeletionMarker<V, E>>
    void apply(InMemoryTrie<U>.ApplyState stateTyped,
               DeletionAwareCursor<V, E> mutationCursor,
               final UpsertTransformer<T, V> dataTransformer,
               final UpsertTransformer<D, E> deletionTransformer,
               final UpsertTransformer<T, E> deleter)
    throws TrieSpaceExhaustedException
    {
        @SuppressWarnings("rawtypes")   // We use a raw ApplyState to be able to treat this trie as deterministic on T as well as range on D
        InMemoryRangeTrie.ApplyState state = stateTyped;

        int prevAscendLimit = state.setAscendLimit(state.currentDepth);
        while (true)
        {
            RangeCursor<E> deletionBranch = mutationCursor.deletionBranchCursor();
            if (deletionBranch != null)
            {
                // Apply deletion to our deletion branch.
                // Note: we don't ensure no covering deletion branches, and we don't delete stuff in the inserted branch
                // that is deleted by our deletion branch.
                // TODO: Maybe we should (one way to do it is to union input with our deletion trie).
                state.descendToAlternate();
                InMemoryRangeTrie.applyRanges(state, deletionBranch, deletionTransformer);
                // TODO
                state.attachAlternate(false);
                // Apply the same deletion to live branch.
                deletionBranch = mutationCursor.deletionBranch();
                delete(state, deletionBranch, deleter);
            }
            else
                applyContent(state, mutationCursor, dataTransformer);

            int depth = mutationCursor.advance();
            // TODO
            if (state.advanceTo(depth, mutationCursor.incomingTransition(), Integer.MAX_VALUE))
                break;
            assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
        }
        state.setAscendLimit(prevAscendLimit);
    }
}
