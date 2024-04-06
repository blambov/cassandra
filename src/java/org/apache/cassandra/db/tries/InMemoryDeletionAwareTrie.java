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

/**
 *
 * @param <U> Common superclass of the deletable and the deletion marker.
 * @param <T>
 * @param <D> Must be a subtype of U.
 */
public class InMemoryDeletionAwareTrie<U extends DeletionAwareTrie.Deletable, T extends U, D extends DeletionAwareTrie.DeletionMarker<T, D>>
extends InMemoryTrie<U> implements DeletionAwareTrieWithImpl<T, D>
{
    public InMemoryDeletionAwareTrie(BufferType bufferType)
    {
        super(bufferType);
    }

    static class DeletionAwareCursor<U extends DeletionAwareTrie.Deletable, T extends U, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends MemtableCursor<U> implements Cursor<T, D>
    {
        DeletionAwareCursor(InMemoryReadTrie<U> trie, int root, int depth, int incomingTransition)
        {
            super(trie, root, depth, incomingTransition);
        }

        DeletionAwareCursor(DeletionAwareCursor<U, T, D> copyFrom)
        {
            super(copyFrom);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T content()
        {
            return (T) content;
        }

        @Override
        public RangeTrieImpl.Cursor<D> deletionBranch()
        {
            return isNull(alternateBranch)
                   ? null
                   : new InMemoryRangeTrie.RangeCursor<>(trie, alternateBranch, depth() - 1, incomingTransition());
        }

        @Override
        public DeletionAwareCursor<U, T, D> duplicate()
        {
            return new DeletionAwareCursor<>(this);
        }
    }

    @Override
    public Cursor<T, D> makeCursor()
    {
        return new DeletionAwareCursor<>(this, root, -1, -1);
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
    throws SpaceExhaustedException
    {
        DeletionAwareTrieImpl.Cursor<V, E> mutationCursor = DeletionAwareTrieImpl.impl(mutation).cursor();
        assert mutationCursor.depth() == 0 : "Unexpected non-fresh cursor.";
        ApplyState state = applyState.start();
        assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        apply(state, mutationCursor, dataTransformer, deletionTransformer, deleter);
        assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        state.attachRoot();
    }

    @SuppressWarnings("unchecked")
    private static <U extends Deletable, T extends U, D extends DeletionMarker<T, D>, V extends Deletable, E extends DeletionMarker<V, E>>
    void apply(InMemoryTrie<U>.ApplyState stateTyped,
               DeletionAwareTrieImpl.Cursor<V, E> mutationCursor,
               final UpsertTransformer<T, V> dataTransformer,
               final UpsertTransformer<D, E> deletionTransformer,
               final UpsertTransformer<T, E> deleter)
    throws SpaceExhaustedException
    {
        @SuppressWarnings("rawtypes")   // We use a raw ApplyState to be able to treat this trie as deterministic on T as well as range on D
        InMemoryTrie.ApplyState state = stateTyped;

        int prevAscendLimit = state.setAscendLimit(state.currentDepth);
        while (true)
        {
            RangeTrieImpl.Cursor<E> deletionBranch = mutationCursor.deletionBranch();
            if (deletionBranch != null)
            {
                // Apply deletion to our deletion branch.
                // Note: we don't ensure no covering deletion branches, and we don't delete stuff in the inserted branch
                // that is deleted by our deletion branch.
                // TODO: Maybe we should (one way to do it is to union input with our deletion trie).
                state.descendToAlternate();
                InMemoryRangeTrie.applyRanges(state, deletionBranch, deletionTransformer);
                state.attachAlternate();
                // Apply the same deletion to live branch.
                deletionBranch = mutationCursor.deletionBranch();
                delete(state, deletionBranch, deleter);
            }
            else
                applyContent(state, mutationCursor, dataTransformer);

            int depth = mutationCursor.advance();
            if (state.advanceTo(depth, mutationCursor.incomingTransition()))
                break;
            assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
        }
        state.setAscendLimit(prevAscendLimit);
    }

    /**
     * Override of dump to provide more detailed printout that includes the type of each node in the trie.
     * We do this via a wrapping cursor that returns a content string for the type of node for every node we return.
     */
    @SuppressWarnings("unchecked")
    @Override
    public String dump(Function<T, String> contentToString)
    {
        return dump(x -> contentToString.apply((T) x), root);
    }
}
