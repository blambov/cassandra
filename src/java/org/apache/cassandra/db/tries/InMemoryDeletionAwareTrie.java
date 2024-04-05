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


//    /**
//     * Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
//     * with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
//     * @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
//     * different than the element type for this memtable trie.
//     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
//     * value. Applied even if there's no pre-existing value in the memtable trie.
//     */
//    public <V extends Deletable, W extends V, E extends DeletionMarker<W, E>>
//    void apply(DeletionAwareTrie<W, E> mutation, final UpsertTransformer<U, V> transformer) throws SpaceExhaustedException
//    {
//    }
//
//    private <V extends Deletable, W extends V, E extends DeletionMarker<W, E>>
//    void apply(ApplyState state, DeletionAwareTrieImpl.Cursor<W, E> mutationCursor, final UpsertTransformer<U, V> transformer) throws SpaceExhaustedException
//    {
//    }

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

    // TODO: Implement InMemoryDTrie.delete using a range and test it with set intersections
    // TODO: Apply deletion-aware trie with content removal and deletion consolidation (using InMemoryRangeTrie.apply and InMemoryDTree.delete)
    // TODO: Delete on the way back in apply; clean range trie tests
    // TODO: Singleton/range deletion cursor to use for apply operations
}
