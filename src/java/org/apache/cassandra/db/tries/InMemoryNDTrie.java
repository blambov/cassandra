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

public class InMemoryNDTrie<T extends NonDeterministicTrie.Mergeable<T>> extends InMemoryTrie<T> implements NonDeterministicTrieWithImpl<T>
{
    public InMemoryNDTrie(BufferType bufferType)
    {
        super(bufferType);
    }

    static class NonDeterministicCursor<T extends NonDeterministicTrie.Mergeable<T>>
    extends MemtableCursor<T>
    implements NonDeterministicTrieImpl.Cursor<T>
    {
        NonDeterministicCursor(InMemoryReadTrie<T> trie, Direction direction, int root, int depth, int incomingTransition)
        {
            super(trie, direction, root, depth, incomingTransition);
        }

        NonDeterministicCursor(NonDeterministicCursor<T> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public NonDeterministicCursor<T> alternateBranch()
        {
            return isNull(alternateBranch) ? null
                                           : new NonDeterministicCursor<>(trie,
                                                                          direction,
                                                                          alternateBranch,
                                                                          depth() - 1,
                                                                          incomingTransition());
        }

        @Override
        public T content()
        {
            return content;
        }

        @Override
        public NonDeterministicCursor<T> duplicate()
        {
            return new NonDeterministicCursor<>(this);
        }
    }

    @Override
    public Cursor<T> makeCursor(Direction direction)
    {
        return new NonDeterministicCursor<>(this, direction, root, -1, -1);
    }



    /**
     * Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
     * with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
     * @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
     * different than the element type for this memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value. Applied even if there's no pre-existing value in the memtable trie.
     */
    public <U extends NonDeterministicTrie.Mergeable<U>>
    void apply(NonDeterministicTrie<U> mutation,
               final UpsertTransformer<T, U> transformer)
    throws SpaceExhaustedException
    {
        NonDeterministicTrieImpl.Cursor<U> mutationCursor = NonDeterministicTrieImpl.impl(mutation).cursor(Direction.FORWARD);
        assert mutationCursor.depth() == 0 : "Unexpected non-fresh cursor.";
        ApplyState state = applyState.start();
        assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        apply(state, mutationCursor, transformer);
        assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        // TODO
        state.attachRoot(Integer.MAX_VALUE);
    }

    static <T extends Mergeable<T>, U extends Mergeable<U>>
    void apply(InMemoryTrie<T>.ApplyState state,
               NonDeterministicTrieImpl.Cursor<U> mutationCursor,
               final UpsertTransformer<T, U> transformer)
    throws SpaceExhaustedException
    {
        int prevAscendLimit = state.setAscendLimit(state.currentDepth);
        while (true)
        {
            applyContent(state, mutationCursor, transformer);
            applyAlternate(state, mutationCursor, transformer);
            int depth = mutationCursor.advance();
            // TODO
            if (state.advanceTo(depth, mutationCursor.incomingTransition(), Integer.MAX_VALUE))
                break;
            assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
        }
        state.setAscendLimit(prevAscendLimit);
    }

    static <T extends Mergeable<T>, U extends Mergeable<U>>
    void applyAlternate(InMemoryTrie<T>.ApplyState state,
                        Cursor<U> mutationCursor,
                        UpsertTransformer<T, U> transformer)
    throws SpaceExhaustedException
    {
        Cursor<U> alternate = mutationCursor.alternateBranch();
        if (alternate != null)
        {
            // Note: This will blow if we can have deep chain of alternates of alternates.
            // The latter is not something we need to support.
            state.descendToAlternate();
            apply(state, alternate, transformer);
            // TODO
            state.attachAlternate(false);
        }
    }


    /**
     * Override of dump to provide more detailed printout that includes the type of each node in the trie.
     * We do this via a wrapping cursor that returns a content string for the type of node for every node we return.
     */
    public String dump(Function<T, String> contentToString)
    {
        return dump(contentToString, root);
    }
}
