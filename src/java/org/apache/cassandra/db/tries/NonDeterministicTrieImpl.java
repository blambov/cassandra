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

interface NonDeterministicTrieImpl<T extends NonDeterministicTrie.Mergeable<T>> extends CursorWalkable<NonDeterministicTrieImpl.Cursor<T>>
{
    interface Cursor<T extends NonDeterministicTrie.Mergeable<T>> extends TrieImpl.Cursor<T>
    {
        /**
         * If this node has an alternate branch, returns a Cursor that walks over it.
         * The constructed cursor will only list an alternate branch rooted at this node, but it will retain the depth
         * to make position comparisons easier.
         * <p>
         * Alternate branches are mechanisms for defining an epsilon (i.e. zero-length) transition to another point in
         * the trie. Theoretically the most useful application for this is to permit non-deterministic variations of
         * automata, including suffix tries. What is most important for us, however, is that this allows one to have
         * sections of the trie that are separately queriable. More specifically, we can use an alternate branch to
         * store deletions; such a branch would not affect normal iteration over the trie, and also enables one to seek
         * into each of the two branches independently: to find active range deletion for a given boundary, which
         * would be the closest deletion but may be millions of live data entries away; and similarly to find the first
         * live item after a boundary in a stream of millions of deletions.
         * <p>
         * Most operation implementations only touch alternate branches if they are explicitly asked to. Walks, entry
         * and value sets will not see any content in alternate branches. Merges will merge and present normal and
         * alternate branches separately and intersection will use the same intersecting set for both normal and
         * alternate paths (ignoring any alternate part in the intersecting set).
         * <p>
         * To perform a walk or enumeration that includes alternate branches, one must explicitly construct a merged
         * (in other words, deterministic) trie view by calling {@link NonDeterministicTrie#deterministic}.
         */
        Cursor<T> alternateBranch();

        /**
         * Make a copy of this cursor which can be separately advanced/queried from the current state.
         */
        Cursor<T> duplicate();
    }

    /**
     * Process the trie using the given Walker.
     */
    default <R> R process(TrieImpl.Walker<T, R> walker, Direction direction)
    {
        return TrieImpl.process(walker, alternativesMergingCursor(direction));
    }

    default TrieImpl.Cursor<T> alternativesMergingCursor(Direction direction)
    {
        return new MergeAlternativeBranchesTrie.MergeAlternativesCursor<>(direction, cursor(direction), false);
    }

    class EmptyCursor<T extends NonDeterministicTrie.Mergeable<T>> extends TrieImpl.EmptyCursor<T> implements Cursor<T>
    {
        @Override
        public Cursor<T> alternateBranch()
        {
            return null;
        }

        @Override
        public Cursor<T> duplicate()
        {
            return depth == 0 ? new EmptyCursor<>() : this;
        }
    }

    @SuppressWarnings("rawtypes")
    NonDeterministicTrieWithImpl EMPTY = dir -> new EmptyCursor();

    static <T extends NonDeterministicTrie.Mergeable<T>> NonDeterministicTrieWithImpl<T> impl(NonDeterministicTrie<T> trie)
    {
        return (NonDeterministicTrieWithImpl<T>) trie;
    }
}
