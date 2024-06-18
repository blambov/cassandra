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

import java.util.Collection;

interface TrieImpl<T> extends CursorWalkable<TrieImpl.Cursor<T>>
{
    /**
     * A trie cursor.
     * <p>
     * This is the internal representation of the trie, which enables efficient walks and basic operations (merge,
     * slice) on tries.
     * <p>
     * The cursor represents the state of a walk over the nodes of trie. It provides three main features:<ul>
     * <li>the current {@code depth} or descend-depth in the trie;</li>
     * <li>the {@code incomingTransition}, i.e. the byte that was used to reach the current point;</li>
     * <li>the {@code content} associated with the current node,</li>
     * </ul>
     * and provides methods for advancing to the next position.  This is enough information to extract all paths, and
     * also to easily compare cursors over different tries that are advanced together. Advancing is always done in
     * order; if one imagines the set of nodes in the trie with their associated paths, a cursor may only advance from a
     * node with a lexicographically smaller path to one with bigger. The {@code advance} operation moves to the immediate
     * next, it is also possible to skip over some items e.g. all children of the current node ({@code skipChildren}).
     * <p>
     * Moving to the immediate next position in the lexicographic order is accomplished by:<ul>
     * <li>if the current node has children, moving to its first child;</li>
     * <li>otherwise, ascend the parent chain and return the next child of the closest parent that still has any.</li>
     * </ul>
     * As long as the trie is not exhausted, advancing always takes one step down, from the current node, or from a node
     * on the parent chain. By comparing the new depth (which {@code advance} also returns) with the one before the advance,
     * one can tell if the former was the case (if {@code newDepth == oldDepth + 1}) and how many steps up we had to take
     * ({@code oldDepth + 1 - newDepth}). When following a path down, the cursor will stop on all prefixes.
     * <p>
     * When it is created the cursor is placed on the root node with {@code depth() = 0}, {@code incomingTransition() = -1}.
     * Since tries can have mappings for empty, content() can possibly be non-null. It is not allowed for a cursor to start
     * in exhausted state (i.e. with {@code depth() = -1}).
     * <p>
     * For example, the following trie:<br/>
     * <pre>
     *  t
     *   r
     *    e
     *     e *
     *    i
     *     e *
     *     p *
     *  w
     *   i
     *    n  *
     * </pre>
     * has nodes reachable with the paths<br/>
     * &nbsp; "", t, tr, tre, tree*, tri, trie*, trip*, w, wi, win*<br/>
     * and the cursor will list them with the following {@code (depth, incomingTransition)} pairs:<br/>
     * &nbsp; (0, -1), (1, t), (2, r), (3, e), (4, e)*, (3, i), (4, e)*, (4, p)*, (1, w), (2, i), (3, n)*
     * <p>
     * Because we exhaust transitions on bigger depths before we go the next transition on the smaller ones, when
     * cursors are advanced together their positions can be easily compared using only the {@code depth} and
     * {@code incomingTransition}:<ul>
     * <li>one that is higher in depth is before one that is lower;</li>
     * <li>for equal depths, the one with smaller incomingTransition is first.</li>
     * </ul>
     * If we consider walking the trie above in parallel with this:<br/>
     * <pre>
     *  t
     *   r
     *    i
     *     c
     *      k *
     *  u
     *   p *
     * </pre>
     * the combined iteration will proceed as follows:<pre>
     *  (0, -1)+  (0, -1)+          cursors equal, advance both
     *  (1, t)+   (1, t)+   t       cursors equal, advance both
     *  (2, r)+   (2, r)+   tr      cursors equal, advance both
     *  (3, e)+ < (3, i)    tre     cursors not equal, advance smaller (3 = 3, e < i)
     *  (4, e)+ < (3, i)    tree*   cursors not equal, advance smaller (4 > 3)
     *  (3, i)+   (3, i)+   tri     cursors equal, advance both
     *  (4, e)  > (4, c)+   tric    cursors not equal, advance smaller (4 = 4, e > c)
     *  (4, e)  > (5, k)+   trick*  cursors not equal, advance smaller (4 < 5)
     *  (4, e)+ < (1, u)    trie*   cursors not equal, advance smaller (4 > 1)
     *  (4, p)+ < (1, u)    trip*   cursors not equal, advance smaller (4 > 1)
     *  (1, w)  > (1, u)+   u       cursors not equal, advance smaller (1 = 1, w > u)
     *  (1, w)  > (2, p)+   up*     cursors not equal, advance smaller (1 < 2)
     *  (1, w)+ < (-1, -1)  w       cursors not equal, advance smaller (1 > -1)
     *  (2, i)+ < (-1, -1)  wi      cursors not equal, advance smaller (2 > -1)
     *  (3, n)+ < (-1, -1)  win*    cursors not equal, advance smaller (3 > -1)
     *  (-1, -1)  (-1, -1)          both exhasted
     *  </pre>
     */
    interface Cursor<T> extends CursorWalkable.Cursor
    {
        /**
         * @return the content associated with the current node. This may be non-null for any presented node, including
         * the root.
         */
        T content();

        /**
         * Advance all the way to the next node with non-null content.
         * <p>
         * It is an error to call this after the trie has already been exhausted (i.e. when depth() == -1);
         * for performance reasons we won't always check this.
         *
         * @param receiver object that will receive all taken transitions
         * @return the content, null if the trie is exhausted
         */
        default T advanceToContent(ResettingTransitionsReceiver receiver)
        {
            int prevDepth = depth();
            while (true)
            {
                int currDepth = advanceMultiple(receiver);
                if (currDepth <= 0)
                    return null;
                if (receiver != null)
                {
                    if (currDepth <= prevDepth)
                        receiver.resetPathLength(currDepth - 1);
                    receiver.addPathByte(incomingTransition());
                }
                T content = content();
                if (content != null)
                    return content;
                prevDepth = currDepth;
            }
        }

        /**
         * Make a copy of this cursor which can be separately advanced/queried from the current state.
         */
        @Override
        Cursor<T> duplicate();

        @Override
        default Cursor<T> tailCursor(Direction direction)
        {
            throw new AssertionError("unimplemented");
        }
    }

    /**
     * Used by {@link Cursor#advanceToContent} to track the transitions and backtracking taken.
     */
    interface ResettingTransitionsReceiver extends TransitionsReceiver
    {
        /**
         * Delete all bytes beyond the given length.
         */
        void resetPathLength(int newLength);
    }

    /**
     * A push interface for walking over the trie. Builds upon TransitionsReceiver to be given the bytes of the
     * path, and adds methods called on encountering content and completion.
     * See {@link TrieDumper} for an example of how this can be used, and {@link TrieEntriesWalker} as a base class
     * for other common usages.
     */
    interface Walker<T, R> extends ResettingTransitionsReceiver
    {
        /** Called when content is found. */
        void content(T content);

        /** Called at the completion of the walk. */
        R complete();
    }

    /**
     * Process the trie using the given Walker.
     */
    default <R> R process(Walker<T, R> walker, Direction direction)
    {
        return process(walker, cursor(direction));
    }

    static <T, R> R process(Walker<T, R> walker, Cursor<T> cursor)
    {
        assert cursor.depth() == 0 : "The provided cursor has already been advanced.";
        T content = cursor.content();   // handle content on the root node
        if (content == null)
            content = cursor.advanceToContent(walker);

        while (content != null)
        {
            walker.content(content);
            content = cursor.advanceToContent(walker);
        }
        return walker.complete();
    }

    Trie.CollectionMergeResolver<Object> THROWING_RESOLVER = new Trie.CollectionMergeResolver<Object>()
    {
        @Override
        public Object resolve(Collection<Object> contents)
        {
            throw error();
        }

        private AssertionError error()
        {
            throw new AssertionError("Entries must be distinct.");
        }
    };

    class EmptyCursor<T> implements Cursor<T>
    {
        int depth = 0;

        public int advance()
        {
            return depth = -1;
        }

        public int skipTo(int skipDepth, int skipTransition)
        {
            return depth = -1;
        }

        public int depth()
        {
            return depth;
        }

        public T content()
        {
            return null;
        }

        public int incomingTransition()
        {
            return -1;
        }

        public Cursor<T> duplicate()
        {
            return depth == 0 ? new EmptyCursor<>() : this;
        }

        public Cursor<T> tailCursor(Direction direction)
        {
            return new EmptyCursor<>();
        }
    }

    TrieWithImpl<Object> EMPTY = dir -> new EmptyCursor<Object>();

    static <T> TrieWithImpl<T> impl(Trie<T> trie)
    {
        return (TrieWithImpl<T>) trie;
    }
}
