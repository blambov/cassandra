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

import org.agrona.DirectBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

interface CursorWalkable<C extends CursorWalkable.Cursor>
{
    /**
     * A trie cursor.
     *
     * This is the internal representation of the trie, which enables efficient walks and basic operations (merge,
     * slice) on tries.
     *
     * The cursor represents the state of a walk over the nodes of trie. It provides three main features:
     * - the current "depth" or descend-depth in the trie;
     * - the "incomingTransition", i.e. the byte that was used to reach the current point;
     * - the "content" associated with the current node,
     * and provides methods for advancing to the next position.  This is enough information to extract all paths, and
     * also to easily compare cursors over different tries that are advanced together. Advancing is always done in
     * order; if one imagines the set of nodes in the trie with their associated paths, a cursor may only advance from a
     * node with a lexicographically smaller path to one with bigger. The "advance" operation moves to the immediate
     * next, it is also possible to skip over some items e.g. all children of the current node ("skipChildren").
     *
     * Moving to the immediate next position in the lexicographic order is accomplished by:
     * - if the current node has children, moving to its first child;
     * - otherwise, ascend the parent chain and return the next child of the closest parent that still has any.
     * As long as the trie is not exhausted, advancing always takes one step down, from the current node, or from a node
     * on the parent chain. By comparing the new depth (which "advance" also returns) with the one before the advance,
     * one can tell if the former was the case (if newDepth == oldDepth + 1) and how many steps up we had to take
     * (oldDepth + 1 - newDepth). When following a path down, the cursor will stop on all prefixes.
     *
     * When it is created the cursor is placed on the root node with depth() = 0, incomingTransition() = -1. Since
     * tries can have mappings for empty, content() can possibly be non-null. It is not allowed for a cursor to start
     * in exhausted state (i.e. with depth() = -1).
     *
     * For example, the following trie:
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
     * has nodes reachable with the paths
     *  "", t, tr, tre, tree*, tri, trie*, trip*, w, wi, win*
     * and the cursor will list them with the following (depth, incomingTransition) pairs:
     *  (0, -1), (1, t), (2, r), (3, e), (4, e)*, (3, i), (4, e)*, (4, p)*, (1, w), (2, i), (3, n)*
     *
     * Because we exhaust transitions on bigger depths before we go the next transition on the smaller ones, when
     * cursors are advanced together their positions can be easily compared using only the depth and incomingTransition:
     * - one that is higher in depth is before one that is lower;
     * - for equal depths, the one with smaller incomingTransition is first.
     *
     * If we consider walking the trie above in parallel with this:
     *  t
     *   r
     *    i
     *     c
     *      k *
     *  u
     *   p *
     * the combined iteration will proceed as follows:
     *  (0, -1)+    (0, -1)+               cursors equal, advance both
     *  (1, t)+     (1, t)+        t       cursors equal, advance both
     *  (2, r)+     (2, r)+        tr      cursors equal, advance both
     *  (3, e)+  <  (3, i)         tre     cursors not equal, advance smaller (3 = 3, e < i)
     *  (4, e)+  <  (3, i)         tree*   cursors not equal, advance smaller (4 > 3)
     *  (3, i)+     (3, i)+        tri     cursors equal, advance both
     *  (4, e)   >  (4, c)+        tric    cursors not equal, advance smaller (4 = 4, e > c)
     *  (4, e)   >  (5, k)+        trick*  cursors not equal, advance smaller (4 < 5)
     *  (4, e)+  <  (1, u)         trie*   cursors not equal, advance smaller (4 > 1)
     *  (4, p)+  <  (1, u)         trip*   cursors not equal, advance smaller (4 > 1)
     *  (1, w)   >  (1, u)         u       cursors not equal, advance smaller (1 = 1, w > u)
     *  (1, w)   >  (2, p)         up*     cursors not equal, advance smaller (1 = 1, w > u)
     *  (1, w)+  <  (-1, -1)       w       cursors not equal, advance smaller (1 > -1)
     *  (2, i)+  <  (-1, -1)       wi      cursors not equal, advance smaller (2 > -1)
     *  (3, n)+  <  (-1, -1)       win*    cursors not equal, advance smaller (3 > -1)
     *  (-1, -1)    (-1, -1)               both exhasted
     *
     * Cursors are created with a direction (forward or reverse), which specifies the order in which a node's children
     * are iterated (smaller first or larger first). Note that entries returned in reverse direction are in
     * lexicographic order for the inverted alphabet, which is not the same as being presented in reverse. For example,
     * a cursor for a trie containing "ab", "abc" and "cba", will visit the nodes in order "cba", "ab", "abc", i.e.
     * prefixes will still be reported before their descendants.
     */
    C cursor(Direction direction);

    interface Cursor
    {
        /**
         * @return the current descend-depth; 0, if the cursor has just been created and is positioned on the root,
         *         and -1, if the trie has been exhausted.
         */
        int depth();

        /**
         * @return the last transition taken; if positioned on the root, return -1
         */
        int incomingTransition();

        /**
         * Advance one position to the node whose associated path is next lexicographically.
         * This can be either:<ul>
         * <li>descending one level to the first child of the current node,
         * <li>ascending to the closest parent that has remaining children, and then descending one level to its next
         *   child.
         * </ul>
         * It is an error to call this after the trie has already been exhausted (i.e. when depth() == -1);
         * for performance reasons we won't always check this.
         *
         * @return depth (can be prev+1 or <=prev), -1 means that the trie is exhausted
         */
        int advance();

        /**
         * Advance, descending multiple levels if the cursor can do this for the current position without extra work
         * (e.g. when positioned on a chain node in a memtable trie). If the current node does not have children this
         * is exactly the same as advance(), otherwise it may take multiple steps down (but will not necessarily, even
         * if they exist).
         * <p>
         * Note that if any positions are passed over, their content or any other feature must be null.
         * <p>
         * This is an optional optimization; the default implementation falls back to calling advance.
         * <p>
         * It is an error to call this after the trie has already been exhausted (i.e. when depth() == -1);
         * for performance reasons we won't always check this.
         *
         * @param receiver object that will receive all transitions taken except the last;
         *                 on ascend, or if only one step down was taken, it will not receive any
         * @return the new depth, -1 if the trie is exhausted
         */
        default int advanceMultiple(TransitionsReceiver receiver)
        {
            return advance();
        }

        /**
         * Advance to the specified depth and incoming transition or the first valid position that is at or after the
         * specified position. The inputs must be something that could be returned by a single call to {@link #advance}
         * (i.e. {@code skipDepth} must be <= current depth + 1, and {@code skipTransition} must be higher than what the
         * current state saw at the requested depth. The skip position must be after the current position.
         * <p>
         * Note that it is permitted to request a skip transition outside the normal range, usually 256 (forward) or
         * -1 (reverse) to request ascent from that depth.
         *
         * @return the new depth, always <= previous depth + 1; -1 if the trie is exhausted
         */
        int skipTo(int skipDepth, int skipTransition);

        /**
         * A version of skipTo which checks if the requested position is ahead of the cursor's current position and only
         * advances if it is. This can only be used if the skipTo instruction was issued for a position that was behind
         * this cursor's (i.e. if the skipTo request is to descend, it is assumed to descend from a position _before_
         * this cursor's and will not be acted on).
         */
        default int maybeSkipTo(int skipDepth, int skipTransition)
        {
            int depth = depth();
            if (skipDepth < depth || skipDepth == depth && skipTransition > incomingTransition())
                return skipTo(skipDepth, skipTransition);
            else
                return depth;
        }


        /**
         * Descend into the cursor with the given path.
         *
         * @return True if the descent is positioned at the end of the given path, false if the trie did not have a path
         * for it.
         */
        default boolean descendAlong(ByteSource bytes)
        {
            int next = bytes.next();
            int depth = depth();
            while (next != ByteSource.END_OF_STREAM)
            {
                if (skipTo(++depth, next) != depth || incomingTransition() != next)
                    return false;
                next = bytes.next();
            }
            return true;
        }

        /**
         * Make a copy of this cursor which can be separately advanced/queried from the current state.
         * Note that some code relies on the fact that the duplicate will be of the same type as the original.
         */
        Cursor duplicate();
    }

    /**
     * Used by {@link TrieImpl.Cursor#advanceMultiple} to feed the transitions taken.
     */
    interface TransitionsReceiver
    {
        /** Add a single byte to the path. */
        void addPathByte(int nextByte);
        /** Add the count bytes from position pos in the given buffer. */
        void addPathBytes(DirectBuffer buffer, int pos, int count);
    }

    // Version of the byte comparable conversion to use for all operations
    ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS50;
}
