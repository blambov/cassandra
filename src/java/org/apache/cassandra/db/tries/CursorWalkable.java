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

interface CursorWalkable<C extends CursorWalkable.Cursor>
{
    C cursor();

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
         * Advance to the specified depth and incoming transition or the first valid position that is after the specified
         * position. The inputs must be something that could be returned by a single call to {@link #advance} (i.e.
         * {@code skipDepth} must be <= current depth + 1, and {@code skipTransition} must be higher than what the
         * current state saw at the requested depth. The skip position must be after the current position.
         *
         * @return the new depth, always <= previous depth + 1; -1 if the trie is exhausted
         */
        int skipTo(int skipDepth, int skipTransition);

        /**
         * A version of skipTo which checks if the requested position is ahead of the cursor's current position and only
         * advances if it is.
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
