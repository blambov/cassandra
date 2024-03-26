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

public class InMemoryRangeTrie<M extends RangeTrie.RangeMarker<M>> extends InMemoryTrie<M> implements RangeTrieWithImpl<M>
{
    public InMemoryRangeTrie(BufferType bufferType)
    {
        super(bufferType);
    }

    @Override
    public Cursor<M> makeCursor()
    {
        return new RangeCursor<>(this, root, -1, -1);
    }


    /**
     *
     * @param <M>
     * @param <Q> Type of the underlying trie. Made generic to accommodate deletion-aware tries where the in-memory
     *            trie is not of the same type as the deletion branches.
     */
    static class RangeCursor<M extends RangeTrie.RangeMarker<M>, Q> extends MemtableCursor<Q> implements RangeTrieImpl.Cursor<M>
    {
        boolean activeIsSet;
        M activeRange;  // only non-null if activeIsSet
        M prevContent;  // can only be non-null if activeIsSet

        RangeCursor(InMemoryReadTrie<Q> trie, int root, int depth, int incomingTransition)
        {
            super(trie, root, depth, incomingTransition);
            activeIsSet = true;
            activeRange = null;
            prevContent = null;
        }

        RangeCursor(RangeCursor<M, Q> copyFrom)
        {
            super(copyFrom);
            this.activeRange = copyFrom.activeRange;
            this.activeIsSet = copyFrom.activeIsSet;
            this.prevContent = copyFrom.prevContent;
        }

        @Override
        public M content()
        {
            return (M) content;
        }

        @Override
        public int advance()
        {
            return updateActiveAndReturn(super.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            return updateActiveAndReturn(super.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            activeIsSet = false;    // since we are skipping, we have no idea where we will end up
            activeRange = null;
            prevContent = null;
            return updateActiveAndReturn(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public M coveringState()
        {
            if (!activeIsSet)
                setActiveState();
            return activeRange;
        }

        private int updateActiveAndReturn(int depth)
        {
            if (depth < 0)
            {
                activeIsSet = true;
                activeRange = null;
                prevContent = null;
                return depth;
            }

            // Always check if we are seeing new content; if we do, that's an easy state update.
            M content = content();
            if (content != null)
            {
                activeRange = content.leftSideAsCovering();
                prevContent = content;
                activeIsSet = true;
            }
            else if (prevContent != null)
            {
                // If the previous state was exact, its right side is what we now have.
                activeRange = prevContent.rightSideAsCovering();
                prevContent = null;
                assert activeIsSet;
            }
            // otherwise the active state is either not set or still valid.
            return depth;
        }

        private void setActiveState()
        {
            assert content() == null;
            M nearestContent = (M) duplicate().advanceToContent(null);
            activeRange = nearestContent != null ? nearestContent.leftSideAsCovering() : null;
            prevContent = null;
            activeIsSet = true;
        }

        @Override
        public RangeCursor<M, Q> duplicate()
        {
            return new RangeCursor<M, Q>(this);
        }
    }


    /**
     * Override of dump to provide more detailed printout that includes the type of each node in the trie.
     * We do this via a wrapping cursor that returns a content string for the type of node for every node we return.
     */
    public String dump(Function<M, String> contentToString)
    {
        return dump(contentToString, root);
    }
}
