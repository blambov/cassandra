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

import org.apache.cassandra.io.compress.BufferType;

public class InMemoryRangeTrie<M extends RangeTrieImpl.RangeMarker<M>> extends InMemoryTrie<M> implements RangeTrieWithImpl<M>
{
    public InMemoryRangeTrie(BufferType bufferType)
    {
        super(bufferType);
    }

    @Override
    public Cursor<M> cursor()
    {
        return new RangeCursor(root, -1, -1);
    }


    private class RangeCursor extends MemtableCursor implements RangeTrieImpl.Cursor<M>
    {
        M activeRange;
        boolean activeIsSet = false;
        boolean activeIsExact = false;

        RangeCursor(int root, int depth, int incomingTransition)
        {
            super(root, depth, incomingTransition);
            activeRange = null;
        }

        RangeCursor(RangeCursor copyFrom)
        {
            super(copyFrom);
            this.activeRange = copyFrom.activeRange;
            this.activeIsSet = copyFrom.activeIsSet;
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
            activeIsSet = activeIsExact = false;    // since we are skipping, we have no idea where we will end up
            return updateActiveAndReturn(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public M state()
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
                activeIsExact = false;
                return depth;
            }

            // Always check if we are seeing new content; if we do, that's an easy state update.
            M content = content();
            if (content != null)
            {
                activeRange = content;
                activeIsExact = true;
                activeIsSet = true;
            }
            else if (activeIsExact)
            {
                // If the previous state was exact, its right side is what we now have.
                activeRange = activeRange.rightSideAsActive();
                activeIsExact = false;
                assert activeIsSet;
            }
            // otherwise the active state is either not set or still valid.
            return depth;
        }

        private void setActiveState()
        {
            M content = duplicate().advanceToContent(null);
            if (content != null)
                content = content.leftSideAsActive();
            activeIsExact = false;
            activeIsSet = true;
            activeRange = content;
        }

        @Override
        public RangeCursor duplicate()
        {
            return new RangeCursor(this);
        }
    }
}
