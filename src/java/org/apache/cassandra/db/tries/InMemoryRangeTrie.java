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
        M activeRange = null;
        boolean activeSet = false;

        RangeCursor(int root, int depth, int incomingTransition)
        {
            super(root, depth, incomingTransition);
            activeRange = null;
        }

        RangeCursor(RangeCursor copyFrom)
        {
            super(copyFrom);
            this.activeRange = copyFrom.activeRange;
            this.activeSet = copyFrom.activeSet;
        }

        private int resetActiveAndReturn(int depth)
        {
            activeSet = false;
            activeRange = null;
            return depth;
        }

        @Override
        public int advance()
        {
            return resetActiveAndReturn(super.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            return resetActiveAndReturn(super.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return resetActiveAndReturn(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public M state()
        {
            if (!activeSet)
            {
                activeSet = true;
                activeRange = getActiveState();
            }
            return activeRange;
        }

        private M getActiveState()
        {
            if (depth() < 0)
                return null;
            M content = content();
            if (content == null)
            {
                content = duplicate().advanceToContent(null);
                if (content != null)
                    content = content.asActiveState();
            }
            return content;
        }

        @Override
        public RangeCursor duplicate()
        {
            return new RangeCursor(this);
        }
    }
}
