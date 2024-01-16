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

public class AlternateViewTrie<T> extends Trie<T>
{
    final Trie<T> wrapped;

    public AlternateViewTrie(Trie<T> wrapped)
    {
        this.wrapped = wrapped;
    }

    @Override
    protected Cursor<T> cursor()
    {
        return new AlterateViewCursor<>(wrapped.cursor());
    }

    private static class AlterateViewCursor<T> implements Cursor<T>
    {
        final Cursor<T> wrapped;
        Cursor<T> alternate;

        public AlterateViewCursor(Cursor<T> wrapped)
        {
            this.wrapped = wrapped;
            enterWrappedNode(wrapped.depth());
        }

        @Override
        public int depth()
        {
            return alternate != null ? alternate.depth() : wrapped.depth();
        }

        @Override
        public int incomingTransition()
        {
            return alternate != null ? alternate.incomingTransition() : wrapped.incomingTransition();
        }

        @Override
        public T content()
        {
            return alternate != null ? alternate.content() : null;
        }

        @Override
        public int advance()
        {
            if (alternate != null)
            {
                int depth = alternate.advance();
                if (depth >= 0)
                    return depth;
                // Alternative exhausted, advance main path without descending into anything covered by the alternate
                // branch.
                alternate = null;
                return enterWrappedNode(wrapped.skipTo(wrapped.depth(), wrapped.incomingTransition() + 1));
            }
            return enterWrappedNode(wrapped.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            if (alternate == null)
                return enterWrappedNode(wrapped.advanceMultiple(receiver));
            if (alternate.depth() > wrapped.depth() || alternate.incomingTransition() < wrapped.incomingTransition())
            {
                int depth = alternate.advanceMultiple(receiver);
                if (depth >= 0)
                    return depth;
                // Note: the call above should not have put anything in the receiver (it ascended to the end marker)
                // Alternative exhausted, advance main path without descending into anything covered by the alternate
                // branch.
                alternate = null;
                return enterWrappedNode(wrapped.skipTo(wrapped.depth(), wrapped.incomingTransition() + 1));
            }
            // Otherwise we have both, and they are positioned on the same node. Can't descend multiple.
            return advance();
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            if (alternate != null)
            {
                int depth = alternate.skipTo(skipDepth, skipTransition);
                if (depth >= 0)
                    return depth;
                // Alternative exhausted, fall through to advancing main path, but make sure to skip beyond the root of
                // the alternate branch.
                alternate = null;
                final int wrappedDepth = wrapped.depth();
                final int wrappedTransition = wrapped.incomingTransition() + 1;
                if (skipDepth > wrappedDepth || skipDepth == wrappedDepth && skipTransition < wrappedTransition)
                {
                    skipDepth = wrappedDepth;
                    skipTransition = wrappedTransition;
                }
            }
            return enterWrappedNode(wrapped.skipTo(skipDepth, skipTransition));
        }

        @Override
        public Cursor<T> duplicate()
        {
            return null;
        }

        private int enterWrappedNode(int depth)
        {
            if (depth < 0)
                return depth;
            Cursor<T> alt = wrapped.alternateBranch();
            if (alt != null)
            {
                assert alternate == null;
                alternate = alt;
            }
            return depth;
        }
    }
}
