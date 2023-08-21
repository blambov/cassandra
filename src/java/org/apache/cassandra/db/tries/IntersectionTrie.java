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

import com.carrotsearch.hppc.IntArrayDeque;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntArrayQueue;

public class IntersectionTrie<T> extends Trie<T>
{
    enum SetBoundary
    {
        START,
        END
    }

    final Trie<T> trie;
    final Trie<SetBoundary> set;

    public IntersectionTrie(Trie<T> trie, Trie<SetBoundary> set)
    {
        this.trie = trie;
        this.set = set;
    }

    @Override
    protected Cursor<T> cursor()
    {
        return new IntersectionCursor(trie.cursor(), set.cursor());
    }

    private static class IntersectionCursor<T> implements Cursor<T>, ResettingTransitionsReceiver
    {
        final Cursor<T> source;
        final Cursor<SetBoundary> set;
        final IntArrayDeque toBoundary;
        int queueStartDepth;
        boolean inSet;

        enum State
        {
            ON_PREFIX,
            IN_SET;
        }

        static final int EMPTY_QUEUE = Integer.MAX_VALUE;

        public IntersectionCursor(Cursor<T> source, Cursor<SetBoundary> set)
        {
            this.source = source;
            this.set = set;
            toBoundary = new IntArrayDeque();
            inSet = set.content() == SetBoundary.START;
            queueStartDepth = set.advance();
            toBoundary.addLast(set.incomingTransition());
        }

        @Override
        public int depth()
        {
            return source.depth();
        }

        @Override
        public int incomingTransition()
        {
            return source.incomingTransition();
        }

        @Override
        public T content()
        {
            return inSet ? source.content() : null;
        }

        @Override
        public int advance()
        {
            while (true)
            {
                int depth;
                int transition;
                if (inSet)
                {
                    depth = source.advance();
                    if (depth > queueStartDepth)
                        return depth;
                    transition = source.incomingTransition();
                    int boundaryFirst = toBoundary.getFirst();
                    if (depth == queueStartDepth && boundaryFirst >= transition)
                    {
                        if (boundaryFirst == transition)
                        {
                            toBoundary.removeFirst();
                            ++queueStartDepth;
                            if (toBoundary.isEmpty())
                            {
                                SetBoundary b = set.content();
                                queueStartDepth = set.advance();
                                toBoundary.addLast(set.incomingTransition());
                                if (b != null)
                                {
                                    assert b == SetBoundary.END;
                                    inSet = false;
                                    continue;
                                }
                            }
                        }
                        return depth;
                    }
                    // Went out of the set.
                }
                else
                {
                    int boundaryFirst = toBoundary.removeFirst();
                    depth = source.skipTo(queueStartDepth, boundaryFirst);
                    transition = source.incomingTransition();
                    if (depth == queueStartDepth && transition == boundaryFirst)
                    {
                        // Still following boundary.
                        if (toBoundary.isEmpty())
                        {
                            SetBoundary b = set.content();
                            queueStartDepth = set.advance();
                            toBoundary.addLast(set.incomingTransition());
                            if (b != null)
                            {
                                assert b == SetBoundary.START;
                                inSet = true;
                            }
                        }
                        else
                            ++queueStartDepth;
                        return depth;
                    }
                    // Otherwise we have gone over the boundary, but we may also have gone over the end one.
                }
                // Find next set boundary one and check if it's an END
                if (advanceToNextSetBoundary(depth, transition))
                    return depth;
            }
        }

        private boolean advanceToNextSetBoundary(int depth, int transition)
        {
            toBoundary.clear();
            queueStartDepth = set.skipTo(depth, transition);
            toBoundary.addLast(set.incomingTransition());
            SetBoundary b = set.advanceToContent(this);
            return inSet = (b == SetBoundary.END);
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            int depth;
            int transition;
            if (inSet)
            {
                depth = source.skipTo(skipDepth, skipTransition);
                if (depth > queueStartDepth)
                    return depth;
                transition = source.incomingTransition();
                int boundaryFirst = toBoundary.getFirst();
                if (depth == queueStartDepth && boundaryFirst >= transition)
                {
                    if (boundaryFirst == transition)
                    {
                        toBoundary.removeFirst();
                        ++queueStartDepth;
                        if (toBoundary.isEmpty())
                        {
                            SetBoundary b = set.content();
                            queueStartDepth = set.advance();
                            toBoundary.addLast(set.incomingTransition());
                            if (b != null)
                            {
                                assert b == SetBoundary.END;
                                inSet = false;
                                return advance();
                            }
                        }
                    }
                    return depth;
                }
                // Went out of the set.
            }
            else
            {
                int boundaryFirst = toBoundary.removeFirst();
                if (queueStartDepth < skipDepth || skipTransition < boundaryFirst)
                {
                    skipDepth = queueStartDepth;
                    skipTransition = boundaryFirst; // set boundary is beyond skip position, skip to that instead
                }
                depth = source.skipTo(skipDepth, skipTransition);
                transition = source.incomingTransition();
                if (depth == queueStartDepth && transition == boundaryFirst)
                {
                    // Still following boundary.
                    if (toBoundary.isEmpty())
                    {
                        SetBoundary b = set.content();
                        queueStartDepth = set.advance();
                        toBoundary.addLast(set.incomingTransition());
                        if (b != null)
                        {
                            assert b == SetBoundary.START;
                            inSet = true;
                        }
                    }
                    else
                        ++queueStartDepth;
                    return depth;
                }
                // Otherwise we have gone over the boundary, but we may also have gone over the end one.
            }
            // Find next set boundary one and check if it's an END
            if (advanceToNextSetBoundary(depth, transition))
                return depth;
            else
                return advance();
        }


        @Override
        public void addPathByte(int nextByte)
        {
            toBoundary.addLast(nextByte);
        }

        @Override
        public void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            for (int i = 0; i < count; ++i)
                addPathByte(buffer.getByte(pos + i) & 0xFF);
        }

        @Override
        public void resetPathLength(int newLength)
        {
            if (newLength <= queueStartDepth)
            {
                queueStartDepth = newLength - 1;
                toBoundary.clear();
            }
            else
            {
                while (newLength > queueStartDepth + toBoundary.size() - 1)
                    toBoundary.removeLast();
            }
        }
    }
}
