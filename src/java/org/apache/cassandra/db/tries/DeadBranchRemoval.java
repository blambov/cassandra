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
import org.agrona.concurrent.UnsafeBuffer;

public class DeadBranchRemoval<T> implements TrieWithImpl<T>
{
    private final TrieImpl<T> source;

    private DeadBranchRemoval(TrieImpl<T> source)
    {
        this.source = source;
    }

    @Override
    public Cursor<T> makeCursor(Direction direction)
    {
        return apply(direction, source.cursor(direction));
    }

    public static <T> Cursor<T> apply(Direction direction, Cursor<T> source)
    {
        return new DeadBranchRemovalCursor<>(direction, source);
    }

    private static class DeadBranchRemovalCursor<T> implements Cursor<T>, TransitionsReceiver
    {
        private final Direction direction;
        private final Cursor<T> source;
        final UnsafeBuffer buffer;
        int buffered;
        int consumed;
        int incomingTransition;
        int depth;

        DeadBranchRemovalCursor(Direction direction, Cursor<T> source)
        {
            this.direction = direction;
            this.source = source;
            this.buffer = new UnsafeBuffer(new byte[16]);
            this.incomingTransition = source.incomingTransition();
            this.depth = source.depth();
        }

        @Override
        public int depth()
        {
            return depth;
        }

        @Override
        public int incomingTransition()
        {
            return incomingTransition;
        }

        @Override
        public T content()
        {
            if (buffered > 0)
                return null;
            return source.content();
        }

        private int exhausted()
        {
            incomingTransition = -1;
            depth = -1;
            return depth;
        }

        @Override
        public int advance()
        {
            if (buffered == 0)
                if (!findData())
                    return exhausted();

            incomingTransition = consume();
            return ++depth;
        }

        private int consume()
        {
            int t = buffer.getByte(consumed++) & 0xFF;
            if (consumed == buffered)
                buffered = 0;
            return t;
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            if (buffered == 0)
                return advance();

            receiver.addPathBytes(buffer, consumed, buffered - consumed - 1);

            buffered = 0;
            incomingTransition = source.incomingTransition();
            return depth = source.depth();
        }

        private boolean findData()
        {
            consumed = 0;
            int prevDepth = depth;
            while (true)
            {
                int currDepth = source.advanceMultiple(this);
                if (currDepth <= 0)
                    return false;

                if (currDepth <= prevDepth)
                {
                    if (currDepth <= depth)
                    {
                        depth = currDepth - 1;
                        buffered = 0;
                    }
                    else
                        buffered = currDepth - depth - 1;
                }
                addPathByte(source.incomingTransition());

                if (hasContent())
                    return true;
                prevDepth = currDepth;
            }
        }

        public boolean hasContent()
        {
            return source.content() != null
                // TODO: || source.alternateBranch() != null
            ;
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            if (buffered > 0 && skipDepth > depth)
            {
                assert skipDepth == depth + 1;
                int next = consume();
                if (direction.ge(next, skipTransition))
                {
                    incomingTransition = next;
                    return ++depth;
                }
            }
            depth = source.skipTo(skipDepth, skipTransition);
            if (depth == -1)
                return exhausted();
            buffered = 0;
            --depth;
            addPathByte(source.incomingTransition());
            if (!findData())
                return exhausted();
            incomingTransition = consume();
            return ++depth;
        }
//
//        @Override
//        public T advanceToContent(ResettingTransitionsReceiver receiver)
//        {
//            return source.advanceToContent(receiver);
//        }

//        @Override
//        public Cursor<T> alternateBranch()
//        {
//            if (buffered > 0)
//                return null;
//            return source.alternateBranch();
//        }

        @Override
        public Cursor<T> duplicate()
        {
            return apply(direction, source.duplicate());
        }

        @Override
        public void addPathByte(int nextByte)
        {
            ensureRoom(buffer, buffered + 1);
            buffer.putByte(buffered++, (byte) nextByte);
        }

        @Override
        public void addPathBytes(DirectBuffer data, int pos, int count)
        {
            ensureRoom(buffer, buffered + count);
            buffer.putBytes(buffered, data, pos, count);
            buffered += count;
        }

        private static void ensureRoom(DirectBuffer buffer, int newLength)
        {
            if (newLength > buffer.capacity())
            {
                byte[] newBuffer = new byte[Integer.highestOneBit(newLength - 1) * 2];  // exact length match is okay
                buffer.getBytes(0, newBuffer, 0, buffer.capacity());
                buffer.wrap(newBuffer);
            }
        }
    }
}
