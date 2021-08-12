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

import org.agrona.concurrent.UnsafeBuffer;

/**
 * Simple utility class for dumping the structure of a trie to string.
 */
class TrieDumper<T>
{
    public static <T> String process(Function<T, String> contentToString, Trie<T> trie)
    {
        return dump(contentToString, trie.cursor());
    }

    static <T> String dump(Function<T, String> contentToString, Trie.Cursor<T> cursor)
    {
        StringBuilder sb = new StringBuilder();
        Trie.ResettingTransitionsReceiver receiver = new TransitionsDumper(sb);
        T content = cursor.content();
        if (content == null)
            content = cursor.advanceToContent(receiver);
        while (content != null)
        {
            sb.append(" -> ");
            sb.append(contentToString.apply(content));
            receiver.reset(cursor.level());
            content = cursor.advanceToContent(receiver);
        }
        return sb.toString();
    }

    private static class TransitionsDumper implements Trie.ResettingTransitionsReceiver
    {
        private final StringBuilder b;
        int needsIndent = -1;

        public TransitionsDumper(StringBuilder b)
        {
            this.b = b;
        }

        @Override
        public void reset(int newLength)
        {
            needsIndent = newLength;
        }

        private void maybeIndent()
        {
            if (needsIndent >= 0)
            {
                b.append('\n');
                for (int i = 0; i < needsIndent; ++i)
                    b.append("  ");
                needsIndent = -1;
            }
        }

        @Override
        public void add(int incomingTransition)
        {
            maybeIndent();
            b.append(String.format("%02x", incomingTransition));
        }

        @Override
        public void add(UnsafeBuffer buf, int pos, int count)
        {
            maybeIndent();
            for (int i = 0; i < count; ++i)
                b.append(String.format("%02x", buf.getByte(pos + i) & 0xFF));
        }
    }
}
