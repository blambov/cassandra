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

public class NegatedTrieSet extends TrieSet
{
    final TrieSet source;

    public NegatedTrieSet(TrieSet source)
    {
        this.source = source;
    }

    @Override
    protected Cursor cursor()
    {
        return new NegatedCursor(source.cursor());
    }

    static class NegatedCursor implements Cursor
    {
        final Cursor source;

        NegatedCursor(Cursor source)
        {
            this.source = source;
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
        public Contained contained()
        {
            switch (source.contained())
            {
                case OUTSIDE_PREFIX:
                    return Contained.INSIDE_PREFIX;
                case START:
                    return Contained.END;
                case INSIDE_PREFIX:
                    return Contained.OUTSIDE_PREFIX;
                case END:
                    return Contained.START;
                default:
                    throw new AssertionError();
            }
        }

        @Override
        public int advance()
        {
            return source.advance();
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return source.skipTo(skipDepth, skipTransition);
        }

        @Override
        public Cursor duplicate()
        {
            return new NegatedCursor(source.duplicate());
        }
    }
}
