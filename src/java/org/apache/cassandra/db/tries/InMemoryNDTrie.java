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

public class InMemoryNDTrie<T extends NonDeterministicTrie.Mergeable<T>> extends InMemoryTrie<T> implements NonDeterministicTrieWithImpl<T>
{
    public InMemoryNDTrie(BufferType bufferType)
    {
        super(bufferType);
    }

    static class NonDeterministicCursor<T extends NonDeterministicTrie.Mergeable<T>>
    extends MemtableCursor<T>
    implements NonDeterministicTrieImpl.Cursor<T>
    {
        NonDeterministicCursor(InMemoryReadTrie<T> trie, int root, int depth, int incomingTransition)
        {
            super(trie, root, depth, incomingTransition);
        }

        NonDeterministicCursor(NonDeterministicCursor<T> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public NonDeterministicCursor<T> alternateBranch()
        {
            return isNull(alternateBranch) ? null : new NonDeterministicCursor<>(trie, alternateBranch, depth() - 1, incomingTransition());
        }

        @Override
        public T content()
        {
            return content;
        }

        @Override
        public NonDeterministicCursor<T> duplicate()
        {
            return new NonDeterministicCursor<>(this);
        }
    }

    @Override
    public Cursor<T> makeCursor()
    {
        return new NonDeterministicCursor<>(this, root, -1, -1);
    }

    /**
     * Override of dump to provide more detailed printout that includes the type of each node in the trie.
     * We do this via a wrapping cursor that returns a content string for the type of node for every node we return.
     */
    public String dump(Function<T, String> contentToString)
    {
        return dump(contentToString, root);
    }
}
