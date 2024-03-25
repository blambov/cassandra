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

public class InMemoryNDTrie<T extends NonDeterministicTrie.Mergeable<T>> extends InMemoryTrie<T> implements NonDeterministicTrieWithImpl<T>
{
    public InMemoryNDTrie(BufferType bufferType)
    {
        super(bufferType);
    }

    class NonDeterministicCursor extends MemtableCursor implements NonDeterministicTrieImpl.Cursor<T>
    {
        NonDeterministicCursor(int root, int depth, int incomingTransition)
        {
            super(root, depth, incomingTransition);
        }

        NonDeterministicCursor(NonDeterministicCursor copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public NonDeterministicCursor alternateBranch()
        {
            return isNull(alternateBranch) ? null : new NonDeterministicCursor(alternateBranch, depth() - 1, incomingTransition());
        }

        @Override
        public NonDeterministicCursor duplicate()
        {
            return new NonDeterministicCursor(this);
        }
    }

    @Override
    public Cursor<T> makeCursor()
    {
        return new NonDeterministicCursor(root, -1, -1);
    }
}
