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
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class InMemoryDTrie<T> extends InMemoryTrie<T> implements TrieWithImpl<T>
{
    public InMemoryDTrie(MemtableAllocationStrategy strategy)
    {
        super(strategy);
    }

    private InMemoryDTrie(MemtableAllocationStrategy strategy, int root)
    {
        super(strategy);
        this.root = root;
    }

    public static <T> InMemoryDTrie<T> shortLived()
    {
        return new InMemoryDTrie<T>(shortLivedStrategy());
    }

    public static <T> InMemoryDTrie<T> longLived(OpOrder opOrder)
    {
        return new InMemoryDTrie<T>(longLivedStrategy(opOrder));
    }

    public static <T> InMemoryDTrie<T> longLived(BufferType bufferType, OpOrder opOrder)
    {
        return new InMemoryDTrie<T>(longLivedStrategy(bufferType, opOrder));
    }

    static class DeterministicCursor<T> extends MemtableCursor<T> implements TrieImpl.Cursor<T>
    {
        DeterministicCursor(InMemoryReadTrie<T> trie, Direction direction, int root, int depth, int incomingTransition)
        {
            super(trie, direction, root, depth, incomingTransition);
        }

        DeterministicCursor(DeterministicCursor<T> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public T content()
        {
            return content;
        }

        @Override
        public DeterministicCursor<T> duplicate()
        {
            return new DeterministicCursor<>(this);
        }
    }

    @Override
    public Cursor<T> makeCursor(Direction direction)
    {
        return new DeterministicCursor<>(this, direction, root, -1, -1);
    }

    /**
     * Override of dump to provide more detailed printout that includes the type of each node in the trie.
     * We do this via a wrapping cursor that returns a content string for the type of node for every node we return.
     */
    @Override
    public String dump(Function<T, String> contentToString)
    {
        return dump(contentToString, root);
    }

    /**
     * Override tailTrie with a version that seeks to the tail position and returns a trie with that as a root.
     * This is important to ensure that further modifications to the trie are not reflected in the extracted trie
     * (as long as force-copying applies on that level of the trie).
     *
     * @param key
     * @return Tail trie rooted at the position reached after following the given path. May be null or empty if the path
     * is not found.
     */
    @Override
    public Trie<T> tailTrie(ByteComparable key)
    {
        int node = nodeAt(key);
        return isNull(node) ? null : new InMemoryDTrie<>(allocator, node);
    }
}
