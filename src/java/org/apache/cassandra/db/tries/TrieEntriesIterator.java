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

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;

import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Convertor of trie entries to iterator where each entry is passed through {@link #mapContent} (to be implemented by
 * descendants).
 */
public abstract class TrieEntriesIterator<T, V> extends AbstractIterator<V>
{
    private final Trie.Cursor<T> cursor;

    protected TrieEntriesIterator(Trie<T> trie)
    {
        this.cursor = trie.cursor();
    }

    public V computeNext()
    {
        T value = cursor.advanceToContent();
        if (value == null)
            return endOfData();

        int keyLength = cursor.level();

        byte[] array = new byte[keyLength];
        cursor.retrieveKey(array);
        return mapContent(value, array);
    }

    protected abstract V mapContent(T content, byte[] bytes);

    /**
     * Iterator representing the content of the trie a sequence of (path, content) pairs.
     */
    static class AsEntries<T>
    extends TrieEntriesIterator<T, Map.Entry<ByteComparable, T>>
    {
        public AsEntries(Trie<T> trie)
        {
            super(trie);
        }

        protected Map.Entry<ByteComparable, T> mapContent(T content, byte[] bytes)
        {
            return toEntry(content, bytes);
        }
    }

    static <T> java.util.Map.Entry<ByteComparable, T> toEntry(T content, byte[] bytes)
    {
        ByteComparable b = ByteComparable.fixedLength(bytes);
        return new AbstractMap.SimpleImmutableEntry<>(b, content);
    }
}
