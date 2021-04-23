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
import java.util.Iterator;
import java.util.Map;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Convertor of trie entries to iterator where each entry is passed through {@link #mapContent} (to be implemented by
 * descendants).
 */
public abstract class TrieEntriesIterator<T, V> implements Iterator<V>, Trie.ResettingTransitionsReceiver
{
    private final Trie.Cursor<T> cursor;
    private byte[] keyBytes = new byte[32];
    private int keyPos = 0;
    T next;
    boolean gotNext;

    protected TrieEntriesIterator(Trie<T> trie)
    {
        cursor = trie.cursor();
        next = cursor.content();
        gotNext = next != null;
    }

    public boolean hasNext()
    {
        if (!gotNext)
        {
            next = cursor.advanceToContent(this);
            gotNext = true;
        }

        return next != null;
    }

    public V next()
    {
        gotNext = false;
        T v = next;
        next = null;
        return mapContent(v, keyBytes, keyPos);
    }

    public void add(int t)
    {
        if (keyPos >= keyBytes.length)
            keyBytes = Arrays.copyOf(keyBytes, keyPos * 2);
        keyBytes[keyPos++] = (byte) t;
    }

    public void add(UnsafeBuffer b, int pos, int count)
    {
        int newPos = keyPos + count;
        if (newPos > keyBytes.length)
            keyBytes = Arrays.copyOf(keyBytes, Math.max(newPos + 16, keyBytes.length * 2));
        b.getBytes(pos, keyBytes, keyPos, count);
        keyPos = newPos;
    }

    public void reset(int newLength)
    {
        keyPos = newLength;
    }

    protected abstract V mapContent(T content, byte[] bytes, int byteLength);

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

        protected Map.Entry<ByteComparable, T> mapContent(T content, byte[] bytes, int byteLength)
        {
            return toEntry(content, bytes, byteLength);
        }
    }

    static <T> java.util.Map.Entry<ByteComparable, T> toEntry(T content, byte[] bytes, int byteLength)
    {
        ByteComparable b = ByteComparable.fixedLength(Arrays.copyOf(bytes, byteLength));
        return new AbstractMap.SimpleImmutableEntry<>(b, content);
    }
}
