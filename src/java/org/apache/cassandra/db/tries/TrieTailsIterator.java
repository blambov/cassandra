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
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Convertor of trie entries to iterator where each entry is passed through {@link #mapContent} (to be implemented by
 * descendants).
 */
public abstract class TrieTailsIterator<T, V> extends TriePathReconstructor implements Iterator<V>
{
    private final Direction direction;
    private final TrieImpl.Cursor<T> cursor;
    private final Predicate<T> predicate;
    T next;
    boolean gotNext;

    TrieTailsIterator(Direction direction, TrieImpl.Cursor<T> cursor, Predicate<T> predicate)
    {
        this.direction = direction;
        this.cursor = cursor;
        this.predicate = predicate;
        assert cursor.depth() == 0;
    }

    public boolean hasNext()
    {
        while (!gotNext)
        {
            int depth = cursor.depth();
            if (depth > 0)
            {
                // if we are not at the root, skip the branch we just returned
                depth = cursor.skipTo(depth, cursor.incomingTransition() + 1);
                if (depth < 0)
                    return false;
                resetPathLength(depth - 1);
                addPathByte(cursor.incomingTransition());
            }
            next = cursor.content();

            if (next == null) // usually true
                next = cursor.advanceToContent(this);
            if (next != null)
                gotNext = predicate.test(next);
            else
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

    protected abstract V mapContent(T content, byte[] bytes, int byteLength);

    /**
     * Iterator representing the content of the trie a sequence of (path, content) pairs.
     */
    static class AsEntries<T, U> extends TrieTailsIterator<T, Map.Entry<ByteComparable, U>>
    {
        final Function<ByteComparable, U> tailMaker;

        public AsEntries(TrieImpl.Cursor<T> cursor, Predicate<T> predicate, Function<ByteComparable, U> tailMaker)
        {
            super(cursor, predicate);
            this.tailMaker = tailMaker;
        }

        @Override
        protected Map.Entry<ByteComparable, U> mapContent(T content, byte[] bytes, int byteLength)
        {
            ByteComparable key = toByteComparable(bytes, byteLength);
            return new AbstractMap.SimpleImmutableEntry<>(key, tailMaker.apply(key));
        }
    }
}
