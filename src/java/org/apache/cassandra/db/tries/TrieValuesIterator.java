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

import java.util.Iterator;

/**
 * Ordered iterator of trie content.
 */
class TrieValuesIterator<T> implements Iterator<T>
{
    private final Trie.Cursor<T> cursor;
    T next;
    boolean gotNext;

    protected TrieValuesIterator(Trie.Cursor<T> cursor)
    {
        this.cursor = cursor;
        assert cursor.depth() == 0;
        next = cursor.content();
        gotNext = next != null;
    }

    public boolean hasNext()
    {
        if (!gotNext)
        {
            next = cursor.advanceToContent(null);
            gotNext = true;
        }

        return next != null;
    }

    public T next()
    {
        if (!hasNext())
            throw new IllegalStateException("next without hasNext");

        gotNext = false;
        T v = next;
        next = null;
        return v;
    }

    static class FilteredByType<T, U> implements Iterator<U>
    {
        private final Trie.Cursor<T> cursor;
        T next;
        boolean gotNext;
        Class<U> clazz;

        FilteredByType(Trie.Cursor<T> cursor, Class<U> clazz)
        {
            this.cursor = cursor;
            this.clazz = clazz;
            assert cursor.depth() == 0;
            next = cursor.content();
            gotNext = next != null && clazz.isInstance(next);
        }

        public boolean hasNext()
        {
            while (!gotNext)
            {
                next = cursor.advanceToContent(null);
                if (next == null)
                    return false;
                gotNext = clazz.isInstance(next);
            }

            return true;
        }

        public U next()
        {
            if (!hasNext())
                throw new IllegalStateException("next without hasNext");

            gotNext = false;
            T v = next;
            next = null;
            return (U) v;
        }
    }
}
