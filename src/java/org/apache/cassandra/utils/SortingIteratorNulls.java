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

package org.apache.cassandra.utils;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

public class SortingIteratorNulls<T> implements Iterator<T>
{
    final Comparator<? super T> comparator;
    final T[] heap;
    final int size;

    public <V> SortingIteratorNulls(Class<T> itemClass, Comparator<? super T> comparator, Collection<V> sources, Function<V, T> mapper)
    {
        this.comparator = comparator;
        size = Math.max(1, sources.size());
        @SuppressWarnings("unchecked")
        T[] h = (T[]) Array.newInstance(itemClass, size);
        this.heap = h;
        int count = 0;
        for (V v : sources)
        {
            T item = mapper.apply(v);
            heap[count++] = item;
        }
        for (int i = size / 2 - 1; i >= 0; --i)
            heapifyDown(heap[i], i);
    }

    protected boolean greaterThan(T a, T b)
    {
        // nulls are greater than non-nulls to be placed at the end
        if (a == null || b == null)
            return b != null;
        return comparator.compare(a, b) > 0;
    }

    @Override
    public boolean hasNext()
    {
        return heap[0] != null;
    }

    @Override
    public T next()
    {
        T item = heap[0];
        if (item == null)
            throw new NoSuchElementException();
        heapifyDown(null, 0);
        return item;
    }

    public void skipTo(T targetKey)
    {
        skipTo(0, targetKey);
    }

    private void skipTo(int heapIndex, T targetKey)
    {
        if (heapIndex >= size)
            return;
        if (!greaterThan(targetKey, heap[heapIndex]))
            return;

        int nextIndex = heapIndex * 2 + 1;
        if (nextIndex >= size)
        {
            heap[heapIndex] = null;
        }
        else
        {
            skipTo(nextIndex, targetKey);
            skipTo(nextIndex + 1, targetKey);

            heapifyDown(null, heapIndex);
        }
    }

    /**
     * Push the given state down in the heap from the given index until it finds its proper place among
     * the subheap rooted at that position.
     */
    private void heapifyDown(T item, int index)
    {
        while (true)
        {
            int next = index * 2 + 1;
            if (next >= size)
                break;
            // Select the smaller of the two children to push down to.
            if (next + 1 < size && greaterThan(heap[next], heap[next + 1]))
                ++next;
            // If the child is greater or equal, the invariant has been restored.
            if (!greaterThan(item, heap[next]))
                break;
            heap[index] = heap[next];
            index = next;
        }
        heap[index] = item;
    }

    public static class Closeable<T> extends SortingIteratorNulls<T> implements CloseableIterator<T>
    {
        final Runnable onClose;

        public <V> Closeable(Class<T> itemClass,
                             Comparator<? super T> comparator,
                             Collection<V> sources,
                             Function<V, T> mapper,
                             Runnable onClose)
        {
            super(itemClass, comparator, sources, mapper);
            this.onClose = onClose;
        }

        @Override
        public void close()
        {
            onClose.run();
        }
    }
}
