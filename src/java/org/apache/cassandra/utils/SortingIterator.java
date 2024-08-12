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

public class SortingIterator<T> implements Iterator<T>
{
    final Comparator<? super T> comparator;
    final T[] heap;
    int size;

    public <V> SortingIterator(Class<T> itemClass, Comparator<? super T> comparator, Collection<V> sources, Function<V, T> mapper)
    {
        this.comparator = comparator;
        @SuppressWarnings("unchecked")
        T[] h = (T[]) Array.newInstance(itemClass, sources.size());
        this.heap = h;
        for (V v : sources)
        {
            T item = mapper.apply(v);
            if (item != null)
                heap[size++] = item;
        }
        for (int i = size / 2 - 1; i >= 0; --i)
            heapifyDown(heap[i], i);
    }

    protected boolean greaterThan(T a, T b)
    {
        return comparator.compare(a, b) > 0;
    }

    @Override
    public boolean hasNext()
    {
        return size > 0;
    }

    @Override
    public T next()
    {
        if (size <= 0)
            throw new NoSuchElementException();
        T item = heap[0];
        heapifyDown(heap[--size], 0);
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

        T newItem;
        do
        {
            if (--size <= heapIndex)
                return;
            newItem = heap[size];
        }
        while (greaterThan(targetKey, newItem));

        int nextIndex = heapIndex * 2 + 1;
        if (nextIndex >= size)
        {
            heap[heapIndex] = newItem;
        }
        else
        {
            skipTo(nextIndex, targetKey);
            skipTo(nextIndex + 1, targetKey);

            heapifyDown(newItem, heapIndex);
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

    public static class Closeable<T> extends SortingIterator<T> implements CloseableIterator<T>
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
