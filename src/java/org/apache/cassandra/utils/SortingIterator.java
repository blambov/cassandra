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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * An iterator that lists a set of items in order.
 * <p>
 * This is intended for use where we would normally read only a small subset of the elements, or where we would skip
 * over large sections of the sorted set. To implement this efficiently, we put the data in a binary heap and extract
 * elements as the iterator is queried, effectively performing heapsort. We also implement a quicker skipTo operation
 * where we remove all smaller elements and restore the heap for all of them in one step.
 * <p>
 * As in heapsort, the first stage of the process has complexity O(n), and every next item is extracted in O(log n)
 * steps. skipTo works in O(m.log n) steps (where m is the number of skipped items), but is also limited to O(n) when m
 * is large by the same argument as the initial heap construction.
 * <p>
 * The accepts and stores nulls as non-present values, which turns out to be quite a bit more efficient for iterating
 * these sets when the comparator is complex at the expense of a small slowdown for simple comparators. The reason for
 * this is that we can remove entries by replacing them with nulls and letting these descend the heap, which avoids half
 * the comparisons compared to using the largest live element.
 * <p>
 * This class is not intended to be used as a priority queue and does not support adding elements to the set after the
 * initial construction. If a priority queue is required, see {@link LucenePriorityQueue}.
 */
public class SortingIterator<T> implements Iterator<T>
{
    final Comparator<? super T> comparator;
    final Object[] heap;

    SortingIterator(Comparator<? super T> comparator, Object[] data)
    {
        this.comparator = comparator;
        this.heap = data;

        heapify();
    }

    public static <T> SortingIterator<T> create(Comparator<? super T> comparator, Collection<T> sources)
    {
        return new SortingIterator<>(comparator, sources.isEmpty() ? new Object[1] : sources.toArray());
    }

    public static <T, V> SortingIterator<T> create(Comparator<? super T> comparator, Collection<V> sources, Function<V, T> mapper)
    {
        return new Builder<>(sources, mapper).build(comparator);
    }

    public static <T, V> CloseableIterator<T> createCloseable(Comparator<? super T> comparator, Collection<V> sources, Function<V, T> mapper, Runnable onClose)
    {
        return new Builder<>(sources, mapper).closeable(comparator, onClose);
    }

    public static <T> SortingIterator<T> createDeduplicating(Comparator<? super T> comparator, Collection<T> sources)
    {
        return new Deduplicating<>(comparator, sources.isEmpty() ? new Object[1] : sources.toArray());
    }

    private void heapify()
    {
        for (int i = heap.length / 2 - 1; i >= 0; --i)
            heapifyDown(heap[i], i);
    }

    @SuppressWarnings("unchecked")
    protected boolean greaterThan(Object a, Object b)
    {
        // nulls are treated as greater than non-nulls to be placed at the end of the sequence
        if (a == null || b == null)
            return b != null;
        return comparator.compare((T) a, (T) b) > 0;
    }

    @Override
    public boolean hasNext()
    {
        return heap[0] != null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T next()
    {
        Object item = heap[0];
        if (item == null)
            throw new NoSuchElementException();
        heapifyDown(null, 0);
        return (T) item;
    }

    /**
     * Get the next element and consume all items equal to it.
     */
    @SuppressWarnings("unchecked")
    public T nextAndSkipEqual()
    {
        Object item = heap[0];
        if (item == null)
            throw new NoSuchElementException();
        skipBeyond(item, 0);
        return (T) item;
    }

    /**
     * Recursively drop all elements in the subheap rooted at the given heapIndex that are not beyond the given
     * targetKey, and restore the heap ordering on the way back from the recursion.
     */
    private void skipBeyond(Object targetKey, int heapIndex)
    {
        if (heapIndex >= heap.length)
            return;
        if (greaterThan(heap[heapIndex], targetKey))
            return;

        int nextIndex = heapIndex * 2 + 1;
        if (nextIndex >= heap.length)
        {
            heap[heapIndex] = null;
        }
        else
        {
            skipBeyond(targetKey, nextIndex);
            skipBeyond(targetKey, nextIndex + 1);

            heapifyDown(null, heapIndex);
        }
    }

    /**
     * Skip to the first element that is greater than or equal to the given key.
     */
    public void skipTo(T targetKey)
    {
        skipTo(targetKey, 0);
    }

    /**
     * Recursively drop all elements in the subheap rooted at the given heapIndex that are before the given
     * targetKey, and restore the heap ordering on the way back from the recursion.
     */
    private void skipTo(T targetKey, int heapIndex)
    {
        if (heapIndex >= heap.length)
            return;
        if (!greaterThan(targetKey, heap[heapIndex]))
            return;

        int nextIndex = heapIndex * 2 + 1;
        if (nextIndex >= heap.length)
        {
            heap[heapIndex] = null;
        }
        else
        {
            skipTo(targetKey, nextIndex);
            skipTo(targetKey, nextIndex + 1);

            heapifyDown(null, heapIndex);
        }
    }

    /**
     * Push the given state down in the heap from the given index until it finds its proper place among
     * the subheap rooted at that position.
     */
    private void heapifyDown(Object item, int index)
    {
        while (true)
        {
            int next = index * 2 + 1;
            if (next >= heap.length)
                break;
            // Select the smaller of the two children to push down to.
            if (next + 1 < heap.length && greaterThan(heap[next], heap[next + 1]))
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

        public <V> Closeable(Comparator<? super T> comparator,
                             Object[] data,
                             Runnable onClose)
        {
            super(comparator, data);
            this.onClose = onClose;
        }

        @Override
        public void close()
        {
            onClose.run();
        }
    }

    public static class Deduplicating<T> extends SortingIterator<T>
    {
        public Deduplicating(Comparator<? super T> comparator, Object[] data)
        {
            super(comparator, data);
        }

        @Override
        public T next()
        {
            return super.nextAndSkipEqual();
        }
    }

    public static class Builder<T>
    {
        Object[] data;
        int count;

        public Builder()
        {
            this(16);
        }

        public Builder(int initialSize)
        {
            data = new Object[Math.max(initialSize, 1)]; // at least one element so that we don't need to special-case empty
            count = 0;
        }

        public <V> Builder(Collection<V> collection, Function<V, T> mapper)
        {
            this(collection.size());
            for (V item : collection)
                data[count++] = mapper.apply(item); // this may be null, which the iterator will properly handle
        }

        public Builder<T> add(T element)
        {
            if (element != null)   // avoid growing if we don't need to
            {
                if (count == data.length)
                    data = Arrays.copyOf(data, data.length * 2);
                data[count++] = element;
            }
            return this;
        }

        public Builder<T> addAll(Collection<? extends T> collection)
        {
            if (count + collection.size() > data.length)
                data = Arrays.copyOf(data, count + collection.size());
            for (T item : collection)
                data[count++] = item;
            return this;
        }

        public <V> Builder<T> addAll(Collection<V> collection, Function<V, T> mapper)
        {
            if (count + collection.size() > data.length)
                data = Arrays.copyOf(data, count + collection.size());
            for (V item : collection)
                data[count++] = mapper.apply(item); // this may be null, which the iterator will properly handle
            return this;
        }

        public int size()
        {
            return count; // Note: may include null elements, depending on how data is added
        }

        public SortingIterator<T> build(Comparator<? super T> comparator)
        {
            return new SortingIterator<>(comparator, data);    // this will have nulls at the end, which is okay
        }

        public Closeable<T> closeable(Comparator<? super T> comparator, Runnable onClose)
        {
            return new Closeable<>(comparator, data, onClose);
        }

        public SortingIterator<T> deduplicating(Comparator<? super T> comparator)
        {
            return new Deduplicating<>(comparator, data);
        }

        // This does not offer build methods that trim the array to count (i.e. Arrays.copyOf(data, count) instead of
        // data), because it is only meant for short-lived operations where the iterator is not expected to live much
        // longer than the builder and thus both the builder and iterator will almost always expire in the same GC cycle
        // and thus the cost of trimming is not offset by any gains.
    }
}
