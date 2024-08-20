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

import java.util.Comparator;
import java.util.function.BiFunction;

/**
 * A base binary heap implementation with fixed size, supporting only operations that push
 * data down in the heap (i.e. after the initial initialization of the heap from a collection
 * of items, only top/smallest items can be modified (removed or replaced with larger)).
 * <p>
 * This class does not implement a priority queue (because a queue's purpose is to support
 * adding and removing items), but works very well as a source of sorted entries e.g. for
 * merging iterators, producing a sorted iterator from an unsorted list of items
 * ({@link SortingIterator}) or selecting the top items from data of unbounded size
 * ({@link TopKSelector}).
 * <p>
 * The implementation supports nulls among the source entries and relies on support for them
 * to make some operations more efficient.
 * <p>
 * This class is not intended to be used as a priority queue and does not support adding elements
 * to the set after the initial construction. If a priority queue is required, see
 * {@link LucenePriorityQueue}.
 */
public abstract class BinaryHeap
{
    // Note: This class is tested via its descendants by SortingIteratorTest and TopKSelectorTest.

    protected final Object[] heap;

    /**
     * Create a binary heap with the given array. The data must be heapified before being used.
     */
    public BinaryHeap(Object[] data)
    {
        this.heap = data;
        // Note that we can't perform any preparation here because the subclass defining greaterThan may have not been
        // initialized yet.
    }

    /**
     * Compare two objects and return true iff the first is greater.
     * The method must treat nulls as greater than non-null objects.
     */
    protected abstract boolean greaterThan(Object a, Object b);

    /**
     * Turn the current list of items into a binary heap by using the initial heap construction
     * of the heapsort algorithm with complexity O(heap.length).
     */
    protected void heapify()
    {
        heapifyUpTo(heap.length);
    }

    /**
     * Turn the list of items until the given index into a binary heap by using the initial heap construction
     * of the heapsort algorithm with complexity O(heap.length).
     */
    protected void heapifyUpTo(int size)
    {
        for (int i = size / 2 - 1; i >= 0; --i)
            heapifyDownUpTo(heap[i], i, size);
    }

    protected boolean isEmpty()
    {
        return heap[0] == null;
    }

    /**
     * Return the next element in the heap without advancing.
     */
    protected Object peek()
    {
        return heap[0];
    }

    /**
     * Get and remove the next element in the heap.
     * If the heap contains duplicates, they will be returned in an arbitrary order.
     */
    protected Object pop()
    {
        return replaceTop(null);
    }

    /**
     * Get and replace the top item with a new one. The new must compare greater than
     * or equal to the item being replaced.
     */
    protected Object replaceTop(Object newItem)
    {
        Object item = heap[0];
        heapifyDown(newItem, 0);
        return item;
    }

    /**
     * Get the next element and skip over all items equal to it.
     * Calling this instead of {@link #pop} results in deduplication of the list
     * of entries.
     */
    protected Object popAndSkipEqual()
    {
        Object item = heap[0];
        advanceBeyond(item, 0);
        return item;
    }

    /**
     * Recursively drop all elements in the subheap rooted at the given heapIndex that are not beyond the given
     * targetKey, and restore the heap ordering on the way back from the recursion.
     */
    private void advanceBeyond(Object targetKey, int heapIndex)
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
            advanceBeyond(targetKey, nextIndex);
            advanceBeyond(targetKey, nextIndex + 1);

            heapifyDown(null, heapIndex);
        }
    }

    /**
     * Skip to the first element that is greater than or equal to the given key.
     */
    protected void advanceTo(Object targetKey)
    {
        advanceTo(targetKey, 0);
    }

    /**
     * Recursively drop all elements in the subheap rooted at the given heapIndex that are before the given
     * targetKey, and restore the heap ordering on the way back from the recursion.
     */
    private void advanceTo(Object targetKey, int heapIndex)
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
            advanceTo(targetKey, nextIndex);
            advanceTo(targetKey, nextIndex + 1);

            heapifyDown(null, heapIndex);
        }
    }

    /**
     * Skip to the first element that is greater than or equal to the given key, applying the given function to adjust
     * each element instead of directly replacing it with null.
     */
    protected <T, K extends T> void advanceTo(K targetKey, BiFunction<T, K, T> itemAdvancer)
    {
        advanceTo(targetKey, 0, itemAdvancer);
    }

    /**
     * Recursively drop all elements in the subheap rooted at the given heapIndex that are before the given
     * targetKey, applying the given function to adjust each element instead of directly replacing it with null, and
     * restore the heap ordering on the way back from the recursion.
     */
    private <T, K extends T> void advanceTo(K targetKey, int heapIndex, BiFunction<T, K, T> itemAdvancer)
    {
        if (heapIndex >= heap.length)
            return;
        Object item = heap[heapIndex];
        if (!greaterThan(targetKey, item))
            return;

        int nextIndex = heapIndex * 2 + 1;
        if (nextIndex >= heap.length)
        {
            heap[heapIndex] = itemAdvancer.apply((T) item, targetKey);
        }
        else
        {
            advanceTo(targetKey, nextIndex);
            advanceTo(targetKey, nextIndex + 1);

            heapifyDown(itemAdvancer.apply((T) item, targetKey), heapIndex);
        }
    }

    /**
     * Push the given state down in the heap from the given index until it finds its proper place among
     * the subheap rooted at that position.
     */
    private void heapifyDown(Object item, int index)
    {
        heapifyDownUpTo(item, index, heap.length);
    }

    /**
     * Push the given state down in the heap from the given index until it finds its proper place among
     * the subheap rooted at that position.
     */
    private void heapifyDownUpTo(Object item, int index, int size)
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

    /**
     * Sort the heap by repeatedly popping the top item and placing it at the end of the heap array.
     * The result will contain the elements in the heap sorted in descending order.
     * The heap must be heapified up to the size before calling this method.
     */
    protected void heapSortUpTo(int size)
    {
        // Sorting the ones from 1 will also make put the right value in heap[0]
        heapSortBetween(1, size);
    }

    /**
     * Partially sort the heap by repeatedly popping the top item and placing it at the end of the heap array,
     * until the given start position is reached. This results in a partial sorting where the smallest items
     * (according to the comparator) are placed at positions of the heap between start and size in descending order,
     * and the items before that are left heapified.
     * The heap must be heapified up to the size before calling this method.
     * Used to fetch items after a certain offset in a top-k selection.
     */
    protected void heapSortBetween(int start, int size)
    {
        // Data must already be heapified up to that size, comparator must be reverse
        for (int i = size - 1; i >= start; --i)
        {
            Object top = heap[0];
            heapifyDownUpTo(heap[i], 0, i);
            heap[i] = top;
        }
    }

    /**
     * A binary heap that uses a comparator to determine the order of elements, implementing the necessary handling
     * of nulls.
     */
    public static class WithComparator<T> extends BinaryHeap
    {
        final Comparator<? super T> comparator;

        public WithComparator(Comparator<? super T> comparator, Object[] data)
        {
            super(data);
            this.comparator = comparator;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean greaterThan(Object a, Object b)
        {
            // nulls are treated as greater than non-nulls to be placed at the end of the sequence
            if (a == null || b == null)
                return b != null;
            return comparator.compare((T) a, (T) b) > 0;
        }
    }
}
