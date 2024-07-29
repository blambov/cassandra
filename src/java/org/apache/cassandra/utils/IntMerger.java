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


import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.function.Consumer;

/*
 * The merge cursor is a variation of the idea of a merge iterator with one key observation: because we advance
 * the source iterators together, we can compare them just by depth and incoming transition.
 *
 * The most straightforward way to implement merging of iterators is to use a {@code PriorityQueue},
 * {@code poll} it to find the next item to consume, then {@code add} the iterator back after advancing.
 * This is not very efficient as {@code poll} and {@code add} in all cases require at least
 * {@code log(size)} comparisons and swaps (usually more than {@code 2*log(size)}) per consumed item, even
 * if the input is suitable for fast iteration.
 *
 * The implementation below makes use of the fact that replacing the top element in a binary heap can be
 * done much more efficiently than separately removing it and placing it back, especially in the cases where
 * the top iterator is to be used again very soon (e.g. when there are large sections of the output where
 * only a limited number of input iterators overlap, which is normally the case in many practically useful
 * situations, e.g. levelled compaction).
 *
 * The implementation builds and maintains a binary heap of sources (stored in an array), where we do not
 * add items after the initial construction. Instead we advance the smallest element (which is at the top
 * of the heap) and push it down to find its place for its new position. Should this source be exhausted,
 * we swap it with the last source in the heap and proceed by pushing that down in the heap.
 *
 * In the case where we have multiple sources with matching positions, the merging algorithm
 * must be able to merge all equal values. To achieve this {@code content} walks the heap to
 * find all equal cursors without advancing them, and separately {@code advance} advances
 * all equal sources and restores the heap structure.
 *
 * The latter is done equivalently to the process of initial construction of a min-heap using back-to-front
 * heapification as done in the classic heapsort algorithm. It only needs to heapify subheaps whose top item
 * is advanced (i.e. one whose position matches the current), and we can do that recursively from
 * bottom to top. Should a source be exhausted when advancing, it can be thrown away by swapping in the last
 * source in the heap (note: we must be careful to advance that source too if required).
 *
 * To make it easier to advance efficienty in single-sourced branches of tries, we extract the current smallest
 * cursor (the head) separately, and start any advance with comparing that to the heap's first. When the smallest
 * cursor remains the same (e.g. in branches coming from a single source) this makes it possible to advance with
 * just one comparison instead of two at the expense of increasing the number by one in the general case.
 *
 * Note: This is a simplification of the MergeIterator code from CASSANDRA-8915, without the leading ordered
 * section and equalParent flag since comparisons of cursor positions are cheap.
 */
public abstract class IntMerger<Source>
{
    /**
     * The smallest cursor, tracked separately to improve performance in single-source sections of the trie.
     */
    protected Source head;

    /**
     * Binary heap of the remaining cursors. The smallest element is at position 0.
     * Every element i is smaller than or equal to its two children, i.e.
     *     heap[i] <= heap[i*2 + 1] && heap[i] <= heap[i*2 + 2]
     */
    private final Source[] heap;
    boolean started;

    protected abstract int position(Source s);
    protected abstract void advanceSource(Source s) throws IOException;
    protected abstract void skipSource(Source s, int targetPosition) throws IOException;

    protected boolean greaterCursor(Source a, Source b)
    {
        return position(a) > position(b);
    }

    protected IntMerger(Collection<? extends Source> inputs, Class<Source> sourceClass)
    {
        int count = inputs.size();
        // Get cursors for all inputs. Put one of them in head and the rest in the heap.
        heap = (Source[]) Array.newInstance(sourceClass, count - 1);
        int i = -1;
        for (Source source : inputs)
        {
            if (i >= 0)
                heap[i] = source;
            else
                head = source;
            ++i;
        }
        // Do not fetch items until requested
        started = false;
    }

    /**
     * Apply an operation to all elements on the heap that satisfy, recursively through the heap hierarchy, the
     * {@code shouldContinueWithChild} condition (being equal to the head by default). Descends recursively in the
     * heap structure to all selected children and applies the operation on the way back.
     * <p>
     * This operation can be something that does not change the cursor state or an operation
     * that advances the cursor to a new state, wrapped in a {@link HeapOp} ({@link #advance} or
     * {@link #skipSource}). The latter interface takes care of pushing elements down in the heap after advancing
     * and restores the subheap state on return from each level of the recursion.
     */
    private void advanceHeap(int headPosition, int index) throws IOException
    {
        if (index >= heap.length)
            return;

        Source item = heap[index];
        if (position(item) > headPosition)
            return;

        // If the children are at the same position, they also need advancing and their subheap
        // invariant to be restored.
        advanceHeap(headPosition, index * 2 + 1);
        advanceHeap(headPosition, index * 2 + 2);

        // Apply the action. This is done on the reverse direction to give the action a chance to form proper
        // subheaps and combine them on processing the parent.
        // Apply the operation, which should advance the position of the element.
        advanceSource(item);

        // This method is called on the back path of the recursion. At this point the heaps at both children are
        // advanced and well-formed.
        // Place current node in its proper position.
        heapifyDown(item, index);
        // The heap rooted at index is now advanced and well-formed.
    }


    /**
     * Apply an operation to all elements on the heap that satisfy, recursively through the heap hierarchy, the
     * {@code shouldContinueWithChild} condition (being equal to the head by default). Descends recursively in the
     * heap structure to all selected children and applies the operation on the way back.
     * <p>
     * This operation can be something that does not change the cursor state or an operation
     * that advances the cursor to a new state, wrapped in a {@link HeapOp} ({@link #advance} or
     * {@link #skipSource}). The latter interface takes care of pushing elements down in the heap after advancing
     * and restores the subheap state on return from each level of the recursion.
     */
    private void skipHeap(int targetPosition, int index) throws IOException
    {
        if (index >= heap.length)
            return;

        Source item = heap[index];
        if (position(item) >= targetPosition)
            return;

        // If the children are at the same position, they also need advancing and their subheap
        // invariant to be restored.
        skipHeap(targetPosition, index * 2 + 1);
        skipHeap(targetPosition, index * 2 + 2);

        // Apply the action. This is done on the reverse direction to give the action a chance to form proper
        // subheaps and combine them on processing the parent.
        // Apply the operation, which should advance the position of the element.
        skipSource(item, targetPosition);

        // This method is called on the back path of the recursion. At this point the heaps at both children are
        // advanced and well-formed.
        // Place current node in its proper position.
        heapifyDown(item, index);
        // The heap rooted at index is now advanced and well-formed.
    }

    /**
     * Push the given state down in the heap from the given index until it finds its proper place among
     * the subheap rooted at that position.
     */
    private void heapifyDown(Source item, int index)
    {
        while (true)
        {
            int next = index * 2 + 1;
            if (next >= heap.length)
                break;
            // Select the smaller of the two children to push down to.
            if (next + 1 < heap.length && greaterCursor(heap[next], heap[next + 1]))
                ++next;
            // If the child is greater or equal, the invariant has been restored.
            if (!greaterCursor(item, heap[next]))
                break;
            heap[index] = heap[next];
            index = next;
        }
        heap[index] = item;
    }

    /**
     * Check if the head is greater than the top element in the heap, and if so, swap them and push down the new
     * top until its proper place.
     */
    private int maybeSwapHead()
    {
        int headPosition = position(head);
        int heap0Position = position(heap[0]);
        if (headPosition <= heap0Position)
            return headPosition;   // head is still smallest

        // otherwise we need to swap heap and heap[0]
        Source newHeap0 = head;
        head = heap[0];
        heapifyDown(newHeap0, 0);
        return heap0Position;
    }

    protected int advance() throws IOException
    {
        if (started)
        {
            advanceHeap(position(head), 0);
            advanceSource(head);
        }
        else
            initializeHeap();

        return maybeSwapHead();
    }

    private void initializeHeap()
    {
        for (int i = heap.length - 1; i >= 0; --i)
            heapifyDown(heap[i], i);
        started = true;
    }

    protected int skipTo(int targetPosition) throws IOException
    {
        // We need to advance all cursors that stand before the requested position.
        // If a child cursor does not need to advance as it is greater than the skip position, neither of the ones
        // below it in the heap hierarchy do as they can't have an earlier position.
        if (started)
            skipHeap(targetPosition, 0);
        else
            initializeSkipping(targetPosition);

        skipSource(head, targetPosition);
        return maybeSwapHead();
    }

    private void initializeSkipping(int targetPosition) throws IOException
    {
        for (int i = heap.length - 1; i >= 0; --i)
        {
            Source item = heap[i];
            skipSource(item, targetPosition);
            heapifyDown(item, i);
        }
        started = true;
    }

    protected void applyToAll(Consumer<Source> op)
    {
        for (int i = heap.length - 1; i >= 0; --i)
            op.accept(heap[i]);
        op.accept(head);
    }
}

