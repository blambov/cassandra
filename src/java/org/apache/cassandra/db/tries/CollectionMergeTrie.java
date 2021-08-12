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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import com.google.common.collect.Iterables;

/**
 * A merged view of multiple tries.
 *
 * Note: We use same input and output types to be able to switch to directly returning single-origin branches.
 */
class CollectionMergeTrie<T> extends Trie<T>
{
    private final CollectionMergeResolver<T> resolver;  // only called on more than one input
    protected final Collection<? extends Trie<T>> inputs;

    CollectionMergeTrie(Collection<? extends Trie<T>> inputs, CollectionMergeResolver<T> resolver)
    {
        this.resolver = resolver;
        this.inputs = inputs;
    }

    @Override
    protected Cursor<T> cursor()
    {
        return new CollectionMergeCursor<>(resolver, inputs);
    }

    static <T> boolean greaterCursor(Cursor<T> c1, Cursor<T> c2)
    {
        int c1level = c1.level();
        int c2level = c2.level();
        if (c1level != c2level)
            return c1level < c2level;
        return c1.incomingTransition() > c2.incomingTransition();
    }

    static <T> boolean equalCursor(Cursor<T> c1, Cursor<T> c2)
    {
        return c1.level() == c2.level() && c1.incomingTransition() == c2.incomingTransition();
    }

    static class CollectionMergeCursor<T> implements Cursor<T>
    {
        private final CollectionMergeResolver<T> resolver;
        private final Cursor<T>[] heap;
        private Cursor<T> head;
        private final List<T> contents;

        public CollectionMergeCursor(CollectionMergeResolver<T> resolver, Collection<? extends Trie<T>> inputs)
        {
            this.resolver = resolver;
            int count = inputs.size();
            heap = new Cursor[count - 1];
            contents = new ArrayList<>(count);
            int i = -1;
            --count;
            for (Trie<T> trie : inputs)
            {
                Cursor<T> cursor = trie.cursor();
                if (cursor.level() < 0 && count > 0)
                    heap[--count] = cursor;    // empty trie / no root, put it at the end
                else if (i >= 0)
                    heap[i++] = cursor;
                else
                {
                    head = cursor;
                    ++i;
                }
            }
        }

        @Override
        public int advance()
        {
            advance(0);
            return maybeSwapHead(head.advance());
        }

        /**
         * Advance the state of the input at the given index and any of its descendants that are at the same
         * transition byte and restore the heap invariant for the subtree rooted at the given index.
         * Calls itself recursively and used by advance with index = 0 to advance the state of the merge.
         */
        private void advance(int index)
        {
            if (index >= heap.length)
                return;
            Cursor<T> item = heap[index];
            if (!equalCursor(item, head))
                return;

            // If the children are at the same transition byte, they also need advancing and their subheap
            // invariant to be restored.
            advance(index * 2 + 1);
            advance(index * 2 + 2);

            item.advance();

            // At this point the heaps at both children are advanced and well-formed. Place current node in its
            // proper position.
            heapifyDown(item, index);
            // The heap rooted at index is now advanced and well-formed.
        }

        /**
         * Push the given state down in the heap from the given index until it finds its proper place among
         * the subheap rooted at that position.
         */
        private void heapifyDown(Cursor<T> item, int index)
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

        private int maybeSwapHead(int headLevel)
        {
            int heap0Level = heap[0].level();
            if (headLevel > heap0Level ||
                (headLevel == heap0Level && head.incomingTransition() <= heap[0].incomingTransition()))
                return headLevel;
            // otherwise we need to swap heap and heap[0]
            Cursor<T> newHeap0 = head;
            head = heap[0];
            heapifyDown(newHeap0, 0);
            return heap0Level;
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            if (equalCursor(heap[0], head))
                return advance();   // more than one source at current position, can't do multiple.

            return maybeSwapHead(head.advanceMultiple(receiver));
        }

        @Override
        public int ascend()
        {
            ascend(0);
            return maybeSwapHead(head.ascend());
        }

        private void ascend(int index)
        {
            if (index >= heap.length)
                return;
            Cursor<T> item = heap[index];
            if (head.level() != item.level())
                return;

            ascend(index * 2 + 1);
            ascend(index * 2 + 2);

            item.ascend();
            heapifyDown(item, index);
        }

        @Override
        public int level()
        {
            return head.level();
        }

        @Override
        public int incomingTransition()
        {
            return head.incomingTransition();
        }

        @Override
        public T content()
        {
            T itemContent = head.content();
            if (itemContent != null)
                contents.add(itemContent);

            collectContent(0);
            T toReturn;
            switch (contents.size())
            {
                case 0:
                    toReturn = null;
                    break;
                case 1:
                    toReturn = contents.get(0);
                    break;
                default:
                    toReturn = resolver.resolve(contents);
                    break;
            }
            contents.clear();
            return toReturn;
        }

        private void collectContent(int index)
        {
            if (index >= heap.length)
                return;
            Cursor<T> item = heap[index];
            if (!equalCursor(item, head))
                return;

            T itemContent = item.content();
            if (itemContent != null)
                contents.add(itemContent);

            collectContent(index * 2 + 1);
            collectContent(index * 2 + 2);
        }
    }

    /**
     * Special instance for sources that are guaranteed distinct. The main difference is that we can form unordered
     * value list by concatenating sources.
     */
    static class Distinct<T> extends CollectionMergeTrie<T>
    {
        Distinct(Collection<? extends Trie<T>> inputs)
        {
            super(inputs, throwingResolver());
        }

        @Override
        public Iterable<T> valuesUnordered()
        {
            return Iterables.concat(Iterables.transform(inputs, Trie::valuesUnordered));
        }
    }
}
