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

    public <L> Node<T, L> root()
    {
        List<Node<T, L>> nodes = new ArrayList<>(inputs.size());
        for (Trie<T> input : inputs)
        {
            Node<T, L> root = input.root();
            if (root != null)
                nodes.add(root);
        }
        return makeMerge(resolver, nodes);
    }

    @Override
    protected Cursor<T> cursor()
    {
        return new CollectionMergeCursor<>(resolver, inputs);
    }

    private static <T, L> Node<T, L> makeMerge(CollectionMergeResolver<T> resolver, List<Node<T, L>> nodes)
    {
        switch (nodes.size())
        {
        case 0:
            return null;
        case 1:
            return nodes.get(0);
        case 2:
            return new MergeTrie.MergeNode<>(resolver, nodes.get(0), nodes.get(1));
        default:
            return new MergeNode<>(resolver, nodes);
        }
    }

    static class MergeNode<T, L> extends Node<T, L>
    {
        private final CollectionMergeResolver<T> resolver;  // only called on more than one input
        final List<Node<T, L>> nodes;
        T content;
        volatile boolean contentMerged = false;

        MergeNode(CollectionMergeResolver<T> resolver, List<Node<T, L>> nodes)
        {
            // All children necessarily use the same parent link (given during getCurrentChild). Make that ours.
            super(nodes.get(0).parentLink);
            this.resolver = resolver;
            this.nodes = nodes;
        }

        /*
         * The merge node is effectively a merge iterator of children.
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
         * of the heap) and push it down to find its place for its new transition character. Should this source
         * be exhausted, we swap it with the last source in the heap and proceed by pushing that down in the
         * heap.
         *
         * In the case where we have multiple sources with matching transition characters, the merging algorithm
         * must be able to merge all equal values. To achieve this {@code getCurrentChild} walks the heap to
         * find all equal items without advancing the sources, and separately {@code advanceIteration} advances
         * all equal sources and restores the heap structure.
         *
         * The latter is done equivalently to the process of building the initial heap in {@code startIteration}
         * using back-to-front heapification as done in the classic heapsort algorithm. It only needs to heapify
         * subheaps whose top item is advanced (i.e. one whose transition character matches the current),
         * and we can do that recursively from bottom to top. Should a source be exhausted when advancing, it can
         * be thrown away by swapping in the last source in the heap (note: we must be careful to advance that
         * source too if required).
         *
         * Note: This is a simplification of the MergeIterator code from CASSANDRA-8915, without the leading ordered
         * section and equalParent flag since comparisons of transition characters are cheap.
         */

        public Remaining startIteration()
        {
            int count = nodes.size();
            // Get every input's initial state and move nodes with no children at the end.
            for (int i = 0; i < count; ++i)
            {
                Node<T, L> ni = nodes.get(i);
                boolean sHas = ni.startIteration() != null;
                if (!sHas)
                {
                    --count;
                    // put last one at its place (will do nothing if count now equals i)
                    nodes.set(i, nodes.get(count));
                    nodes.remove(count);
                    // make sure the moved input is processed
                    --i;
                }
            }
            // We now create a heap from the input states we got. This process has linear complexity in the number
            // of input states (see heapsort algorithm).
            while (--count >= 0)
                heapifyDown(count);

            if (nodes.isEmpty())
                return null;
            currentTransition = nodes.get(0).currentTransition;
            return Remaining.MULTIPLE;
        }

        public Remaining advanceIteration()
        {
            int current = currentTransition;
            advance(current, 0);

            if (nodes.isEmpty())
                return null;
            currentTransition = nodes.get(0).currentTransition;
            return Remaining.MULTIPLE;
        }

        public Node<T, L> getCurrentChild(L parent)
        {
            int current = currentTransition;
            List<Node<T, L>> children = new ArrayList<>(nodes.size());
            collectEqual(0, current, parent, children);
            return makeMerge(resolver, children);
        }

        /**
         * Gets the child for every input in the heap rooted at the given index that is at the given transition.
         * Calls itself recursively and used by getCurrentChild with index = 0 and transition = state[0].transition
         * to get the child for all inputs that are positioned at the current minimal transition.
         */
        void collectEqual(int index, int transition, L parent, List<Node<T, L>> list)
        {
            Node<T, L> child = nodes.get(index).getCurrentChild(parent);
            if (child != null)
                list.add(child);

            // Check if any of the children in the heap are at the same transition.
            // If so, collect children recursively.
            int next = index * 2 + 1;
            if (next < nodes.size() && nodes.get(next).currentTransition == transition)
                collectEqual(next, transition, parent, list);
            ++next;
            if (next < nodes.size() && nodes.get(next).currentTransition == transition)
                collectEqual(next, transition, parent, list);
        }

        /**
         * Advance the state of the input at the given index and any of its descendants that are at the same
         * transition byte and restore the heap invariant for the subtree rooted at the given index.
         * Calls itself recursively and used by advanceState with index = 0 and transition = state[0].transition
         * to advance the state of the merge.
         */
        private void advance(int transition, int index)
        {
            Node<T, L> n = nodes.get(index);
            // Advance current node and remove it from active heap if it has no further children.
            while (n.advanceIteration() == null)
            {
                // n has no further children, it needs to be removed from the active heap.
                // Move the last to index'th position and continue processing with that node.
                int nodeCount = nodes.size() - 1;
                n = nodes.remove(nodeCount);
                if (nodeCount == index)
                    return; // done, n was at the end of the heap so the subheap to advance is now empty thus
                            // the invariant is trivially true

                nodes.set(index, n);
                // The node we swapped in may also need advancing. If so, repeat the procedure above.
                if (n.currentTransition > transition)
                    break;
            }

            // If the children are at the same transition byte, they also need advancing and their subheap
            // invariant to be restored.
            int next = index * 2 + 1;
            if (next < nodes.size() && nodes.get(next).currentTransition == transition)
                advance(transition, next);
            ++next;
            if (next < nodes.size() && nodes.get(next).currentTransition == transition)
                advance(transition, next);

            // At this point the heaps at both children are advanced and well-formed. Place current node in its
            // proper position.
            heapifyDown(index);
            // The heap rooted at index is now advanced and well-formed.
        }

        /**
         * Push the given state down in the heap from the given index until it finds its proper place among
         * the subheap rooted at that position.
         */
        private void heapifyDown(int index)
        {
            Node<T, L> node = nodes.get(index);

            int transition = node.currentTransition;
            while (true)
            {
                int next = index * 2 + 1;
                if (next >= nodes.size())
                    break;
                // Select the smaller of the two children to push down to.
                if (next + 1 < nodes.size() && nodes.get(next).currentTransition > nodes.get(next + 1).currentTransition)
                    ++next;
                // If the child is greater, the invariant has been restored.
                if (nodes.get(next).currentTransition >= transition)
                    break;
                nodes.set(index, nodes.get(next));
                index = next;
            }
            nodes.set(index, node);
        }

        public T content()
        {
            if (!contentMerged)
            {
                // If we only have input from zero or one source, we will keep it here, avoiding the allocation
                // of the list until necessary.
                T v = null;
                Collection<T> values = null;
                for (Node<T, L> n : nodes)
                {
                    T c = n.content();
                    if (c == null)
                        continue;
                    if (v == null)
                    {
                        v = c;  // one element
                        continue;
                    }
                    if (values == null)
                    {
                        // more than one
                        values = new ArrayList<>();
                        values.add(v);
                    }
                    values.add(c);
                }

                if (values == null)
                    content = v;
                else
                    content = resolver.resolve(values);

                // Save content to avoid doing this costly operation again.
                contentMerged = true;
            }
            return content;
        }
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
