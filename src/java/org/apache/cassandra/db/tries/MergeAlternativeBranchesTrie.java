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
import java.util.List;

public class MergeAlternativeBranchesTrie<T> extends Trie<T>
{
    final private Trie<T> source;
    final private CollectionMergeResolver<T> resolver;
    final private boolean omitMain;

    MergeAlternativeBranchesTrie(Trie<T> source, CollectionMergeResolver<T> resolver, boolean omitMain)
    {
        super();
        this.source = source;
        this.resolver = resolver;
        this.omitMain = omitMain;
    }

    @Override
    protected Cursor<T> cursor()
    {
        Cursor<T> cursor = new MergeAlternativesCursor<>(resolver, source, omitMain);
        if (omitMain)
            cursor = DeadBranchRemoval.apply(cursor);
        return cursor;
    }

    /**
     * Compare the positions of two cursors. One is before the other when
     * - its depth is greater, or
     * - its depth is equal, and the incoming transition is smaller.
     */
    static <T> boolean greaterCursor(Cursor<T> c1, Cursor<T> c2)
    {
        int c1depth = c1.depth();
        int c2depth = c2.depth();
        if (c1depth != c2depth)
            return c1depth < c2depth;
        return c1.incomingTransition() > c2.incomingTransition();
    }

    static <T> boolean equalCursor(Cursor<T> c1, Cursor<T> c2)
    {
        return c1.depth() == c2.depth() && c1.incomingTransition() == c2.incomingTransition();
    }

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
     * its position will push it after all still-valid cursors.
     *
     * In the case where we have multiple sources with matching positions, the merging algorithm
     * must be able to merge all equal values. To achieve this {@code content} walks the heap to
     * find all equal cursors without advancing them, and separately {@code advance} advances
     * all equal sources and restores the heap structure.
     *
     * The latter is done equivalently to the process of initial construction of a min-heap using back-to-front
     * heapification as done in the classic heapsort algorithm. It only needs to heapify subheaps whose top item
     * is advanced (i.e. one whose position matches the current), and we can do that recursively from
     * bottom to top.
     *
     * To make it easier to advance efficienty in single-sourced branches of tries, we extract the current smallest
     * cursor (the head) separately, and start any advance with comparing that to the heap's first. When the smallest
     * cursor remains the same (e.g. in branches coming from a single source) this makes it possible to advance with
     * just one comparison instead of two at the expense of increasing the number by one in the general case.
     *
     * Note: This is a simplification of the MergeIterator code from CASSANDRA-8915, without the leading ordered
     * section and equalParent flag since comparisons of cursor positions are cheap.
     */
    static class MergeAlternativesCursor<T> implements Cursor<T>
    {
        private final CollectionMergeResolver<T> resolver;
        private final Cursor<T> cursorToOmit;

        /**
         * The smallest cursor, tracked separately to improve performance in single-source sections of the trie.
         */
        private Cursor<T> head;

        /**
         * Binary heap of the remaining cursors. The smallest element is at position 0.
         * Every element i is smaller than or equal to its two children, i.e.
         *     heap[i] <= heap[i*2 + 1] && heap[i] <= heap[i*2 + 2]
         */
        private final List<Cursor<T>> heap;

        /**
         * A list used to collect contents during content() calls.
         */
        private final List<T> contents;

        MergeAlternativesCursor(CollectionMergeResolver<T> resolver, Trie<T> source, boolean omitMain)
        {
            this.resolver = resolver;
            head = source.cursor();
            cursorToOmit = omitMain ? head : null;
            heap = new ArrayList<>();
            contents = new ArrayList<>();
            maybeSwapHeadAndEnterNode(head.depth());
        }

        MergeAlternativesCursor(MergeAlternativesCursor<T> copyFrom)
        {
            this.resolver = copyFrom.resolver;
            List<Cursor<T>> list = new ArrayList<>(copyFrom.heap.size());
            Cursor<T> toOmit = null;
            for (Cursor<T> tCursor : copyFrom.heap)
            {
                Cursor<T> duplicate = tCursor.duplicate();
                list.add(duplicate);
                if (tCursor == copyFrom.cursorToOmit)
                    toOmit = duplicate;
            }
            this.heap = list;
            this.contents = new ArrayList<>(copyFrom.contents.size()); // no need to copy contents
            this.head = copyFrom.head.duplicate();
            if (copyFrom.head == copyFrom.cursorToOmit)
                toOmit = this.head;
            this.cursorToOmit = toOmit;
        }

        /**
         * Interface for internal operations that can be applied to selected top elements of the heap.
         */
        interface HeapOp<T>
        {
            void apply(MergeAlternativesCursor<T> self, Cursor<T> cursor, int index);

            default boolean shouldContinueWithChild(Cursor<T> child, Cursor<T> head)
            {
                return equalCursor(child, head);
            }
        }

        /**
         * Apply a non-interfering operation, i.e. one that does not change the cursor state, to all inputs in the heap
         * that are on equal position to the head.
         * For interfering operations like advancing the cursors, use {@link #advanceEqualAndRestoreHeap(AdvancingHeapOp)}.
         */
        private void applyToEqualOnHeap(HeapOp<T> action)
        {
            applyToSelectedElementsInHeap(action, 0);
        }

        /**
         * Interface for internal advancing operations that can be applied to the heap cursors. This interface provides
         * the code to restore the heap structure after advancing the cursors.
         */
        interface AdvancingHeapOp<T> extends HeapOp<T>
        {
            void apply(Cursor<T> cursor);

            default void apply(MergeAlternativesCursor<T> self, Cursor<T> cursor, int index)
            {
                // Apply the operation, which should advance the position of the element.
                apply(cursor);

                // This method is called on the back path of the recursion. At this point the heaps at both children are
                // advanced and well-formed.
                // Place current node in its proper position.
                self.heapifyDown(cursor, index);
                // The heap rooted at index is now advanced and well-formed.
            }
        }


        /**
         * Advance the state of all inputs in the heap that are on equal position as the head and restore the heap
         * invariant.
         */
        private void advanceEqualAndRestoreHeap(AdvancingHeapOp<T> action)
        {
            applyToSelectedElementsInHeap(action, 0);
        }

        /**
         * Apply an operation to all elements on the heap that satisfy, recursively through the heap hierarchy, the
         * {@code shouldContinueWithChild} condition (being equal to the head by default). Descends recursively in the
         * heap structure to all selected children and applies the operation on the way back.
         * <p>
         * This operation can be something that does not change the cursor state (see {@link #content}) or an operation
         * that advances the cursor to a new state, wrapped in a {@link AdvancingHeapOp} ({@link #advance} or
         * {@link #skipTo}). The latter interface takes care of pushing elements down in the heap after advancing
         * and restores the subheap state on return from each level of the recursion.
         */
        private void applyToSelectedElementsInHeap(HeapOp<T> action, int index)
        {
            if (index >= heap.size())
                return;
            Cursor<T> item = heap.get(index);
            if (!action.shouldContinueWithChild(item, head))
                return;

            // If the children are at the same position, they also need advancing and their subheap
            // invariant to be restored.
            applyToSelectedElementsInHeap(action, index * 2 + 1);
            applyToSelectedElementsInHeap(action, index * 2 + 2);

            // Apply the action. This is done on the reverse direction to give the action a chance to form proper
            // subheaps and combine them on processing the parent.
            action.apply(this, item, index);
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
                if (next >= heap.size())
                    break;
                // Select the smaller of the two children to push down to.
                if (next + 1 < heap.size() && greaterCursor(heap.get(next), heap.get(next + 1)))
                    ++next;
                // If the child is greater or equal, the invariant has been restored.
                if (!greaterCursor(item, heap.get(next)))
                    break;
                heap.set(index, heap.get(next));
                index = next;
            }
            heap.set(index, item);
        }

        /**
         * Pull the given state up in the heap from the given index until it finds its proper place.
         */
        private void heapifyUp(Cursor<T> item, int index)
        {
            while (index > 0)
            {
                int parent = (index - 1) / 2;
                if (!greaterCursor(heap.get(parent), item))
                    break;
                heap.set(index, heap.get(parent));
                index = parent;
            }
            heap.set(index, item);
        }

        /**
         * Check if the head is greater than the top element in the heap, and if so, swap them and push down the new
         * top until its proper place.
         * @param headDepth the depth of the head cursor (as returned by e.g. advance).
         * @return the new head element's depth
         */
        private int maybeSwapHead(int headDepth)
        {
            if (heap.isEmpty())
                return headDepth;

            int heap0Depth = heap.get(0).depth();
            if (headDepth > heap0Depth ||
                (headDepth == heap0Depth && head.incomingTransition() <= heap.get(0).incomingTransition()))
                return headDepth;   // head is still smallest

            // otherwise we need to swap heap and heap.get(0)
            Cursor<T> newHeap0 = head;
            head = heap.get(0);
            heapifyDown(newHeap0, 0);
            return heap0Depth;
        }

        private int maybeSwapHeadAndEnterNode(int headDepth)
        {
            headDepth = maybeSwapHead(headDepth);
            if (headDepth < 0)
                return headDepth;   // we are done

            int sizeBeforeAlternatives = removeTrailingDoneCursors();
            addAlternative(head, -1);
            applyToEqualOnHeap(MergeAlternativesCursor::addAlternative);
            for (int i = sizeBeforeAlternatives; i < heap.size(); ++i)
                heapifyUp(heap.get(i), i);
            return headDepth;
        }

        private int removeTrailingDoneCursors()
        {
            for (int i = heap.size() - 1; i >= 0; --i)
            {
                Cursor<T> cursor = heap.get(i);
                if (cursor.depth() != -1)
                    return i + 1;
                heap.remove(i);
            }
            return 0;
        }

        private void addAlternative(Cursor<T> cursor, int index)
        {
            Cursor<T> alternative = cursor.alternateBranch();
            while (alternative != null)
            {
                assert equalCursor(head, alternative);
                heap.add(alternative);
                alternative = alternative.alternateBranch();
            }
        }

        @Override
        public int advance()
        {
            advanceEqualAndRestoreHeap(Cursor::advance);
            return maybeSwapHeadAndEnterNode(head.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            // If the current position is present in just one cursor, we can safely descend multiple levels within
            // its branch as no one of the other tries has content for it.
            if (!heap.isEmpty() && equalCursor(heap.get(0), head))
                return advance();   // More than one source at current position, do single-step advance.

            // If there are no children, i.e. the cursor ascends, we have to check if it's become larger than some
            // other candidate.
            return maybeSwapHeadAndEnterNode(head.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            // We need to advance all cursors that stand before the requested position.
            // If a child cursor does not need to advance as it is greater than the skip position, neither of the ones
            // below it in the heap hierarchy do as they can't have an earlier position.
            class SkipTo implements AdvancingHeapOp<T>
            {
                @Override
                public boolean shouldContinueWithChild(Cursor<T> child, Cursor<T> head)
                {
                    // When the requested position descends, the implicit prefix bytes are those of the head cursor,
                    // and thus we need to check against that if it is a match.
                    if (equalCursor(child, head))
                        return true;
                    // Otherwise we can compare the child's position against a cursor advanced as requested, and need
                    // to skip only if it would be before it.
                    int childDepth = child.depth();
                    return childDepth > skipDepth ||
                           childDepth == skipDepth && child.incomingTransition() < skipTransition;
                }

                @Override
                public void apply(Cursor<T> cursor)
                {
                    cursor.skipTo(skipDepth, skipTransition);
                }
            }

            applyToSelectedElementsInHeap(new SkipTo(), 0);
            return maybeSwapHeadAndEnterNode(head.skipTo(skipDepth, skipTransition));
        }

        @Override
        public int depth()
        {
            return head.depth();
        }

        @Override
        public int incomingTransition()
        {
            return head.incomingTransition();
        }

        @Override
        public T content()
        {
            applyToEqualOnHeap(MergeAlternativesCursor::collectContent);
            collectContent(head, -1);

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

        private void collectContent(Cursor<T> item, int index)
        {
            if (item == cursorToOmit)
                return;

            T itemContent = item.content();
            if (itemContent != null)
                contents.add(itemContent);
        }

        @Override
        public Cursor<T> alternateBranch()
        {
            return null;
        }

        @Override
        public Cursor<T> duplicate()
        {
            return new MergeAlternativesCursor<>(this);
        }
    }
}
