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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

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
abstract class CollectionMergeCursor<C extends CursorWalkable.Cursor> implements CursorWalkable.Cursor
{
    /**
     * The smallest cursor, tracked separately to improve performance in single-source sections of the trie.
     */
    C head;

    /**
     * Binary heap of the remaining cursors. The smallest element is at position 0.
     * Every element i is smaller than or equal to its two children, i.e.
     * heap[i] <= heap[i*2 + 1] && heap[i] <= heap[i*2 + 2]
     */
    private final C[] heap;

    @SuppressWarnings("unchecked")
    <L> CollectionMergeCursor(Collection<L> inputs, Function<L, C> getter, Class<? super C> cursorClass)
    {
        int count = inputs.size();
        Iterator<L> it = inputs.iterator();
        if (!it.hasNext())
            throw new IllegalArgumentException("No inputs");
        head = getter.apply(it.next());

        // Get cursors for all inputs. Put one of them in head and the rest in the heap.
        heap = (C[]) Array.newInstance(cursorClass, count - 1);
        int i = 0;
        while (it.hasNext())
            heap[i++] = getter.apply(it.next());

        // The cursors are all currently positioned on the root and thus in valid heap order.
    }

    @SuppressWarnings("unchecked")
    CollectionMergeCursor(CollectionMergeCursor<C> copyFrom)
    {
        this.head = (C) copyFrom.head.duplicate();
        C[] list = (C[]) Array.newInstance(copyFrom.heap.getClass().getComponentType(), copyFrom.heap.length);
        for (int i = 0; i < list.length; ++i)
            list[i] = (C) copyFrom.heap[i].duplicate();
        this.heap = list;
    }

    /**
     * Compare the positions of two cursors. One is before the other when
     * - its depth is greater, or
     * - its depth is equal, and the incoming transition is smaller.
     */
    static <C extends CursorWalkable.Cursor> boolean greaterCursor(C c1, C c2)
    {
        int c1depth = c1.depth();
        int c2depth = c2.depth();
        if (c1depth != c2depth)
            return c1depth < c2depth;
        return c1.incomingTransition() > c2.incomingTransition();
    }

    static <C extends CursorWalkable.Cursor> boolean equalCursor(C c1, C c2)
    {
        return c1.depth() == c2.depth() && c1.incomingTransition() == c2.incomingTransition();
    }

    /**
     * Interface for internal operations that can be applied to selected top elements of the heap.
     */
    interface HeapOp<C extends CursorWalkable.Cursor, D>
    {
        void apply(C cursor, int index, D datum);

        default boolean shouldContinueWithChild(C child, C head)
        {
            return equalCursor(child, head);
        }
    }

    /**
     * Apply a non-interfering operation, i.e. one that does not change the cursor state, to all inputs in the heap
     * that are on equal position to the head.
     * For interfering operations like advancing the cursors, use {@link #advanceEqualAndRestoreHeap(AdvancingHeapOp)}.
     */
    <D> void applyToEqualOnHeap(HeapOp<C, D> action, D datum)
    {
        applyToSelectedElementsInHeap(action, 0, datum);
    }
    
    <D> void applyToAllOnHeap(HeapOp<C, D> action, D datum)
    {
        for (int i = 0; i < heap.length; i++)
            action.apply(heap[i], i, datum);
    }

    /**
     * Interface for internal advancing operations that can be applied to the heap cursors. This interface provides
     * the code to restore the heap structure after advancing the cursors.
     */
    interface AdvancingHeapOp<C extends CursorWalkable.Cursor> extends HeapOp<C, CollectionMergeCursor<C>>
    {
        void apply(C cursor);

        default void apply(C cursor, int index, CollectionMergeCursor<C> self)
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
    private void advanceEqualAndRestoreHeap(AdvancingHeapOp<C> action)
    {
        applyToSelectedElementsInHeap(action, 0, this);
    }

    /**
     * Apply an operation to all elements on the heap that satisfy, recursively through the heap hierarchy, the
     * {@code shouldContinueWithChild} condition (being equal to the head by default). Descends recursively in the
     * heap structure to all selected children and applies the operation on the way back.
     * <p>
     * This operation can be something that does not change the cursor state (see {@link WithContent#content}) or an
     * operation that advances the cursor to a new state, wrapped in a {@link AdvancingHeapOp} ({@link #advance} or
     * {@link #skipTo}). The latter interface takes care of pushing elements down in the heap after advancing
     * and restores the subheap state on return from each level of the recursion.
     */
    private <D> void applyToSelectedElementsInHeap(HeapOp<C, D> action, int index, D datum)
    {
        if (index >= heap.length)
            return;
        C item = heap[index];
        if (!action.shouldContinueWithChild(item, head))
            return;

        // If the children are at the same position, they also need advancing and their subheap
        // invariant to be restored.
        applyToSelectedElementsInHeap(action, index * 2 + 1, datum);
        applyToSelectedElementsInHeap(action, index * 2 + 2, datum);

        // Apply the action. This is done on the reverse direction to give the action a chance to form proper
        // subheaps and combine them on processing the parent.
        action.apply(item, index, datum);
    }

    /**
     * Push the given state down in the heap from the given index until it finds its proper place among
     * the subheap rooted at that position.
     */
    private void heapifyDown(C item, int index)
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
     *
     * @param headDepth the depth of the head cursor (as returned by e.g. advance).
     * @return the new head element's depth
     */
    private int maybeSwapHead(int headDepth)
    {
        int heap0Depth = heap[0].depth();
        if (headDepth > heap0Depth ||
            (headDepth == heap0Depth && head.incomingTransition() <= heap[0].incomingTransition()))
            return headDepth;   // head is still smallest

        // otherwise we need to swap heap and heap[0]
        C newHeap0 = head;
        head = heap[0];
        heapifyDown(newHeap0, 0);
        return heap0Depth;
    }

    @Override
    public int advance()
    {
        return doAdvance();
    }

    private int doAdvance()
    {
        advanceEqualAndRestoreHeap(CursorWalkable.Cursor::advance);
        return maybeSwapHead(head.advance());
    }

    @Override
    public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
    {
        // If the current position is present in just one cursor, we can safely descend multiple levels within
        // its branch as no one of the other tries has content for it.
        if (branchHasMultipleSources())
            return doAdvance();   // More than one source at current position, do single-step advance.

        // If there are no children, i.e. the cursor ascends, we have to check if it's become larger than some
        // other candidate.
        return maybeSwapHead(head.advanceMultiple(receiver));
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        // We need to advance all cursors that stand before the requested position.
        // If a child cursor does not need to advance as it is greater than the skip position, neither of the ones
        // below it in the heap hierarchy do as they can't have an earlier position.
        class SkipTo implements AdvancingHeapOp<C>
        {
            @Override
            public boolean shouldContinueWithChild(C child, C head)
            {
                // When the requested position descends, the inplicit prefix bytes are those of the head cursor,
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
            public void apply(C cursor)
            {
                cursor.skipTo(skipDepth, skipTransition);
            }
        }

        advanceEqualAndRestoreHeap(new SkipTo());
        return maybeSwapHead(head.skipTo(skipDepth, skipTransition));
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

    boolean branchHasMultipleSources()
    {
        return equalCursor(heap[0], head);
    }

    boolean isExhausted()
    {
        return head.depth() < 0;
    }

    boolean addCursor(C cursor)
    {
        if (isExhausted())
        {
            // easy case
            head = cursor;
            return true;
        }

        int index;
        for (index = 0; index < heap.length; ++index)
            if (heap[index].depth() < 0)
                break;
        if (index == heap.length)
            return false;

        heapifyUp(cursor, index);
        return true;
    }

    /**
     * Push the given state down in the heap from the given index until it finds its proper place among
     * the subheap rooted at that position.
     */
    private void heapifyUp(C item, int index)
    {
        while (true)
        {
            if (index == 0)
            {
                if (greaterCursor(head, item))
                {
                    heap[0] = head;
                    head = item;
                    return;
                }
                else
                    break;
            }
            int prev = (index - 1) / 2;

            // If the parent is lesser or equal, the invariant has been restored.
            if (!greaterCursor(heap[prev], item))
                break;
            heap[index] = heap[prev];
            index = prev;
        }
        heap[index] = item;
    }

    static abstract class WithContent<T, C extends TrieImpl.Cursor<T>> extends CollectionMergeCursor<C> implements TrieImpl.Cursor<T>
    {
        final Trie.CollectionMergeResolver<T> resolver;

        /**
         * A list used to collect contents during content() calls.
         */
        final List<T> contents;
        T collectedContent;
        boolean contentCollected;

        <L> WithContent(Trie.CollectionMergeResolver<T> resolver, Collection<L> inputs, Function<L, C> getter, Class<? super C> cursorClass)
        {
            super(inputs, getter, cursorClass);
            this.resolver = resolver;
            contents = new ArrayList<>(inputs.size());
        }

        WithContent(WithContent<T, C> copyFrom)
        {
            super(copyFrom);
            this.resolver = copyFrom.resolver;
            this.contents = new ArrayList<>(copyFrom.contents.size()); // no need to copy
            this.contentCollected = copyFrom.contentCollected;
            this.collectedContent = copyFrom.collectedContent;
        }

        @Override
        public int advance()
        {
            contentCollected = false;
            return super.advance();
        }

        @Override
        public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
        {
            contentCollected = false;
            return super.advanceMultiple(receiver);
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            contentCollected = false;
            return super.skipTo(skipDepth, skipTransition);
        }

        @Override
        public T content()
        {
            return maybeCollectContent();
        }

        T maybeCollectContent()
        {
            if (!contentCollected)
            {
                collectedContent = isExhausted() ? null : collectContent();
                contentCollected = true;
            }
            return collectedContent;
        }

        T collectContent()
        {
            applyToEqualOnHeap(WithContent::collectContent, contents);
            collectContent(head, -1, contents);
            return resolveContent();
        }

        T resolveContent()
        {
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

        private static <T, C extends TrieImpl.Cursor<T>>
        void collectContent(C item, int index, List<T> contentsList)
        {
            T itemContent = item.content();
            if (itemContent != null)
                contentsList.add(itemContent);
        }
    }

    static class Deterministic<T> extends WithContent<T, TrieImpl.Cursor<T>> implements TrieImpl.Cursor<T>
    {

        <L> Deterministic(Trie.CollectionMergeResolver<T> resolver, Collection<L> inputs, Function<L, TrieImpl.Cursor<T>> getter)
        {
            super(resolver, inputs, getter, TrieImpl.Cursor.class);
        }

        Deterministic(Trie.CollectionMergeResolver<T> resolver, Collection<? extends Trie<T>> inputs)
        {
            this(resolver, inputs, trie -> TrieImpl.impl(trie).cursor());
        }

        Deterministic(WithContent<T, TrieImpl.Cursor<T>> copyFrom)
        {
            super(copyFrom);
        }


        @Override
        public Deterministic<T> duplicate()
        {
            return new Deterministic<>(this);
        }
    }


    static class NonDeterministic<T extends NonDeterministicTrie.Mergeable<T>>
    extends WithContent<T, NonDeterministicTrieImpl.Cursor<T>>
    implements NonDeterministicTrieImpl.Cursor<T>
    {
        <L> NonDeterministic(Collection<L> inputs, Function<L, NonDeterministicTrieImpl.Cursor<T>> getter)
        {
            super(NonDeterministic::resolve, inputs, getter, NonDeterministicTrieImpl.Cursor.class);
        }

        NonDeterministic(Collection<? extends NonDeterministicTrie<T>> inputs)
        {
            this(inputs, trie -> NonDeterministicTrieImpl.impl(trie).cursor());
        }

        NonDeterministic(WithContent<T, NonDeterministicTrieImpl.Cursor<T>> copyFrom)
        {
            super(copyFrom);
        }

        static <T extends NonDeterministicTrie.Mergeable<T>> T resolve(Collection<T> contents)
        {
            Iterator<T> it = contents.iterator();
            assert it.hasNext();
            T first = it.next();
            while (it.hasNext())
                first = first.mergeWith(it.next());
            return first;
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> alternateBranch()
        {
            class Collector implements HeapOp<NonDeterministicTrieImpl.Cursor<T>, Void>
            {
                NonDeterministicTrieImpl.Cursor<T> first = null;
                List<NonDeterministicTrieImpl.Cursor<T>> list = null;

                @Override
                public void apply(NonDeterministicTrieImpl.Cursor<T> cursor, int index, Void datum)
                {
                    NonDeterministicTrieImpl.Cursor<T> alternate = cursor.alternateBranch();
                    if (alternate != null)
                    {
                        if (first == null)
                            first = alternate;
                        else
                        {
                            if (list == null)
                            {
                                list = new ArrayList<>();
                                list.add(first);
                            }
                            list.add(alternate);
                        }
                    }
                }
            }
            var collector = new Collector();

            collector.apply(head, -1, null);
            applyToEqualOnHeap(collector, null);
            if (collector.first == null)
                return null;

            if (collector.list == null)
                return collector.first;

            return new NonDeterministic<>(collector.list, x -> x);
        }

        @Override
        public NonDeterministic<T> duplicate()
        {
            return new NonDeterministic<>(this);
        }
    }
    
    
    static class Range<M extends RangeTrie.RangeMarker<M>> extends WithContent<M, RangeTrieImpl.Cursor<M>> implements RangeTrieImpl.Cursor<M>
    {
        <L> Range(Trie.CollectionMergeResolver<M> resolver, Collection<L> inputs, Function<L, RangeTrieImpl.Cursor<M>> getter)
        {
            super(resolver, inputs, getter, RangeTrieImpl.Cursor.class);
        }

        Range(Trie.CollectionMergeResolver<M> resolver, Collection<? extends RangeTrie<M>> inputs)
        {
            this(resolver, inputs, trie -> RangeTrieImpl.impl(trie).cursor());
        }

        Range(WithContent<M, RangeTrieImpl.Cursor<M>> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public Range<M> duplicate()
        {
            return new Range<>(this);
        }

        static <M extends RangeTrie.RangeMarker<M>> M getState(RangeTrieImpl.Cursor<M> item)
        {
            M itemState = item.content();
            if (itemState == null)
                itemState = item.coveringState();
            return itemState;
        }

        @Override
        M collectContent()
        {
            applyToAllOnHeap(Range::collectState, this);
            M headState = getState(head);
            if (headState != null)
                contents.add(headState);

            return resolveContent();
        }

        private static <M extends RangeTrie.RangeMarker<M>, C extends RangeTrieImpl.Cursor<M>>
        void collectState(C item, int index, Range<M> self)
        {
            M itemState = equalCursor(item, self.head) ? getState(item) : item.coveringState();
            if (itemState != null)
                self.contents.add(itemState);
        }

        @Override
        public M coveringState()
        {
            final M state = maybeCollectContent();
            return state != null ? state.leftSideAsCovering() : null;
        }

        @Override
        public M content()
        {
            final M state = maybeCollectContent();
            return state != null ? state.toContent() : null;
        }
    }

    static class DeletionAware<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends CollectionMergeCursor.WithContent<T, DeletionAwareTrieImpl.Cursor<T, D>> implements DeletionAwareTrieImpl.Cursor<T, D>
    {
        final BiFunction<D, T, T> deleter;
        final Trie.CollectionMergeResolver<D> deletionResolver;
        final Range<D> relevantDeletions;
        int deletionBranchDepth = -1;

        enum DeletionState
        {
            NONE,
            MATCHING,
            AHEAD
        }
        DeletionState relevantDeletionsState = DeletionState.NONE;

        <L> DeletionAware(Trie.CollectionMergeResolver<T> mergeResolver,
                          Trie.CollectionMergeResolver<D> deletionResolver,
                          BiFunction<D, T, T> deleter,
                          Collection<L> inputs,
                          Function<L, DeletionAwareTrieImpl.Cursor<T, D>> getter)
        {
            super(mergeResolver,
                  inputs,
                  getter,
                  DeletionAwareTrieImpl.Cursor.class);
            // We will add deletion sources to the above as we find them.
            this.deletionResolver = deletionResolver;
            this.deleter = deleter;
            // Make a flexible merger for the deletion branches
            relevantDeletions = new Range<D>(deletionResolver,
                                             Collections.<RangeTrieImpl.Cursor<D>>nCopies(inputs.size(),
                                                                                          RangeTrieImpl.done()),
                                             x -> x);
            maybeAddDeletionsBranch(this.depth());
        }

        DeletionAware(Trie.CollectionMergeResolver<T> mergeResolver,
                      Trie.CollectionMergeResolver<D> deletionResolver,
                      BiFunction<D, T, T> deleter,
                      Collection<? extends DeletionAwareTrie<T, D>> inputs)
        {
            this(mergeResolver, deletionResolver, deleter, inputs, trie -> DeletionAwareTrieImpl.impl(trie).cursor());
        }

        public DeletionAware(DeletionAware<T, D> copyFrom)
        {
            super(copyFrom);
            this.deleter = copyFrom.deleter;
            this.deletionResolver = copyFrom.deletionResolver;
            this.deletionBranchDepth = copyFrom.deletionBranchDepth;
            this.relevantDeletions = copyFrom.relevantDeletions.duplicate();
        }

        @Override
        public int advance()
        {
            return maybeAddDeletionsBranch(super.advance());
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return maybeAddDeletionsBranch(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
        {
            if (relevantDeletionsState == DeletionState.MATCHING)
                return advance();
            return maybeAddDeletionsBranch(super.advanceMultiple(receiver));
        }

        void adjustDeletionState(int deletionDepth, int contentDepth, int contentTransition)
        {
            if (deletionDepth < 0)
                relevantDeletionsState = DeletionState.NONE;
            else if (deletionDepth < contentDepth)
                relevantDeletionsState = DeletionState.AHEAD;
            else if (contentTransition < relevantDeletions.incomingTransition())
                relevantDeletionsState = DeletionState.AHEAD;
            else
                relevantDeletionsState = DeletionState.MATCHING;
        }

        int maybeAddDeletionsBranch(int depth)
        {
            int contentTransition = incomingTransition();
            int deletionDepth;
            switch (relevantDeletionsState)
            {
                case MATCHING:
                    deletionDepth = relevantDeletions.skipTo(depth, contentTransition);
                    break;
                case AHEAD:
                    deletionDepth = relevantDeletions.maybeSkipTo(depth, contentTransition);
                    break;
                default:
                    deletionDepth = -1;
                    break;
            }

            if (depth <= deletionBranchDepth)   // ascending above common deletions root
            {
                deletionBranchDepth = -1;
                assert deletionDepth < 0;
            }

            if (branchHasMultipleSources())
            {
                maybeAddDeletionsBranch(head, 0, relevantDeletions);
                applyToEqualOnHeap(DeletionAware::maybeAddDeletionsBranch, relevantDeletions);
                deletionDepth = relevantDeletions.depth();  // newly inserted cursors may have adjusted the deletion cursor's position
            }
            // otherwise even if there is deletion, it cannot affect any of the other branches (and head is assumed to
            // not have anything that can be affected by its own deletion trie).

            adjustDeletionState(deletionDepth, depth, contentTransition);
            return depth;
        }

        @Override
        T resolveContent()
        {
            T content = super.resolveContent();
            if (content == null)
                return null;

            D deletion;
            switch (relevantDeletionsState)
            {
                case MATCHING:
                    deletion = relevantDeletions.content();
                    if (deletion != null)
                        break;
                    // else fall through
                case AHEAD:
                    deletion = relevantDeletions.coveringState();
                    break;
                default:
                    deletion = null;
            }
            if (deletion == null)
                return content;
            return deletion.delete(content);
        }

        static <T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
        void maybeAddDeletionsBranch(DeletionAwareTrieImpl.Cursor<T, D> cursor, int index, Range<D> relevantDeletions)
        {
            RangeTrieImpl.Cursor<D> deletionsBranch = cursor.deletionBranch();
            if (deletionsBranch != null)
                addCursorOrThrow(relevantDeletions, deletionsBranch);
        }

        static <C extends CursorWalkable.Cursor> void addCursorOrThrow(CollectionMergeCursor<C> where, C cursor)
        {
            boolean succeeded = where.addCursor(cursor);
            assert succeeded : "Too many deletion cursors added likely due to non-overlap of deletion branches violation.";
        }

        @Override
        public RangeTrieImpl.Cursor<D> deletionBranch()
        {
            int depth = depth();
            if (deletionBranchDepth != -1 && depth > deletionBranchDepth)
                return null;    // already covered by a deletion branch, if there is any here it will be reflected in that

            if (!branchHasMultipleSources())
                return head.deletionBranch();

            // We are positioned at a multi-source branch. If one has a deletion branch, we must combine it with the
            // deletion-tree branch of the others to make sure that we merge any lower-level deletion branch with it.

            // We have already created the merge of all present deletion branches in relevantDeletions. If that's empty,
            // there's no deletion rooted here.
            if (relevantDeletions.depth() < 0)
                return null;

            Range<D> deletions = relevantDeletions.duplicate();
            // Now add the deletion-tree branch of all sources that did not present a deletion branch.
            maybeAddDeletionTrieBranch(head, 0, deletions);
            applyToEqualOnHeap(DeletionAware::maybeAddDeletionTrieBranch, deletions);

            deletionBranchDepth = depth;
            return deletions;
        }

        static <T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
        void maybeAddDeletionTrieBranch(DeletionAwareTrieImpl.Cursor<T,D> cursor, int i, Range<D> deletions)
        {
            RangeTrieImpl.Cursor<D> deletionsBranch = cursor.deletionBranch();
            if (deletionsBranch == null)
                addCursorOrThrow(deletions, new DeletionAwareTrieImpl.DeletionsTrieCursor(cursor.duplicate()));
            // otherwise deletions already contains this cursor
        }

        @Override
        public DeletionAwareTrieImpl.Cursor<T, D> duplicate()
        {
            return new DeletionAware<>(this);
        }
    }

}
