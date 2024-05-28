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

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.LoggerFactory;

import org.agrona.collections.IntArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.github.jamm.MemoryLayoutSpecification;

/**
 * Allocation strategy for buffers and arrays for InMemoryTrie's. Controls how space is allocated and reused.
 */
public interface MemtableAllocationStrategy
{
    // Initial capacity for the node data buffer.
    static final int INITIAL_BUFFER_CAPACITY = 256;
    static final int BLOCK_SIZE = InMemoryReadTrie.BLOCK_SIZE;

    /** Return a reference to the UnsafeBuffer this allocator is using */
    UnsafeBuffer buffer();
    /** Return a reference to the content and metadata array that this allocator is using */
    ExpandableAtomicReferenceArray array();

    /** Allocates a new cell in the buffer of size BLOCK_SIZE, aligned to BLOCK_SIZE bytes. The new cell is zeroed out. */
    int allocateCell() throws InMemoryTrie.SpaceExhaustedException;
    /**
     * Allocates a new cell in the buffer, and copies the content of the given cell. The argument does not need to be
     * BLOCK_SIZE aligned -- the method will copy the cell that contains the given pointer, and return a pointer with
     * the same offset in the copied cell.
     * The original cell is understood to no longer be necessary and is marked for recycling.
     */
    int copyCell(int cell) throws InMemoryTrie.SpaceExhaustedException;
    /**
     * Allocate a new place in the objects array (for content or metadata).
     */
    int allocateObject() throws InMemoryTrie.SpaceExhaustedException;

    /**
     * Marks the given cell for recycling. The argument does not need to be aligned (i.e. we remove the offset and thus
     * recycle the cell containing the given pointer).
     *
     * When the cell is actually reused depends on the recycling strategy. In any case it cannot be before the current
     * mutation is complete (because it may still be walking cells that have been moved), and any concurrent readers
     * that have started before this cell has become unreachable must also have completed.
     */
    void recycleCell(int position);

    /**
     * Mark the given place in the objects array for recycling.
     *
     * When the place is actually reused depends on the recycling strategy. In any case it cannot be before the current
     * mutation is complete (because it may still be walking cells that have been moved), and any concurrent readers
     * that have started before this cell has become unreachable must also have completed.
     */
    void recycleObject(int pos);

    /**
     * Size of the _used_ data on heap maintained by this strategy. The actual allocation is usually higher, as we don't
     * count any space that has been reserved, or that has been recycled (as the table should be able to grow to use it).
     * Note that this does not count any of the objects referenced by the objects array.
     */
    long sizeOnHeap();
    /**
     * Size of the _used_ data off heap maintained by this strategy. The actual allocation is usually higher, as we don't
     * count any space that has been reserved, or that has been recycled (as the table should be able to grow to use it).
     */
    long sizeOffHeap();

    /**
     * Returns whether a buffer change has been made; UnsafeBuffer uses a non-volatile write to adopt a new buffer, and
     * to make sure changes are visible to readers from other threads the trie must touch the volatile root.
     * Note that this must be done before completeMutation, to ensure that no readers can see stale content at the time
     * completeMutation takes its barriers.
     */
    boolean bufferGrew();

    /**
     * To be called when a mutation completes. No new readers must be able to see recycled content at the time of this
     * call (the paths for reaching them must have been overwritten via a volatile write; additionally, if the buffer
     * has grown, the root variable (which is stored outside the buffer) must have accepted a volatile write).
     * No recycled cell can be made available for reuse before this is called, and before any readers started before
     * this call have completed.
     */
    void completeMutation();

    /**
     * Called when a mutation is aborted because of an exception. This means that the cells that were marked for
     * recycling are still going to be in use (unless this is called a later separate completeMutation call may release
     * and reuse them, causing corruption).
     *
     * Aborted mutations are not normal, and at this time we are not trying to ensure that a trie will behave at its
     * best if an abort has taken place (i.e. it may take more space, be slower etc.), but it should still operate
     * correctly.
     */
    void abortMutation();

    /**
     * Returns true if the allocation threshold has been reached. To be called by the the writing thread (ideally, just
     * after the write completes). When this returns true, the user should switch to a new trie as soon as feasible.
     *
     * The trie expects up to 10% growth above this threshold. Any growth beyond that may be done inefficiently, and
     * the trie will fail altogether when the size grows beyond 2G - 256 bytes.
     */
    boolean reachedAllocatedSizeThreshold();

    /**
     * Discard/clean all off-heap buffers for quicker reuse.
     */
    void discardBuffers();

    /**
     * Returns the amount of on-heap memory that is allocated but not currently in use.
     */
    long availableForAllocationOnHeap();

    /**
     * Returns the amount of on-heap memory that is allocated but not currently in use.
     */
    long availableForAllocationOffHeap();


    static void expand(BufferType bufferType, UnsafeBuffer buffer, int newSize)
    {
        switch (bufferType)
        {
            case ON_HEAP:
            {
                // Use byte array directly, saving some unnecessary allocations
                byte[] newBuffer = new byte[newSize];
                buffer.getBytes(0, newBuffer, 0, buffer.capacity());
                buffer.wrap(newBuffer);
                return;
            }
            default:
            {
                ByteBuffer newBuffer = bufferType.allocate(newSize);
                buffer.getBytes(0, newBuffer, 0, buffer.capacity());
                buffer.wrap(newBuffer);
                return;
            }
        }
    }


    /**
     * Strategy for small short-lived tries, usually on-heap. This strategy does not reuse any cells.
     */
    class NoReuseStrategy implements MemtableAllocationStrategy
    {
        private static final long EMPTY_SIZE_ON_HEAP; // for space calculations
        private static final long EMPTY_SIZE_OFF_HEAP; // for space calculations

        static
        {
            NoReuseStrategy empty = new NoReuseStrategy(BufferType.ON_HEAP);
            EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty) -
                                 empty.array.length() * MemoryLayoutSpecification.SPEC.getReferenceSize() -
                                 empty.buffer.capacity();
            empty = new NoReuseStrategy(BufferType.OFF_HEAP);
            EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty) -
                                  empty.array.length() * MemoryLayoutSpecification.SPEC.getReferenceSize() -
                                  empty.buffer.capacity();
        }

        final BufferType bufferType;
        final UnsafeBuffer buffer;
        final ExpandableAtomicReferenceArray array;

        int contentCount = 0;
        int allocatedPos = 0;
        boolean bufferGrew = false;

        public NoReuseStrategy(BufferType bufferType)
        {
            this.bufferType = bufferType;
            this.buffer = new UnsafeBuffer();
            this.array = new ExpandableAtomicReferenceArray(8);
            assert INITIAL_BUFFER_CAPACITY % BLOCK_SIZE == 0;
            expand(bufferType, buffer, INITIAL_BUFFER_CAPACITY);
        }

        public UnsafeBuffer buffer()
        {
            return buffer;
        }

        public ExpandableAtomicReferenceArray array()
        {
            return array;
        }

        int allocateUninitializedCell() throws InMemoryTrie.SpaceExhaustedException
        {
            int v = allocatedPos;
            if (buffer.capacity() == v)
            {
                expand(bufferType, buffer,  InMemoryTrie.sizeForGrowth(buffer.capacity(), allocatedPos + BLOCK_SIZE));
                // The above does not contain any happens-before enforcing writes.
                // Because they don't see the new buffer, readers from other threads may fail to see any modifications
                // for a long time. To prevent this, we signal the trie during completeMutation so that it can touch the
                // volatile root and enforce visibility for the new buffer.
                bufferGrew = true;
            }

            allocatedPos += BLOCK_SIZE;
            return v;
        }

        public int allocateCell() throws InMemoryTrie.SpaceExhaustedException
        {
            // Not reusing, so cell should already be zeroed.
            return allocateUninitializedCell();
        }

        public int copyCell(int cell) throws InMemoryTrie.SpaceExhaustedException
        {
            int copy = allocateUninitializedCell();
            buffer.putBytes(copy, buffer, cell & -BLOCK_SIZE, BLOCK_SIZE);
            recycleCell(cell);
            return copy | (cell & (BLOCK_SIZE - 1));
        }

        public int allocateObject() throws InMemoryTrie.SpaceExhaustedException
        {
            int index = contentCount++;

            if (index == array.length())
                array.resize(index * 2);

            return index;
        }

        public void recycleCell(int cell)
        {
            // No reuse, do nothing
        }

        public void recycleObject(int index)
        {
            // No reuse, we don't need to do anything
            // We can't zero out the pointer because readers may still need it.
        }

        public boolean bufferGrew()
        {
            if (!bufferGrew)
                return false;

            bufferGrew = false;
            return true;
        }

        public void completeMutation()
        {
            // No reuse, nothing to do
        }

        public void abortMutation()
        {
            // No reuse, nothing to do
        }

        public long sizeOffHeap()
        {
            return (bufferType == BufferType.ON_HEAP ? 0 : allocatedPos) - availableForAllocationOffHeap();
        }

        public long sizeOnHeap()
        {
            return (contentCount * MemoryLayoutSpecification.SPEC.getReferenceSize() +
                    (bufferType == BufferType.ON_HEAP ? allocatedPos + EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP))
                    - availableForAllocationOnHeap();
        }

        public long availableForAllocationOnHeap()
        {
            return 0;
        }

        public long availableForAllocationOffHeap()
        {
            return 0;
        }

        public boolean reachedAllocatedSizeThreshold()
        {
            return allocatedPos >= InMemoryTrie.ALLOCATED_SIZE_THRESHOLD;
        }

        /**
         * For tests only! Advance the allocation pointer (and allocate space) by this much to test behaviour close to
         * full.
         */
        @VisibleForTesting
        int advanceAllocatedPos(int wantedPos) throws InMemoryTrie.SpaceExhaustedException
        {
            while (allocatedPos < wantedPos)
                allocateCell();
            return allocatedPos;
        }

        public void discardBuffers()
        {
            FileUtils.clean(buffer.byteBuffer());
        }
    }

    /**
     * Reuse strategy for large, long-lived tries. Recycles cells and objects when it knows that the mutation recycling
     * them has completed, and all reads started no later than this completion have also completed (signalled by an
     * OpOrder which the strategy assumes all readers subscribe to).
     *
     * The implementation of the recycling strategy is done by OpOrderReusedIndexes below; this class uses one of them
     * for buffer cells and one for array slots.
     */
    class OpOrderReuseStrategy extends NoReuseStrategy
    {
        final OpOrder opOrder;
        final OpOrderReusedIndexes cells;
        final OpOrderReusedIndexes pojos;

        public OpOrderReuseStrategy(BufferType bufferType, final OpOrder order)
        {
            super(bufferType);
            this.opOrder = order;
            this.cells = new OpOrderReusedIndexes(this::fillCells);
            this.pojos = new OpOrderReusedIndexes(this::fillObjects);
        }

        private void fillCells(IndexList indexList) throws InMemoryTrie.SpaceExhaustedException
        {
            int block = BLOCK_SIZE * REUSE_BLOCK_SIZE;
            int pos = allocatedPos;
            if (buffer.capacity() < pos + block)
            {
                expand(bufferType, buffer, InMemoryTrie.sizeForGrowth(buffer.capacity(), pos + block));
                // The above does not contain any happens-before enforcing writes.
                // Because they don't see the new buffer, readers from other threads may fail to see any modifications
                // for a long time. To prevent this, we signal the trie during completeMutation so that it can touch the
                // volatile root and enforce visibility for the new buffer.
                bufferGrew = true;
            }

            allocatedPos += block;
            int[] indexes = indexList.indexes;
            for (int i = 0; i < REUSE_BLOCK_SIZE; ++i)
                indexes[REUSE_BLOCK_SIZE - 1 - i] = pos + i * BLOCK_SIZE;  // fill from the back, so that lowest indexes are used first
            indexList.count = REUSE_BLOCK_SIZE;
        }

        private void fillObjects(IndexList indexList)
        {
            int pos = contentCount;
            if (pos + REUSE_BLOCK_SIZE > array.length())
                array.resize(Math.max(pos + REUSE_BLOCK_SIZE, array.length() * 2));

            contentCount += REUSE_BLOCK_SIZE;
            int[] indexes = indexList.indexes;
            for (int i = 0; i < REUSE_BLOCK_SIZE; ++i)
                indexes[i] = pos + i;
            indexList.count = REUSE_BLOCK_SIZE;
        }

        @Override
        int allocateUninitializedCell() throws InMemoryTrie.SpaceExhaustedException
        {
            return cells.allocate();
        }

        @Override
        public int allocateCell() throws InMemoryTrie.SpaceExhaustedException
        {
            int cell = allocateUninitializedCell();
            buffer.setMemory(cell, BLOCK_SIZE, (byte) 0);
            return cell;
        }

        @Override
        public void recycleCell(int cell)
        {
            // we are called with pointers, get the cell start
            cells.recycle(cell & -BLOCK_SIZE);
        }

        @Override
        public int allocateObject() throws InMemoryTrie.SpaceExhaustedException
        {
            return pojos.allocate();
        }

        @Override
        public void recycleObject(int index)
        {
            // Note: we can't clear the reference because readers may still need it
            pojos.recycle(index);
        }

        @Override
        public void completeMutation()
        {
            cells.mutationComplete(opOrder);
            pojos.mutationComplete(opOrder);
            // Note: The above may issue two separate barriers, but I don't think the overhead is big enough to matter.
        }

        @Override
        public void abortMutation()
        {
            cells.mutationAborted();
            pojos.mutationAborted();
            // Note: The above may issue two separate barriers, but I don't think the overhead is big enough to matter.
        }

        @Override
        public long availableForAllocationOnHeap()
        {
            return pojos.availableForAllocation() * MemoryLayoutSpecification.SPEC.getReferenceSize()
                   + (bufferType == BufferType.ON_HEAP ? cells.availableForAllocation() * BLOCK_SIZE : 0);
        }

        @Override
        public long availableForAllocationOffHeap()
        {
            return bufferType == BufferType.ON_HEAP ? 0 : cells.availableForAllocation() * BLOCK_SIZE;
        }

        @Override
        public boolean reachedAllocatedSizeThreshold()
        {
            return allocatedPos >= InMemoryTrie.ALLOCATED_SIZE_THRESHOLD;
        }
    }

    interface Allocator<T>
    {
        void allocateNewItems(T list) throws InMemoryTrie.SpaceExhaustedException;
    }

    /**
     * Core of the OpOrder recycling strategy. Holds queues of indexes available for recycling.
     *
     * The queues ar organized in blocks of REUSE_BLOCK_SIZE entries. The blocks move through the following stages:
     * - Being filled with newly released indexes. In this stage they are at the head of the "justReleased" list. When
     *   a block becomes full, a new block is created and attached to the head of the list.
     * - Full, but the mutation that released one or more of the mutations in them has not yet completed. In this stage
     *   they are attached to the "justReleased" list as the second or further block. When a mutationComplete is
     *   received, all such blocks get issued a common OpOrder.Barrier and are attached to "awaitingBarrierTail" (which
     *   is the tail of the "free" list).
     * - Awaiting a barrier. In this stage they are in the "free" list after its head, closer to its
     *   "awaitingBarrierTail", identified by the fact that their barrier has not yet expired. Note that the blocks are
     *   put in the order in which their barriers are issued, thus if a block has an active barrier, all blocks that
     *   follow it in the list also do.
     * - Ready for use. In this stage they are still in the "free" list after its head, but their barrier has now
     *   expired. All the indexes in such blocks can now be reused, and will be when the head of the list is exhausted.
     * - Active free block at the head of the "free" list. This block is the one new allocations are served from. When
     *   it is exhausted, we check if the next block's barrier has expired. If so, the "free" pointer moves to it.
     *   If not, there's nothing to reuse as any blocks in the list still have an active barrier, thus we grab some new
     *   memory and refill the block.
     * - If a mutation is aborted by an error, we throw away all cells in the "justReleased" list. This is done so that
     *   none of the cells that were marked for release, but whose parent chain may have remained in place, making them
     *   reachable, are reused and corrupt the trie. This will leak some cells (from earlier mutations in the block
     *   and/or ones whose parents have already been moved), but we prefer not to pay the cost of identifying the exact
     *   cells that need to remain or be recycled.
     *   We assume that exceptions while mutating are not normal and should not happen, and thus a temporary leak (e.g.
     *   until the memtable is switched) is acceptable. Should this change (e.g. if a trie is used for the full lifetime
     *   of the process or longer and exceptions are expected as part of its function), we can implement a reachability
     *   walk to identify orphaned cells and call it with some frequency after one or more exceptions have occured.
     */
    static class OpOrderReusedIndexes
    {
        /**
         * Cells list holding indexes that are just recycled. When full, new one is allocated and linked.
         *
         * On mutationComplete, any full (in justReleased.nextList) lists get issued a barrier and are moved to
         * awaitingBarrierTail.
         */
        IndexList justReleased;

        /**
         * Tail of the "free and awaiting barrier" queue. This is reachable by following the links from free.
         *
         * Full lists are attached to this tail when their barrier is issued.
         * Lists are consumed from the head when free becomes empty if the list at the head has an expired barrier.
         */
        IndexList awaitingBarrierTail;

        /**
         * Current free list, head of the "free and awaiting barrier" queue. Allocations are served from here.
         *
         * Starts full, and when it is exhausted we check the barrier at the next linked block.
         * If expired, update free to point to it (consuming one block from the queue).
         * If not, re-fill the block by allocating a new set of REUSE_BLOCK_SIZE indexes.
         */
        IndexList free;

        /**
         * Called to allocate a new block of indexes to distribute.
         */
        final Allocator<IndexList> allocator;

        public OpOrderReusedIndexes(Allocator<IndexList> allocator)
        {
            this.allocator = allocator;
            justReleased = new IndexList(null);
            awaitingBarrierTail = free = new IndexList(null);
            try
            {
                allocator.allocateNewItems(free);
            }
            catch (InMemoryTrie.SpaceExhaustedException e)
            {
                throw new AssertionError(e);    // unexpected, initial size can't trigger this
            }
        }

        int allocate() throws InMemoryTrie.SpaceExhaustedException
        {
            if (free.count == 0)
            {
                IndexList awaitingBarrierHead = free.nextList;
                if (awaitingBarrierHead != null &&
                    (awaitingBarrierHead.barrier == null || awaitingBarrierHead.barrier.allPriorOpsAreFinished()))
                {
                    // A block is ready for reuse. Switch to it.
                    free = awaitingBarrierHead;
                    // Maybe recycle the "free" IndexList?
                }
                else
                {
                    // Nothing available for reuse. Grab more memory.
                    allocator.allocateNewItems(free);
                }
            }

            return free.indexes[--free.count];
        }

        void recycle(int index)
        {
            if (justReleased.count == REUSE_BLOCK_SIZE)
            {
                // Block is full, allocate and attach a new one.
                justReleased = new IndexList(justReleased);
            }

            justReleased.indexes[justReleased.count++] = index;
        }

        void mutationComplete(OpOrder opOrder)
        {
            IndexList toProcess = justReleased.nextList;
            if (toProcess == null)
                return;

            // We have some completed blocks now, issue a barrier for them and move them to the
            // "free and awaiting barrier" queue.
            justReleased.nextList = null;

            OpOrder.Barrier barrier = null;
            if (opOrder != null)
            {
                barrier = opOrder.newBarrier();
                barrier.issue();
            }

            IndexList last = null;
            for (IndexList current = toProcess; current != null; current = current.nextList)
            {
                current.barrier = barrier;
                last = current;
            }

            assert awaitingBarrierTail.nextList == null;
            awaitingBarrierTail.nextList = toProcess;
            awaitingBarrierTail = last;
        }

        public void mutationAborted()
        {
            // Some of the releases in the justReleased queue may still be reachable cells.
            // We don't have a way of telling which, so we have to remove everything.
            justReleased.nextList = null;
            justReleased.count = 0;
        }

        /**
         * Returns the number of indexes that are somewhere in the recycling pipeline.
         */
        public int availableForAllocation()
        {
            int count = 0;
            for (IndexList list = justReleased; list != null; list = list.nextList)
                count += list.count;
            for (IndexList list = free; list != null; list = list.nextList) // includes awaiting barrier
                count += list.count;
            return count;
        }

        /**
         * Constructs a list of all the cells that are in the recycling pipeline.
         * Used to test available and unreachable cells are the same thing.
         */
        @VisibleForTesting
        IntArrayList allAvailable()
        {
            IntArrayList res = new IntArrayList(availableForAllocation(), -1);
            for (IndexList list = justReleased; list != null; list = list.nextList)
                res.addAll(new IntArrayList(list.indexes, list.count, -1));
            for (IndexList list = free; list != null; list = list.nextList) // includes awaiting barrier
                res.addAll(new IntArrayList(list.indexes, list.count, -1));
            return res;
        }
    }


    static final int REUSE_BLOCK_SIZE = 1024;

    static class IndexList
    {
        final int[] indexes;
        int count;
        OpOrder.Barrier barrier;
        IndexList nextList;

        IndexList(IndexList next)
        {
            indexes = new int[REUSE_BLOCK_SIZE];
            nextList = next;
            count = 0;
        }
    }
}
