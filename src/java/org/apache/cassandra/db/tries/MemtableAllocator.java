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
import java.util.Arrays;

import com.google.common.annotations.VisibleForTesting;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ObjectSizes;
import org.github.jamm.MemoryLayoutSpecification;

public abstract class MemtableAllocator<T> extends Trie<T>
{
    static final int BLOCK_SIZE = 32;

    // Initial capacity for the node data buffer.
    static final int INITIAL_BUFFER_CAPACITY = 256;
    static final int INITIAL_CONTENTS_CAPACITY = 32;

    static final int BUF_SHIFT = 16;    // 2 MiB
    static final int BUF_CAPACITY = 1 << BUF_SHIFT;
    static final int CONTENTS_SHIFT = BUF_SHIFT - 2;   // This will take as much space as a buffer slab for 32-bit pointers
    static final int CONTENTS_CAPACITY = 1 << CONTENTS_SHIFT;
    private static final long UNSAFE_BUFFER_EMPTY_SIZE = ObjectSizes.measureDeep(new UnsafeBuffer(ByteBuffer.allocateDirect(0)));
    private static final long ATOMIC_REFERENCE_ARRAY_EMPTY_SIZE = ObjectSizes.measureDeep(new ExpandableAtomicReferenceArray<Object>(0));

    protected final BufferType bufferType;    // on or off heap
    UnsafeBuffer[] buffers;
    ExpandableAtomicReferenceArray<T>[] contentArrays;

    public MemtableAllocator(BufferType bufferType)
    {
        this.buffers = new UnsafeBuffer[16];  // for a total of 2G bytes
        this.contentArrays = new ExpandableAtomicReferenceArray[16];
            // takes at least 4 bytes to write pointer to one content -> 4 times smaller than buffers
        this.bufferType = bufferType;
        assert INITIAL_BUFFER_CAPACITY % BLOCK_SIZE == 0;
    }

    /*
     Buffer, content list and block management
     */
    int getChunkIdx(int pos, int chunkShift)
    {
        return pos >> chunkShift;
    }

    int inChunkPointer(int pos, int chunkSize)
    {
        return pos & (chunkSize - 1);
    }

    UnsafeBuffer getChunk(int pos)
    {
        int bufIdx = getChunkIdx(pos, BUF_SHIFT);
        return buffers[bufIdx];
    }

    int inChunkPointer(int pos)
    {
        return inChunkPointer(pos, BUF_CAPACITY);
    }

    final int getUnsignedByte(int pos)
    {
        return getChunk(pos).getByte(inChunkPointer(pos)) & 0xFF;
    }

    final int getUnsignedShort(int pos)
    {
        return getChunk(pos).getShort(inChunkPointer(pos)) & 0xFFFF;
    }

    final int getInt(int pos)
    {
        return getChunk(pos).getInt(inChunkPointer(pos));
    }

    final void putInt(int pos, int value)
    {
        getChunk(pos).putInt(inChunkPointer(pos), value);
    }

    final void putIntOrdered(int pos, int value)
    {
        getChunk(pos).putIntOrdered(inChunkPointer(pos), value);
    }

    final void putIntVolatile(int pos, int value)
    {
        getChunk(pos).putIntVolatile(inChunkPointer(pos), value);
    }

    final void putShort(int pos, short value)
    {
        getChunk(pos).putShort(inChunkPointer(pos), value);
    }

    final void putShortVolatile(int pos, short value)
    {
        getChunk(pos).putShort(inChunkPointer(pos), value);
    }

    final void putByte(int pos, byte value)
    {
        getChunk(pos).putByte(inChunkPointer(pos), value);
    }

    protected boolean maybeGrow(int allocatedPos)
    {
        // Note: If this method is modified, please run MemtableTrieTest.testOver1GSize to verify it acts correctly
        // close to the 2G limit.
        int bufIdx = getChunkIdx(allocatedPos, BUF_SHIFT);
        if (bufIdx == buffers.length)
            buffers = Arrays.copyOf(buffers, bufIdx * 2);
        UnsafeBuffer buffer = buffers[bufIdx];
        if (buffer == null)
        {
            assert inChunkPointer(allocatedPos) == 0;
            ByteBuffer newBuffer = bufIdx == 0 ? BufferType.ON_HEAP.allocate(INITIAL_BUFFER_CAPACITY)
                                               : bufferType.allocate(BUF_CAPACITY);
            buffers[bufIdx] = new UnsafeBuffer(newBuffer);
            return true;
        }
        else if (bufIdx == 0 && allocatedPos == buffer.capacity())
        {
            int newCapacity = allocatedPos * 2;
            ByteBuffer newBuffer = (newCapacity < BUF_CAPACITY ? BufferType.ON_HEAP : bufferType).allocate(newCapacity);
            buffer.getBytes(0, newBuffer, 0, allocatedPos);
            buffer.wrap(newBuffer);
            return true;
        }
        return false;
    }

    T getContent(int index)
    {
        return contentArrays[getChunkIdx(index, CONTENTS_SHIFT)].get(inChunkPointer(index, CONTENTS_CAPACITY));
    }

    protected int addContent(int index, T value)
    {
        int bufIdx = getChunkIdx(index, CONTENTS_SHIFT);
        int ofs = inChunkPointer(index, CONTENTS_CAPACITY);
        if (bufIdx == contentArrays.length)
            contentArrays = Arrays.copyOf(contentArrays, bufIdx * 2);
        ExpandableAtomicReferenceArray<T> array = contentArrays[bufIdx];
        if (array == null)
        {
            assert ofs == 0;
            contentArrays[bufIdx] = array = new ExpandableAtomicReferenceArray<>(bufIdx == 0 ? INITIAL_CONTENTS_CAPACITY : CONTENTS_CAPACITY);
        }
        else if (bufIdx == 0 && index == array.length())
        {
            array.expand(index * 2);
        }
        array.lazySet(ofs, value);

        // Note: None of the above uses a volatile set. The reason this is okay is that the new information is not
        // visible to any reader until it finds its index written somewhere in the trie. As long as the write that
        // attaches that information to the trie is done with a volatile write (which we always ensure), a happens-
        // before relationship is formed between the read, the attachment write (done by this thread some time after
        // this call), and the changes to the pointer, content chunk, and content chunk array.
        // This happens-before relationship ensures that the reader must see the new values as set above.
        return index;
    }

    protected void setContent(int index, T value)
    {
        int chunkIdx = getChunkIdx(index, CONTENTS_SHIFT);
        int ofs = inChunkPointer(index, CONTENTS_CAPACITY);
        ExpandableAtomicReferenceArray<T> array = contentArrays[chunkIdx];
        array.set(ofs, value);
    }


    /** Returns the on heap size of the memtable trie itself, not counting any space taken by referenced content. */
    public long allocatorExtraSizeOnHeap(int allocatedPos, int contentCount)
    {
        return (getChunkIdx(allocatedPos - 1, BUF_SHIFT) + 1) * UNSAFE_BUFFER_EMPTY_SIZE +
               (getChunkIdx(contentCount - 1, CONTENTS_SHIFT) + 1) * ATOMIC_REFERENCE_ARRAY_EMPTY_SIZE;
    }

    public void discardBuffers()
    {
        if (bufferType == BufferType.ON_HEAP)
            return; // no cleaning needed

        for (UnsafeBuffer b : buffers)
        {
            if (b != null)
                FileUtils.clean(b.byteBuffer());
        }
    }
}
