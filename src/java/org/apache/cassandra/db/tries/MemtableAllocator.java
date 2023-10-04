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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
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

    static final int LISTS_FIRST_RESIZE = 16;

    // TODO: make configurable
    static final int BUF_SHIFT = 21;    // 2 MiB
    static final int BUF_CAPACITY = 1 << BUF_SHIFT;
    static final int CONTENTS_SHIFT = BUF_SHIFT - 2;   // This will take as much space as a buffer slab for 32-bit pointers
    static final int CONTENTS_CAPACITY = 1 << CONTENTS_SHIFT;
    private static final long UNSAFE_BUFFER_EMPTY_SIZE = ObjectSizes.measureDeep(new UnsafeBuffer(ByteBuffer.allocateDirect(0))) +
                                                         MemoryLayoutSpecification.SPEC.getReferenceSize();
    private static final long REFERENCE_ARRAY_EMPTY_SIZE = ObjectSizes.sizeOfReferenceArray(0) +
                                                           MemoryLayoutSpecification.SPEC.getReferenceSize();

    private static VarHandle ARRAY_HANDLE = MethodHandles.arrayElementVarHandle(Object[].class);

    protected final BufferType bufferType;    // on or off heap
    UnsafeBuffer[] buffers;
    Object[][] contentArrays;

    public MemtableAllocator(BufferType bufferType)
    {
        this.buffers = new UnsafeBuffer[1];
        this.contentArrays = new Object[1][];
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

    protected boolean maybeGrowBuffer(int allocatedPos)
    {
        // Note: If this method is modified, please run MemtableTrieTest.testOver1GSize to verify it acts correctly
        // close to the 2G limit.
        boolean result = false;
        int bufIdx = getChunkIdx(allocatedPos, BUF_SHIFT);
        if (bufIdx == buffers.length)
        {
            buffers = Arrays.copyOf(buffers, Math.max(bufIdx * 2, LISTS_FIRST_RESIZE));
            result = true;
        }
        UnsafeBuffer buffer = buffers[bufIdx];
        if (buffer == null)
        {
            assert inChunkPointer(allocatedPos) == 0;
            ByteBuffer newBuffer = bufIdx == 0 ? BufferType.ON_HEAP.allocate(INITIAL_BUFFER_CAPACITY)
                                               : bufferType.allocate(BUF_CAPACITY);
            buffers[bufIdx] = new UnsafeBuffer(newBuffer);
            result = true;
        }
        else if (bufIdx == 0 && allocatedPos == buffer.capacity())
        {
            int newCapacity = allocatedPos * 2;
            ByteBuffer newBuffer = (newCapacity < BUF_CAPACITY ? BufferType.ON_HEAP : bufferType).allocate(newCapacity);
            buffer.getBytes(0, newBuffer, 0, allocatedPos);
            buffer.wrap(newBuffer);
            result = true;
        }
        return result;
    }

    T getContent(int index)
    {
        return (T) ARRAY_HANDLE.getVolatile(contentArrays[getChunkIdx(index, CONTENTS_SHIFT)],
                                            inChunkPointer(index, CONTENTS_CAPACITY));
    }

    /**
     * Add a new content entry, return true if this caused the content arrays to grow (which necessitates global
     * happens-before enforcement by touching the root).
     */
    protected boolean addContentMaybeGrow(int index, T value)
    {
        boolean result = false;
        int bufIdx = getChunkIdx(index, CONTENTS_SHIFT);
        int ofs = inChunkPointer(index, CONTENTS_CAPACITY);
        if (bufIdx == contentArrays.length)
        {
            contentArrays = Arrays.copyOf(contentArrays, Math.max(bufIdx * 2, LISTS_FIRST_RESIZE));
            result = true;
        }
        Object[] array = contentArrays[bufIdx];
        if (array == null)
        {
            assert ofs == 0;
            contentArrays[bufIdx] = array = new Object[bufIdx == 0 ? INITIAL_CONTENTS_CAPACITY : CONTENTS_CAPACITY];
            result = true;
        }
        else if (bufIdx == 0 && index == array.length)
        {
            contentArrays[bufIdx] = array = Arrays.copyOf(array, index * 2);
            result = true;
        }
        array[ofs] = value;
        return result;
    }

    protected void setContent(int index, T value)
    {
        ARRAY_HANDLE.setVolatile(contentArrays[getChunkIdx(index, CONTENTS_SHIFT)],
                                 inChunkPointer(index, CONTENTS_CAPACITY),
                                 value);
    }


    /** Returns the on heap size of the memtable trie itself, not counting any space taken by referenced content. */
    public long allocatorExtraSizeOnHeap(int allocatedPos, int contentCount)
    {
        return (getChunkIdx(allocatedPos - 1, BUF_SHIFT) + 1) * UNSAFE_BUFFER_EMPTY_SIZE +
               (getChunkIdx(contentCount - 1, CONTENTS_SHIFT) + 1) * REFERENCE_ARRAY_EMPTY_SIZE;
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
