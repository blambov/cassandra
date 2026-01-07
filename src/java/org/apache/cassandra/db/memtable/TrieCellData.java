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

package org.apache.cassandra.db.memtable;

import java.nio.ByteBuffer;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.db.rows.AbstractBufferCellData;
import org.apache.cassandra.db.rows.CellData;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;

/// [CellData] objects stored in in-memory tries.
/// Uses one 32-byte cell of an in-memory trie's buffer to store the data of a cell (without path and column id). This
/// includes liveness (timestamp/ttl/local deletion time) and value. If the value is small enough to fit, it is placed
/// directly inside the 32-byte cell; otherwise we use the given saver/loader to map it to a long integer handle and
/// store the handle.
public class TrieCellData extends AbstractBufferCellData
{
    public interface ExternalBufferHandler
    {
        /// Store the data in the given buffer and return an integer handle for it (e.g. a native address).
        long store(ByteBuffer buffer, int length) throws TrieSpaceExhaustedException;

        /// Load the data from the given handle (e.g. a native address) and return it in a buffer.
        ByteBuffer load(long handle, int length);

        /// Release a handle which will no longer be used.
        void release(long handle, int length);
    }

    public static final int OFFSET_TIMESTAMP = 0;
    public static final int OFFSET_LOCAL_DELETION_TIME = 8;
    public static final int OFFSET_TTL = 12;
    /// If the value fits, it is placed starting from this offset in the trie cell.
    public static final int OFFSET_DATA = 16;
    /// If the value does not fit, these 8 bytes hold its external handle.
    public static final int OFFSET_EXTERNAL_HANDLE = 16;
    /// If the value does not fit, these 4 bytes hold its length.
    public static final int OFFSET_EXTERNAL_LENGTH = 24;

    public static final int OFFSET_FLAGS = 31;

    /// If set, the value is stored externally, and we hold its handle and length.
    static final byte FLAG_EXTERNAL = (byte) 0x80;
    static final byte FLAG_IS_COUNTER_CELL = 0x40;

    // Bits 0x30 cannot be used (used by TrieMemtable for type id)

    static final int MAX_VALUE_LENGTH = 15;
    static final byte LENGTH_MASK = 0x0F;

    final UnsafeBuffer buffer;
    final int offset;
    final ExternalBufferHandler loader;

    /// Store the given cell data in the 32 bytes of `buffer` starting at offset `offset`. If the value cannot fit in
    /// this space, use the given external saver to store it, and save the resulting handle and the length of the value.
    public static void serialize(CellData<?, ?> cell,
                                 int typeBits,
                                 UnsafeBuffer buffer, int offset,
                                 ExternalBufferHandler externalBufferSaver)
    throws TrieSpaceExhaustedException
    {
        ByteBuffer value = cell.buffer();
        int length = value.remaining();
        buffer.putLongOrdered(offset + OFFSET_TIMESTAMP, cell.timestamp());
        buffer.putIntOrdered(offset + OFFSET_LOCAL_DELETION_TIME, cell.localDeletionTime());
        buffer.putIntOrdered(offset + OFFSET_TTL, cell.ttl());
        buffer.putByte(offset + OFFSET_FLAGS,
                       (byte) (typeBits |
                               (length <= MAX_VALUE_LENGTH ? 0 : FLAG_EXTERNAL) |
                               (cell.isCounterCell() ? FLAG_IS_COUNTER_CELL : 0) |
                               (length <= MAX_VALUE_LENGTH ? length : 0)));

        if (length <= MAX_VALUE_LENGTH)
        {
            // using the offset, length version of putBytes to make sure the source buffer's position is not touched
            buffer.putBytes(offset + OFFSET_DATA, value, 0, length);
        }
        else
        {
            long handle = externalBufferSaver.store(value, length);
            buffer.putLongOrdered(offset + OFFSET_EXTERNAL_HANDLE, handle);
            buffer.putIntOrdered(offset + OFFSET_EXTERNAL_LENGTH, length);
        }
    }

    /// Construct a [CellData] representation of the data stored in the 32 bytes at the `offset` in `buffer`.
    /// The given `loader` is used to retrieve the value if it is stored externally.
    public TrieCellData(UnsafeBuffer buffer, int offset, ExternalBufferHandler loader)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.loader = loader;
    }

    private byte getFlags()
    {
        return buffer.getByte(offset + OFFSET_FLAGS);
    }

    @Override
    public boolean isCounterCell()
    {
        return (getFlags() & FLAG_IS_COUNTER_CELL) != 0;
    }

    @Override
    public int valueSize()
    {
        byte flags = getFlags();
        if ((flags & FLAG_EXTERNAL) != 0)
            return buffer.getInt(offset + OFFSET_EXTERNAL_LENGTH);
        else
            return flags & LENGTH_MASK;
    }

    @Override
    public ByteBuffer value()
    {
        ByteBuffer buf;
        byte flags = getFlags();
        if ((flags & FLAG_EXTERNAL) == 0)
        {
            int length = flags & LENGTH_MASK;
            buf = buffer.byteBuffer().duplicate();
            buf.position(offset + OFFSET_DATA);
            buf.limit(offset + OFFSET_DATA + length);
            return buf; // we don't need to slice
        }
        else
        {
            long handle = buffer.getLong(offset + 16);
            int length = buffer.getInt(offset + 24);
            return loader.load(handle, length);
        }
    }

    @Override
    public long timestamp()
    {
        return buffer.getLong(offset + OFFSET_TIMESTAMP);
    }

    @Override
    public int ttl()
    {
        return buffer.getInt(offset + OFFSET_TTL);
    }

    @Override
    public int localDeletionTime()
    {
        return buffer.getInt(offset + OFFSET_LOCAL_DELETION_TIME);
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return 0;
    }

    public static long offTrieSize(CellData<?, ?> cell)
    {
        int sz = cell.valueSize();
        return sz <= MAX_VALUE_LENGTH ? 0 : sz;
    }

    public static void release(UnsafeBuffer buffer, int offset, ExternalBufferHandler handler)
    {
        byte flags = buffer.getByte(offset + OFFSET_FLAGS);
        if ((flags & FLAG_EXTERNAL) == 0)
            return;
        long handle = buffer.getLong(offset + OFFSET_EXTERNAL_HANDLE);
        int length = buffer.getInt(offset + OFFSET_EXTERNAL_LENGTH);
        handler.release(handle, length);
    }
}
