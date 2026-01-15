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

package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.db.context.CounterContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TrieCellData implements CellData<ByteBuffer>
{
    public interface ExternalBufferSaver
    {
        long store(ByteBuffer buffer, int length);
    }

    public interface ExternalBufferLoader
    {
        ByteBuffer load(long address, int length);
    }

    public static final int TIMESTAMP_OFFSET = 0;
    public static final int LOCAL_DELETION_TIME_OFFSET = 8;
    public static final int TTL_OFFSET = 12;
    public static final int DATA_OFFSET = 16;
    public static final int NATIVE_ADDRESS_OFFSET = 16;
    public static final int NATIVE_LENGTH_OFFSET = 24;

    public static final int FLAGS_OFFSET = 31;

    static final byte FLAG_ADDRESS = (byte) 0x80;
    static final byte FLAG_IS_COUNTER_CELL = 0x40;

    static final byte TYPE_CELL = 0x00;

    static final int MAX_LENGTH = 15;
    static final byte LENGTH_MASK = 0x0F;

    final UnsafeBuffer buffer;
    final int offset;
    final ExternalBufferLoader loader;

    public static int size(CellData<?> unused)
    {
        return 32;
    }

    public static void serialize(CellData<?> cell, UnsafeBuffer buffer, int offset,
                                 ExternalBufferSaver externalBufferSaver)
    {
        ByteBuffer value = cell.buffer();
        int length = value.remaining();
        buffer.putLongOrdered(offset + TIMESTAMP_OFFSET, cell.timestamp());
        buffer.putIntOrdered(offset + LOCAL_DELETION_TIME_OFFSET, cell.localDeletionTime());
        buffer.putIntOrdered(offset + TTL_OFFSET, cell.ttl());
        buffer.putByte(offset + FLAGS_OFFSET,
                       (byte) (TYPE_CELL |
                               (length <= MAX_LENGTH ? 0 : FLAG_ADDRESS) |
                               (cell.isCounterCell() ? FLAG_IS_COUNTER_CELL : 0) |
                               (length <= MAX_LENGTH ? length : 0)));

        if (length <= MAX_LENGTH)
        {
            buffer.putBytes(offset + DATA_OFFSET, value, length);
        }
        else
        {

            long address = externalBufferSaver.store(value, length);
            buffer.putLongOrdered(offset + NATIVE_ADDRESS_OFFSET, address);
            buffer.putIntOrdered(offset + NATIVE_LENGTH_OFFSET, length);
        }
    }

    public TrieCellData(UnsafeBuffer buffer, int offset, ExternalBufferLoader loader)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.loader = loader;
    }

    public Cell<ByteBuffer> toCell(ColumnMetadata column, CellPath path)
    {
        return new BufferCell(column, timestamp(), ttl(), localDeletionTime(), value(), path);
    }

    public static CellData reconcile(TrieCellData existing, CellData<?> update)
    {
        if (existing == null)
            return update;

        return Cells.reconcile(existing, update);
    }

    private byte getFlags()
    {
        return buffer.getByte(offset + FLAGS_OFFSET);
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
        if ((flags & FLAG_ADDRESS) != 0)
            return buffer.getInt(offset + NATIVE_LENGTH_OFFSET);
        else
            return flags & LENGTH_MASK;
    }

    @Override
    public ByteBuffer value()
    {
        ByteBuffer buf;
        byte flags = getFlags();
        if ((flags & FLAG_ADDRESS) == 0)
        {
            int length = flags & LENGTH_MASK;
            buf = buffer.byteBuffer().duplicate();
            buf.position(offset + DATA_OFFSET);
            buf.limit(offset + DATA_OFFSET + length);
            return buf.slice();
        }
        else
        {
            long address = buffer.getLong(offset + 16);
            int length = buffer.getInt(offset + 24);
            return loader.load(address, length);
        }
    }

    @Override
    public ValueAccessor<ByteBuffer> accessor()
    {
        return ByteBufferAccessor.instance;
    }

    @Override
    public long timestamp()
    {
        return buffer.getLong(offset + TIMESTAMP_OFFSET);
    }

    @Override
    public int ttl()
    {
        return buffer.getInt(offset + TTL_OFFSET);
    }

    @Override
    public int localDeletionTime()
    {
        return buffer.getInt(offset + LOCAL_DELETION_TIME_OFFSET);
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return 0;
    }

    @Override
    public CellData withNewValue(long timestamp, ByteBuffer value)
    {
        return null;
    }

    public static long offTrieSize(CellData<?> cell)
    {
        int sz = cell.valueSize();
        return sz <= MAX_LENGTH ? 0 : sz;
    }

    @Override
    public String toString()
    {
        if (isCounterCell())
            return String.format("[?=%d ts=%d]", CounterContext.instance().total(value(), accessor()), timestamp());
        if (isTombstone())
            return String.format("[?=<tombstone> %s]", livenessInfoString());
        else
            return String.format("[?=%s %s]", ByteBufferUtil.bytesToHex(buffer()), livenessInfoString());
    }

    private String livenessInfoString()
    {
        if (isExpiring())
            return String.format("ts=%d ttl=%d ldt=%d", timestamp(), ttl(), localDeletionTime());
        else if (isTombstone())
            return String.format("ts=%d ldt=%d", timestamp(), localDeletionTime());
        else
            return String.format("ts=%d", timestamp());
    }
}
