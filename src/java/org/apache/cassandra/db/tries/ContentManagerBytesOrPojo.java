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

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.InMemoryReadTrie.PAYLOAD_OFFSET;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.offset;

public class ContentManagerBytesOrPojo<T> extends ContentManagerPojo<T>
{

    public static final byte AFTER_BRANCH_FLAG = (byte) 0x80;

    interface ContentSerializer<T>
    {
        // size cannot be more than 31 bytes
        int serializedSize(T content);
        // Has serialized size bytes to work with
        void serialize(T content, UnsafeBuffer buffer, int offset);
        // Must know/store the length of the payload
        T deserialize(UnsafeBuffer buffer, int offset);
    }

    private final ContentSerializer<T> serializer;
    private final BufferManager bufferManager;

    public ContentManagerBytesOrPojo(ContentSerializer<T> serializer, BufferManager bufferManager,
                                     InMemoryBaseTrie.ExpectedLifetime lifetime, OpOrder opOrder)
    {
        super(lifetime, opOrder);
        this.serializer = serializer;
        this.bufferManager = bufferManager;
    }


    @Override
    public T getContent(int id)
    {
        if (id < 0)
            return super.getContent(id);
        assert offset(id) == PAYLOAD_OFFSET;
        int cell = id - PAYLOAD_OFFSET;
        return serializer.deserialize(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell));
    }

    @Override
    public boolean shouldPresentAfterBranch(int contentId)
    {
        if (contentId < 0)
            return super.shouldPresentAfterBranch(contentId);
        assert offset(contentId) == PAYLOAD_OFFSET;
        int cell = contentId;
        return (bufferManager.getBuffer(cell).getByte(bufferManager.inBufferOffset(cell)) & AFTER_BRANCH_FLAG) != 0;
    }

    @Override
    public int addContent(T value, boolean contentAfterBranch) throws TrieSpaceExhaustedException
    {
        int size = serializer.serializedSize(value);
        assert size >= 0;
        if (size > 31)
            return size; // special value

        int cell = bufferManager.allocateCell();
        UnsafeBuffer buffer = bufferManager.getBuffer(cell);
        int offset = bufferManager.inBufferOffset(cell);
        serializer.serialize(value, buffer, offset);
        buffer.putByte(cell + PAYLOAD_OFFSET, contentAfterBranch ? AFTER_BRANCH_FLAG : 0);
        return cell + PAYLOAD_OFFSET;
    }

    @Override
    public int setContent(int id, T value) throws TrieSpaceExhaustedException
    {
        if (id < 0)
            return super.setContent(id, value);

        int size = serializer.serializedSize(value);
        assert size >= 0;
        if (size <= 31)
        {
            // we can modify in place
            assert offset(id) == PAYLOAD_OFFSET;
            int cell = id - PAYLOAD_OFFSET;
            UnsafeBuffer buffer = bufferManager.getBuffer(cell);
            int offset = bufferManager.inBufferOffset(cell);
            serializer.serialize(value, buffer, offset);
            return id;
        }
        else
        {
            // Otherwise we need to move the content.
            boolean shouldPresentAfterBranch = shouldPresentAfterBranch(id);
            bufferManager.recycleCell(id);
            return addContent(value, shouldPresentAfterBranch);
        }
    }

    @Override
    public void releaseContent(int id)
    {
        if (id < 0)
            super.releaseContent(id);
        else
            bufferManager.recycleCell(id);
    }

    @Override
    public String dumpContentId(int id)
    {
        if (id < 0)
            return super.dumpContentId(id);

        return String.format("%08x", id);
    }

}
