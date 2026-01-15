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
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.tries.InMemoryReadTrie.PAYLOAD_OFFSET;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.offset;

class ContentManagerBytes<T> implements ContentManager<T>
{
    private final ContentSerializer<T> serializer;
    private final BufferManager bufferManager;

    public ContentManagerBytes(ContentSerializer<T> serializer, BufferManager bufferManager)
    {
        this.serializer = serializer;
        this.bufferManager = bufferManager;
    }


    @Override
    public T getContent(int id)
    {
        if (id < 0)
            return serializer.special(id);
        assert offset(id) == PAYLOAD_OFFSET;
        int cell = id - PAYLOAD_OFFSET;
        return serializer.deserialize(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell));
    }

    @Override
    public boolean shouldPresentAfterBranch(int contentId)
    {
        if (contentId < 0)
            return serializer.shouldPresentSpecialAfterBranch(contentId);
        assert offset(contentId) == PAYLOAD_OFFSET;
        int cell = contentId - PAYLOAD_OFFSET;
        return serializer.shouldPresentAfterBranch(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell));
    }

    @Override
    public boolean shouldPreserveWithoutChildren(int contentId)
    {
        return serializer.shouldPreserveWithoutChildren(contentId);
    }

    @Override
    public int addContent(T value, boolean contentAfterBranch) throws TrieSpaceExhaustedException
    {
        int sizeOrSpecial = serializer.serializedSizeOrSpecial(value, contentAfterBranch);
        if (sizeOrSpecial < 0)
            return sizeOrSpecial; // special value

        int cell = bufferManager.allocateCell();
        serializer.serialize(value, contentAfterBranch, bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell));
        return cell + PAYLOAD_OFFSET;
    }

    @Override
    public int setContent(int id, T value) throws TrieSpaceExhaustedException
    {
        if (id < 0)
            return addContent(value, serializer.shouldPresentSpecialAfterBranch(id));

        assert offset(id) == PAYLOAD_OFFSET;
        int cell = id - PAYLOAD_OFFSET;
        UnsafeBuffer buffer = bufferManager.getBuffer(cell);
        int offset = bufferManager.inBufferOffset(cell);
        if (serializer.setInPlace(buffer, offset, value))
            return id;

        // Otherwise we need to move the content.
        if (serializer.releaseNeeded(id))
            serializer.releaseContent(buffer, offset);
        bufferManager.recycleCell(id);
        return addContent(value, serializer.shouldPresentAfterBranch(buffer, offset));
    }

    @Override
    public void releaseContent(int id)
    {
        if (id < 0)
            return;
        bufferManager.recycleCell(id);
        if (!serializer.releaseNeeded(id))
            return;
        assert offset(id) == PAYLOAD_OFFSET;
        int cell = id - PAYLOAD_OFFSET;
        serializer.releaseContent(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell));
    }

    @Override
    public void completeMutation()
    {
        serializer.completeMutation();
    }

    @Override
    public void abortMutation()
    {
        serializer.abortMutation();
    }

    @Override
    public String dumpContentId(int id)
    {
        if (id < 0)
            return serializer.dumpSpecial(id);

        assert offset(id) == PAYLOAD_OFFSET;
        int cell = id - PAYLOAD_OFFSET;
        return serializer.dumpContent(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell));
    }

    @Override
    public long usedSizeOnHeap()
    {
        // serializer may store large blobs outside our buffers
        return serializer.usedSizeOnHeap();
    }

    @Override
    public long usedSizeOffHeap()
    {
        // serializer may store large blobs outside our buffers
        return serializer.usedSizeOffHeap();
    }

    @Override
    public long unusedReservedOnHeapMemory()
    {
        return 0;
    }

    @Override
    public void releaseReferencesUnsafe()
    {
        // nothing to do as we don't hold any references
    }

    @Override
    public int valuesCount()
    {
        return -1; // unknown
    }

}
