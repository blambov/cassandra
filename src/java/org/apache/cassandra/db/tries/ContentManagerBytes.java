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

import static org.apache.cassandra.db.tries.InMemoryReadTrie.PAYLOAD_OFFSET;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.offset;

/// Content manager used for storing data directly in trie cells.
///
/// Relies on a [ContentSerializer] to perform encoding and decoding of the content and refers to the trie's
/// [BufferManager] to manage the cells used for storing the data.
///
/// It also supports "special" values, encoded as negative integers, which are to be directly mapped to objects by the
/// serialized without taking up trie cells.
///
/// Because the trie cells are limited in size (32 bytes), the user must use its own method of handling payloads that
/// don't fit (e.g. deferring to [ContentManagerPojo] to generate negative special ids for larger objects).
class ContentManagerBytes<T> implements ContentManager<T>
{
    private final ContentSerializer<T> serializer;
    private final BufferManager bufferManager;
    private int valuesCount = 0;

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
        ++valuesCount;
        int idIfSpecial = serializer.idIfSpecial(value, contentAfterBranch);
        if (idIfSpecial < 0)
            return idIfSpecial; // special value

        int cell = bufferManager.allocateCell();
        serializer.serialize(value, contentAfterBranch, bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell));
        return cell + PAYLOAD_OFFSET;
    }

    @Override
    public int setContent(int id, T value) throws TrieSpaceExhaustedException
    {
        if (id < 0)
        {
            serializer.releaseSpecial(id);
            --valuesCount;
            return addContent(value, serializer.shouldPresentSpecialAfterBranch(id));
        }

        assert offset(id) == PAYLOAD_OFFSET;
        int cell = id - PAYLOAD_OFFSET;
        UnsafeBuffer buffer = bufferManager.getBuffer(cell);
        int offset = bufferManager.inBufferOffset(cell);
        if (serializer.setInPlace(buffer, offset, value))
            return id;

        // Otherwise we need to move the content.
        if (serializer.releaseNeeded())
            serializer.release(buffer, offset);
        bufferManager.recycleCell(id);
        --valuesCount; // compensate for one added by addContent
        return addContent(value, serializer.shouldPresentAfterBranch(buffer, offset));
    }

    @Override
    public void releaseContent(int id)
    {
        --valuesCount;
        if (id < 0)
        {
            serializer.releaseSpecial(id);
            return;
        }

        bufferManager.recycleCell(id);
        if (!serializer.releaseNeeded())
            return;
        assert offset(id) == PAYLOAD_OFFSET;
        int cell = id - PAYLOAD_OFFSET;
        serializer.release(bufferManager.getBuffer(cell), bufferManager.inBufferOffset(cell));
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
        return serializer.unusedReservedOnHeapMemory();
    }

    @Override
    public void releaseReferencesUnsafe()
    {
        serializer.releaseReferencesUnsafe();
    }

    @Override
    public int valuesCount()
    {
        return valuesCount;
    }
}
