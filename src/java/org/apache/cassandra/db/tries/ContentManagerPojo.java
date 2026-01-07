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

import java.util.concurrent.atomic.AtomicReferenceArray;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.utils.concurrent.OpOrder;
import org.github.jamm.MemoryLayoutSpecification;

import static org.apache.cassandra.db.tries.InMemoryBaseTrie.REFERENCE_ARRAY_ON_HEAP_SIZE;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.getBufferIdx;
import static org.apache.cassandra.db.tries.InMemoryReadTrie.inBufferOffset;

public class ContentManagerPojo<T> extends MemoryAllocationStrategy.OpOrderReuseStrategy
implements ContentManager<T>, MemoryAllocationStrategy.Allocator
{
    static final int CONTENT_FLAGS_SHIFT = 29;
    static final int CONTENT_INDEX_MASK = (1 << CONTENT_FLAGS_SHIFT) - 1;

    static final int CONTENT_AFTER_BRANCH = 1 << 30;

    static final int CONTENTS_START_SHIFT = 4;
    static final int CONTENTS_START_SIZE = 1 << CONTENTS_START_SHIFT;

    private int contentCount = 0;
    final AtomicReferenceArray<T>[] contentArrays;

    public static <T> ContentManager<T> create(InMemoryBaseTrie.ExpectedLifetime lifetime, OpOrder opOrder)
    {
        switch (lifetime)
        {
            case SHORT:
                return new ContentManagerShortLivedPojo<>();
            case LONG:
                return new ContentManagerPojo<>(opOrder);
            default:
                throw new AssertionError();
        }
    }

    public ContentManagerPojo(OpOrder opOrder)
    {
        super(null, opOrder);
        this.contentArrays = new AtomicReferenceArray[29 - CONTENTS_START_SHIFT];
    }

    @Override
    public T getContent(int id)
    {
        int leadBit = getBufferIdx(id & CONTENT_INDEX_MASK, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(id & CONTENT_INDEX_MASK, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        return array.get(ofs);
    }

    @Override
    public boolean shouldPresentAfterBranch(int contentId)
    {
        return (contentId & CONTENT_AFTER_BRANCH) != 0;
    }

    @Override
    public String dumpContentId(int id)
    {
        return "~" + (id & CONTENT_INDEX_MASK) + ((id & CONTENT_AFTER_BRANCH) != 0 ? "↑" : "");
    }


    /// Allocate a new position in the object array. Used by the memory allocation strategy to allocate a content spot
    /// when it runs out of recycled positions.
    @Override
    public int makeNewSlot()
    {
        int index = contentCount++;
        int leadBit = getBufferIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        if (array == null)
        {
            assert inBufferOffset(index, leadBit, CONTENTS_START_SIZE) == 0 : "Error in content arrays configuration.";
            contentArrays[leadBit] = new AtomicReferenceArray<>(CONTENTS_START_SIZE << leadBit);
        }
        return index;
    }


    @Override
    public int addContent(T value, boolean contentAfterBranch) throws TrieSpaceExhaustedException
    {
        int index = allocate();
        int leadBit = getBufferIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        // no need for a volatile set here; at this point the item is not referenced
        // by any node in the trie, and a volatile set will be made to reference it.
        array.setPlain(ofs, value);
        return formContentId(index, contentAfterBranch);
    }

    private int formContentId(int index, boolean contentAfterBranch)
    {
        return index | (1 << 31) | (contentAfterBranch ? CONTENT_AFTER_BRANCH : 0);
    }

    @Override
    public int setContent(int id, T value)
    {
        int leadBit = getBufferIdx(id & CONTENT_INDEX_MASK, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(id & CONTENT_INDEX_MASK, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        array.set(ofs, value);
        return id;
    }

    @Override
    public void releaseContent(int id)
    {
        recycle(id & CONTENT_INDEX_MASK);
    }

    @Override
    public long usedSizeOffHeap()
    {
        return 0;
    }

    @Override
    public long usedSizeOnHeap()
    {
        return usedObjectSpace() +
               REFERENCE_ARRAY_ON_HEAP_SIZE * getBufferIdx(contentCount, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
    }

    @VisibleForTesting
    long usedObjectSpace()
    {
        return (contentCount - indexCountInPipeline()) * MemoryLayoutSpecification.SPEC.getReferenceSize();
    }

    @Override
    @VisibleForTesting
    public long unusedReservedOnHeapMemory()
    {
        int index = contentCount;
        int leadBit = getBufferIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inBufferOffset(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> contentArray = contentArrays[leadBit];
        long contentOverhead = ((contentArray != null ? contentArray.length() : 0) - ofs);
        contentOverhead += indexCountInPipeline();
        contentOverhead *= MemoryLayoutSpecification.SPEC.getReferenceSize();

        return contentOverhead;
    }

    @Override
    @VisibleForTesting
    public void releaseReferencesUnsafe()
    {
        for (int idx : indexesInPipeline())
            setContent(formContentId(idx, false), null);
    }

    @Override
    public int valuesCount()
    {
        return contentCount;
    }
}
