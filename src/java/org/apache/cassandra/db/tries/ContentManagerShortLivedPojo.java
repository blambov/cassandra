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

import org.github.jamm.MemoryLayoutSpecification;

import static org.apache.cassandra.db.tries.ContentManagerPojo.CONTENT_AFTER_BRANCH;
import static org.apache.cassandra.db.tries.ContentManagerPojo.CONTENT_INDEX_MASK;

public class ContentManagerShortLivedPojo<T> implements ContentManager<T>
{
    private int contentCount = 0;
    volatile AtomicReferenceArray<T> contentArray;

    public ContentManagerShortLivedPojo()
    {
        this.contentArray = new AtomicReferenceArray<>(256);
    }

    @Override
    public T getContent(int id)
    {
        return contentArray.get(id & CONTENT_INDEX_MASK);
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
    private int allocateNewObject()
    {
        int index = contentCount++;
        if (index == contentArray.length())
        {
            AtomicReferenceArray<T> newArray = new AtomicReferenceArray(index * 2);
            for (int i = 0; i < index; ++i)
                newArray.lazySet(i, contentArray.get(i));
            contentArray = newArray; // volatile set, makes lazy sets above visible
        }

        return index;
    }


    @Override
    public int addContent(T value, boolean contentAfterBranch) throws TrieSpaceExhaustedException
    {
        int index = allocateNewObject();
        // no need for a volatile set here; at this point the item is not referenced
        // by any node in the trie, and a volatile set will be made to reference it.
        contentArray.setPlain(index, value);
        return formContentId(index, contentAfterBranch);
    }

    private int formContentId(int index, boolean contentAfterBranch)
    {
        return index | (1 << 31) | (contentAfterBranch ? CONTENT_AFTER_BRANCH : 0);
    }

    @Override
    public int setContent(int id, T value)
    {
        contentArray.set(id & CONTENT_INDEX_MASK, value);
        return id;
    }

    @Override
    public void releaseContent(int id)
    {
        // No reuse, do nothing.
        // Note that we can't clear the reference now because the index may still be in use in concurrent readers.
    }

    @Override
    public void completeMutation()
    {
        // No reuse, do nothing
    }

    @Override
    public void abortMutation()
    {
        // No reuse, do nothing
    }

    @Override
    public long usedSizeOffHeap()
    {
        return 0;
    }

    @Override
    public long usedSizeOnHeap()
    {
        return usedObjectSpace();
    }

    @VisibleForTesting
    long usedObjectSpace()
    {
        return contentCount * MemoryLayoutSpecification.SPEC.getReferenceSize();
    }

    @Override
    @VisibleForTesting
    public long unusedReservedOnHeapMemory()
    {
        return 0;
    }

    @Override
    @VisibleForTesting
    public void releaseReferencesUnsafe()
    {
        // as we don't track the released cells, we can't set the pointers to null
        throw new UnsupportedOperationException();
    }

    @Override
    public int valuesCount()
    {
        return contentCount;
    }
}
