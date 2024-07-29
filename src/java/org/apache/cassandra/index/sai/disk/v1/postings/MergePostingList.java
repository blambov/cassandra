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
package org.apache.cassandra.index.sai.disk.v1.postings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Comparator;
import javax.annotation.concurrent.NotThreadSafe;

import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.Merger;
import org.apache.cassandra.utils.Reducer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Merges multiple {@link PostingList} which individually contain unique items into a single list.
 */
@NotThreadSafe
public class MergePostingList implements PostingList
{
    final ArrayList<PeekablePostingList> postingLists;
    Merger<Integer, PeekablePostingList, Integer> pq;
    final int size;

    private MergePostingList(ArrayList<PeekablePostingList> postingLists)
    {
        checkArgument(!postingLists.isEmpty());
        this.postingLists = postingLists;
        long totalPostings = 0;
        for (PostingList postingList : postingLists)
        {
            totalPostings += postingList.size();
        }
        // We could technically "overflow" integer if enough row ids are duplicated in the source posting lists.
        // The size does not affect correctness, so just use integer max if that happens.
        this.size = (int) Math.min(totalPostings, Integer.MAX_VALUE);
        this.pq = new Merger<>(postingLists,
                               MergePostingList::getNextItem,
                               MergePostingList::advanceList,
                               x -> {},
                               Integer::compare,
                               Reducer.getIdentity());
    }

    public static PostingList merge(ArrayList<PeekablePostingList> postings)
    {
        if (postings.isEmpty())
            return PostingList.EMPTY;

        if (postings.size() == 1)
            return postings.get(0);

        return new MergePostingList(postings);
    }

    static int getNextItem(PostingList postingList)
    {
        try
        {
            return postingList.nextPosting();
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    static int advanceList(PeekablePostingList postingList, Integer position)
    {
        try
        {
            return postingList.advanceWithoutConsuming(position);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    @Override
    public int nextPosting() throws IOException
    {
        return pq.next();
    }

    @SuppressWarnings("resource")
    @Override
    public int advance(int targetRowID) throws IOException
    {
        pq.advanceTo(targetRowID);
        return nextPosting();
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.close(postingLists);
    }
}
