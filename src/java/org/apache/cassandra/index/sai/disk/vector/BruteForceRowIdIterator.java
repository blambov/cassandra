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

package org.apache.cassandra.index.sai.disk.vector;

import java.util.Comparator;
import java.util.NoSuchElementException;

import org.apache.cassandra.index.sai.utils.RowIdWithMeta;
import org.apache.cassandra.index.sai.utils.RowIdWithScore;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.BinaryHeap;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.SortingIterator;


/**
 * An iterator over {@link RowIdWithMeta} that lazily consumes from a {@link SortingIterator} of {@link RowWithApproximateScore}.
 * <p>
 * The idea is that we maintain the same level of accuracy as we would get from a graph search, by re-ranking the top `k``
 * best approximate scores at a time with the full resolution vectors to return the top `limit`.
 * <p>
 * For example, suppose that limit=3 and k=5 and we have ten elements.  After our first re-ranking batch, we can have
 *   ACDEG?????
 * Whenever we read a new element, we will refill the active window with the next element from the approximate score
 * queue. As we pop A, we may encounter a new element G, and re-rank the top 5 elements as
 *    CDEFG????
 * Returning C, we may encounter a new element B and rerank as
 *     BDEFG???
 * leaving B to be returned after C.
 * <p>
 * This illustrates that, also like a graph search, we only guarantee ordering of results within a re-ranking batch,
 * not globally.
 * <p>
 * As an implementation detail, we use a binary heap to maintain state rather than a List and sorting.
 */
public class BruteForceRowIdIterator extends BinaryHeap.WithComparator<RowIdWithScore> implements CloseableIterator<RowIdWithScore>
{
    /**
     * Note: this class has a natural ordering that is inconsistent with equals.
     */
    public static class RowWithApproximateScore implements Comparable<RowWithApproximateScore>
    {
        private final int rowId;
        private final int ordinal;
        private final float appoximateScore;

        public RowWithApproximateScore(int rowId, int ordinal, float appoximateScore)
        {
            this.rowId = rowId;
            this.ordinal = ordinal;
            this.appoximateScore = appoximateScore;
        }

        @Override
        public int compareTo(RowWithApproximateScore o)
        {
            // Inverted comparison to sort in descending order
            return Float.compare(o.appoximateScore, appoximateScore);
        }
    }

    // We use two SortingIterators because we do not need an eager ordering of these results. Depending on how many
    // sstables the query hits and the relative scores of vectors from those sstables, we may not need to return
    // more than the first handful of scores.
    // Priority queue with compressed vector scores
    private final SortingIterator<RowWithApproximateScore> approximateScoreQueue;
    private final JVectorLuceneOnDiskGraph.CloseableReranker reranker;
    private boolean initialized;
    private Object next;

    /**
     * @param approximateScoreQueue A priority queue of rows and their ordinal ordered by their approximate similarity scores
     * @param reranker A function that takes a graph ordinal and returns the exact similarity score
     * @param limit The query limit
     * @param topK The number of vectors to resolve and score before returning results
     */
    public BruteForceRowIdIterator(SortingIterator<RowWithApproximateScore> approximateScoreQueue,
                                   JVectorLuceneOnDiskGraph.CloseableReranker reranker,
                                   int limit,
                                   int topK)
    {
        super(Comparator.naturalOrder(), new Object[topK]);
        this.approximateScoreQueue = approximateScoreQueue;
        this.reranker = reranker;
        this.initialized = false;
        this.next = null;
        assert topK >= limit : "topK must be greater than or equal to limit. Found: " + topK + " < " + limit;
    }

    @Override
    public boolean hasNext()
    {
        if (next == null)
        {
            // Prepare/advance only when the next value is requested.
            if (!initialized)
            {
                for (int i = 0; i < heap.length; ++i)
                    heap[i] = getNextApproximateAndRerank();
                heapify();
                initialized = true;
            }
            else
                replaceTop(getNextApproximateAndRerank());
            // Note that this will be redone if hasNext is called again after it returned false; this is okay because
            // in that case the approximateScoreQueue is empty and we will keep setting next to null and returning false.

            next = peek();
        }
        return next != null;
    }

    @Override
    public RowIdWithScore next()
    {
        if (!hasNext())
            throw new NoSuchElementException("No more elements");

        @SuppressWarnings("unchecked")
        RowIdWithScore result = (RowIdWithScore) next;
        next = null;
        return result;
    }

    private RowIdWithScore getNextApproximateAndRerank()
    {
        if (!approximateScoreQueue.hasNext())
            return null;

        RowWithApproximateScore rowOrdinalScore = approximateScoreQueue.next();
        float score = reranker.similarityTo(rowOrdinalScore.ordinal);
        RowIdWithScore rowIdWithScore = new RowIdWithScore(rowOrdinalScore.rowId, score);
        return rowIdWithScore;
    }

    @Override
    public void close()
    {
        FileUtils.closeQuietly(reranker);
    }
}
