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

package org.apache.cassandra.utils;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.lucene.util.PriorityQueue;

public class LucenePriorityQueue<T> extends PriorityQueue<T>
{
    final Comparator<? super T> comparator;

    public LucenePriorityQueue(int size, Comparator<? super T> comparator)
    {
        super(size);
        this.comparator = comparator;
    }

    @Override
    protected boolean lessThan(T t, T t1)
    {
        return comparator.compare(t, t1) < 0;
    }

    // Must accept and deal with nulls
    public static class SortingIterator<T> extends LucenePriorityQueue<T> implements Iterator<T>
    {
        public SortingIterator(Comparator<? super T> comparator, Collection<T> sources)
        {
            super(sources.size(), comparator);
            addAll(sources);
        }

        @Override
        protected boolean lessThan(T t, T t1)
        {
            // nulls are placed at the end
            if (t == null || t1 == null)
                return t != null;
            return comparator.compare(t, t1) < 0;
        }

        @Override
        public boolean hasNext()
        {
            return top() != null;
        }

        @Override
        public T next()
        {
            if (!hasNext())
                throw new NoSuchElementException();
            return pop();
        }
    }


    public static class CloseableSortingIterator<T> extends SortingIterator<T> implements CloseableIterator<T>
    {
        final Runnable onClose;

        public <V> CloseableSortingIterator(Comparator<? super T> comparator,
                                            Collection<T> sources,
                                            Runnable onClose)
        {
            super(comparator, sources);
            this.onClose = onClose;
        }

        @Override
        public void close()
        {
            onClose.run();
        }
    }
}
