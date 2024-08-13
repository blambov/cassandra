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
package org.apache.cassandra.index.sai.memory;

import java.io.IOException;
import java.util.SortedSet;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.utils.SortingIterator;

public class KeyRangeIterator extends RangeIterator
{
    private final SortingIterator<PrimaryKey> keys;

    /**
     * An in-memory {@link RangeIterator} that uses a {@link SortedSet} which has no duplication as its backing store.
     */
    public KeyRangeIterator(SortedSet<PrimaryKey> keys)
    {
        super(keys.first(), keys.last(), keys.size());
        this.keys = new SortingIterator<>(keys);
    }

    /**
     * An in-memory {@link RangeIterator} that uses a {@link SortingIterator}.
     */
    public KeyRangeIterator(PrimaryKey min, PrimaryKey max, int size, SortingIterator<PrimaryKey> keys)
    {
        super(min, max, size);
        this.keys = keys;
    }

    protected PrimaryKey computeNext()
    {
        if (!keys.hasNext())
            return endOfData();
        return keys.next();
    }

    protected void performSkipTo(PrimaryKey nextKey)
    {
        keys.skipTo(nextKey);
    }

    public void close() throws IOException
    {}
}
