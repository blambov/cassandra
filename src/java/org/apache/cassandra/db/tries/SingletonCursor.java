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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

class SingletonCursor<T> implements NonDeterministicTrieImpl.Cursor<T>
{
    private ByteSource src;
    private int currentDepth;
    private int currentTransition;
    private int nextTransition;
    private final T value;

    SingletonCursor(ByteComparable key, T value)
    {
        src = key.asComparableBytes(CursorWalkable.BYTE_COMPARABLE_VERSION);
        this.value = value;
        currentDepth = 0;
        currentTransition = -1;
        nextTransition = src.next();
    }

    SingletonCursor(SingletonCursor<T> copyFrom)
    {
        ByteSource.Duplicatable dupe = ByteSource.duplicatable(copyFrom.src);
        copyFrom.src = dupe;
        src = dupe.duplicate();
        currentDepth = copyFrom.currentDepth;
        currentTransition = copyFrom.currentTransition;
        nextTransition = copyFrom.nextTransition;
        value = copyFrom.value;
    }

    @Override
    public int advance()
    {
        currentTransition = nextTransition;
        if (currentTransition != ByteSource.END_OF_STREAM)
        {
            nextTransition = src.next();
            return ++currentDepth;
        }
        else
        {
            return currentDepth = -1;
        }
    }

    @Override
    public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
    {
        if (nextTransition == ByteSource.END_OF_STREAM)
            return currentDepth = -1;
        int current = nextTransition;
        int depth = currentDepth;
        int next = src.next();
        while (next != ByteSource.END_OF_STREAM)
        {
            if (receiver != null)
                receiver.addPathByte(current);
            current = next;
            next = src.next();
            ++depth;
        }
        currentTransition = current;
        nextTransition = next;
        return currentDepth = ++depth;
    }

    @Override
    public int skipTo(int depth, int incomingTransition)
    {
        if (depth <= currentDepth)
        {
            assert depth < currentDepth || incomingTransition > currentTransition || depth == -1;
            return currentDepth = -1;  // no alternatives
        }
        if (incomingTransition > nextTransition)
            return currentDepth = -1;   // request is skipping over our path

        return advance();
    }

    @Override
    public SingletonCursor<T> alternateBranch()
    {
        return null;
    }

    @Override
    public SingletonCursor<T> duplicate()
    {
        return new SingletonCursor(this);
    }

    @Override
    public int depth()
    {
        return currentDepth;
    }


    @Override
    public T content()
    {
        return nextTransition == ByteSource.END_OF_STREAM ? value : null;
    }

    @Override
    public int incomingTransition()
    {
        return currentTransition;
    }

    static class Range<T extends RangeTrieImpl.RangeMarker<T>> extends SingletonCursor<T> implements RangeTrieImpl.Cursor<T>
    {
        Range(ByteComparable key, T value)
        {
            super(key, value);
        }

        Range(SingletonCursor<T> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public T coveringState()
        {
            // Since the singleton is only active at a single point, we only return a value for the exact position.
            return null;
        }

        @Override
        public Range<T> duplicate()
        {
            return new Range(this);
        }
    }
}
