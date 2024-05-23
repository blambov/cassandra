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

class SingletonCursor<T> implements TrieImpl.Cursor<T>
{
    private final Direction direction;
    private ByteSource src;
    private int currentDepth;
    private int currentTransition;
    private int nextTransition;
    private final T value;

    SingletonCursor(Direction direction, ByteComparable key, T value)
    {
        this.direction = direction;
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
        direction = copyFrom.direction;
        currentDepth = copyFrom.currentDepth;
        currentTransition = copyFrom.currentTransition;
        nextTransition = copyFrom.nextTransition;
        value = copyFrom.value;
    }

    private int exhausted()
    {
        currentTransition = -1;
        currentDepth = -1;
        return currentDepth;
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
            return exhausted();
        }
    }

    @Override
    public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
    {
        if (nextTransition == ByteSource.END_OF_STREAM)
            return exhausted();
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
            assert depth < currentDepth || direction.gt(incomingTransition, currentTransition) || depth == -1;
            return exhausted();  // no alternatives
        }
        if (direction.gt(incomingTransition, nextTransition))
            return exhausted();   // request is skipping over our path

        return advance();
    }

    @Override
    public SingletonCursor<T> duplicate()
    {
        return new SingletonCursor<>(this);
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

    public Direction direction()
    {
        return direction;
    }

    static class NonDeterministic<T extends NonDeterministicTrie.Mergeable<T>>
    extends SingletonCursor<T>
    implements NonDeterministicTrieImpl.Cursor<T>
    {
        NonDeterministic(Direction direction, ByteComparable key, T value)
        {
            super(direction, key, value);
        }

        NonDeterministic(SingletonCursor<T> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> alternateBranch()
        {
            return null;
        }

        @Override
        public NonDeterministic<T> duplicate()
        {
            return new NonDeterministic<>(this);
        }
    }

    static class Range<T extends RangeTrie.RangeMarker<T>> extends SingletonCursor<T> implements RangeTrieImpl.Cursor<T>
    {
        Range(Direction direction, ByteComparable key, T value)
        {
            super(direction, key, value);
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
            return new Range<>(this);
        }
    }

    static class DeletionAware<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends SingletonCursor<T> implements DeletionAwareTrieImpl.Cursor<T, D>
    {
        DeletionAware(Direction direction, ByteComparable key, T value)
        {
            super(direction, key, value);
        }

        DeletionAware(SingletonCursor<T> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public RangeTrieImpl.Cursor<D> deletionBranch()
        {
            return null;
        }

        @Override
        public DeletionAware<T, D> duplicate()
        {
            return new DeletionAware<>(this);
        }

    }
}
