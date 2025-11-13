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

/// Trie cursor for a singleton trie, mapping a given key to a value.
class SingletonCursor<T> implements Cursor<T>
{
    private final Direction direction;
    ByteSource src;
    final ByteComparable.Version byteComparableVersion;
    final T value;
    private long currentPosition = ROOT_POSITION;
    protected int nextTransition;


    public SingletonCursor(Direction direction, ByteSource src, ByteComparable.Version byteComparableVersion, T value)
    {
        this(direction, src.next(), src, byteComparableVersion, value);
    }

    public SingletonCursor(Direction direction, int firstByte, ByteSource src, ByteComparable.Version byteComparableVersion, T value)
    {
        this.src = src;
        this.direction = direction;
        this.byteComparableVersion = byteComparableVersion;
        this.value = value;
        this.nextTransition = firstByte;
    }

    @Override
    public long advance()
    {
        currentPosition = Cursor.encode(Cursor.depth(currentPosition) + 1, nextTransition, direction())
                          | FLAG_HAS_CHILDREN;
        if (nextTransition != ByteSource.END_OF_STREAM)
        {
            nextTransition = src.next();
            if (nextTransition == ByteSource.END_OF_STREAM)
                currentPosition |= FLAG_HAS_CONTENT;
            return currentPosition | FLAG_DESCENDED;
        }
        else
        {
            return done();
        }
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        if (nextTransition == ByteSource.END_OF_STREAM)
            return done();
        int current = nextTransition;
        int depth = Cursor.depth(currentPosition);
        int next = src.next();
        while (next != ByteSource.END_OF_STREAM)
        {
            if (receiver != null)
                receiver.addPathByte(current);
            current = next;
            next = src.next();
            ++depth;
        }
        currentPosition = Cursor.encode(depth + 1, current, direction())
                          | FLAG_HAS_CHILDREN
                          | FLAG_HAS_CONTENT;
        nextTransition = next;
        return currentPosition | FLAG_DESCENDED;
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        long current = currentPosition;
        if (Cursor.compare(encodedSkipPosition, current) > 0)
            return done();  // no alternatives

        assert Cursor.depth(encodedSkipPosition) == Cursor.depth(current) + 1;
        if (direction.encodeTransitionByte(nextTransition) < Cursor.undecodedTransition(encodedSkipPosition))
            return done();   // request is skipping over our path

        return advance();
    }

    private long done()
    {
        return currentPosition = EXHAUSTED_POSITION;
    }

    protected boolean atEnd()
    {
        return nextTransition == ByteSource.END_OF_STREAM && !Cursor.isExhausted(currentPosition);
    }

    @Override
    public T content()
    {
        return atEnd() ? value : null;
    }

    @Override
    public long encodedPosition()
    {
        return currentPosition;
    }

    @Override
    public Direction direction()
    {
        return direction;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return byteComparableVersion;
    }

    @Override
    public SingletonCursor<T> tailCursor(Direction dir)
    {
        return new SingletonCursor<>(dir, nextTransition, duplicateSource(), byteComparableVersion, value);
    }

    ByteSource.Duplicatable duplicateSource()
    {
        if (!(src instanceof ByteSource.Duplicatable))
            src = ByteSource.duplicatable(src);
        ByteSource.Duplicatable duplicatableSource = (ByteSource.Duplicatable) src;
        return duplicatableSource.duplicate();
    }

    static class Range<S extends RangeState<S>> extends SingletonCursor<S> implements RangeCursor<S>
    {
        public Range(Direction direction, ByteSource src, ByteComparable.Version byteComparableVersion, S value)
        {
            super(direction, src, byteComparableVersion, value);
        }

        public Range(Direction direction, int firstByte, ByteSource src, ByteComparable.Version byteComparableVersion, S value)
        {
            super(direction, firstByte, src, byteComparableVersion, value);
        }

        @Override
        public S precedingState()
        {
            return null;
        }

        @Override
        public S state()
        {
            return content();
        }

        @Override
        public Range<S> tailCursor(Direction dir)
        {
            return new Range<>(dir, nextTransition, duplicateSource(), byteComparableVersion, value);
        }
    }

    static class DeletionAware<T, D extends RangeState<D>>
    extends SingletonCursor<T> implements DeletionAwareCursor<T, D>
    {
        DeletionAware(Direction direction, ByteSource src, ByteComparable.Version byteComparableVersion, T value)
        {
            super(direction, src, byteComparableVersion, value);
        }

        DeletionAware(Direction direction, int firstByte, ByteSource src, ByteComparable.Version byteComparableVersion, T value)
        {
            super(direction, firstByte, src, byteComparableVersion, value);
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return null;
        }

        @Override
        public DeletionAware<T, D> tailCursor(Direction dir)
        {
            return new DeletionAware<>(dir, nextTransition, duplicateSource(), byteComparableVersion, value);
        }
    }

    static class DeletionBranch<T, D extends RangeState<D>>
    extends SingletonCursor<T> implements DeletionAwareCursor<T, D>
    {
        RangeTrie<D> deletionBranch;

        DeletionBranch(Direction direction, ByteSource src, ByteComparable.Version byteComparableVersion, RangeTrie<D> deletionBranch)
        {
            super(direction, src, byteComparableVersion, null);
            this.deletionBranch = deletionBranch;
        }

        DeletionBranch(Direction direction, int firstByte, ByteSource src, ByteComparable.Version byteComparableVersion, RangeTrie<D> deletionBranch)
        {
            super(direction, firstByte, src, byteComparableVersion, null);
            this.deletionBranch = deletionBranch;
            // TODO: Set alternate branch flag
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return atEnd() ? deletionBranch.cursor(direction) : null;
        }

        @Override
        public DeletionBranch<T, D> tailCursor(Direction dir)
        {
            return new DeletionBranch<>(dir, nextTransition, duplicateSource(), byteComparableVersion, deletionBranch);
        }
    }
}
