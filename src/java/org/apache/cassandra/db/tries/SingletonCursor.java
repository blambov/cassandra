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
    final boolean presentOnReturnPath;
    private long currentPosition;
    protected int nextTransition;


    public SingletonCursor(Direction direction, ByteSource src, ByteComparable.Version byteComparableVersion, boolean presentOnReturnPath, T value)
    {
        this(direction, src.next(), src, byteComparableVersion, presentOnReturnPath, value);
    }

    public SingletonCursor(Direction direction, int firstByte, ByteSource src, ByteComparable.Version byteComparableVersion, boolean presentOnReturnPath, T value)
    {
        this.src = src;
        this.direction = direction;
        this.byteComparableVersion = byteComparableVersion;
        this.value = value;
        this.nextTransition = firstByte;
        this.currentPosition = Cursor.rootPosition(direction);
        this.presentOnReturnPath = presentOnReturnPath;
    }

    @Override
    public long advance()
    {
        if (nextTransition == ByteSource.END_OF_STREAM)
            return doneOrRootReturnPath();

        int currentTransition = nextTransition;
        nextTransition = src.next();
        long returnBit = presentOnReturnPath && (nextTransition == ByteSource.END_OF_STREAM) ? ON_RETURN_PATH_BIT : 0;
        currentPosition = Cursor.positionForDescentWithByte(currentPosition, currentTransition) | returnBit;
        return currentPosition;
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        if (nextTransition == ByteSource.END_OF_STREAM)
            return doneOrRootReturnPath();

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
        long returnBit = presentOnReturnPath ? ON_RETURN_PATH_BIT : 0;
        currentPosition = Cursor.encode(depth + 1, current, direction()) | returnBit;
        nextTransition = next;
        return currentPosition;
    }

    private long doneOrRootReturnPath()
    {
        if (currentPosition == Cursor.rootPosition(direction) && presentOnReturnPath)
            return currentPosition |= ON_RETURN_PATH_BIT;
        return done();
    }

    private long doneOrRootReturnPath(long targetPosition)
    {
        if (currentPosition == Cursor.rootPosition(direction) && presentOnReturnPath)
        {
            currentPosition |= ON_RETURN_PATH_BIT;
            if (Cursor.compare(targetPosition, currentPosition) <= 0)
                return currentPosition;
        }
        return done();
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        if (nextTransition == ByteSource.END_OF_STREAM)
            return doneOrRootReturnPath(encodedSkipPosition);

        long nextPosition = Cursor.positionForDescentWithByte(currentPosition, nextTransition);
        // Accept requests for the return path; we will recheck below.
        if (Cursor.compare(encodedSkipPosition, nextPosition | ON_RETURN_PATH_BIT) > 0)
            return done();

        assert Cursor.depth(encodedSkipPosition) == Cursor.depth(nextPosition)
            : "Invalid advance request to " + Cursor.toString(encodedSkipPosition) +
              " to cursor at " + Cursor.toString(currentPosition);

        nextTransition = src.next();
        long returnBit = presentOnReturnPath && (nextTransition == ByteSource.END_OF_STREAM) ? ON_RETURN_PATH_BIT : 0;
        currentPosition = nextPosition | returnBit;
        if (Cursor.compare(encodedSkipPosition, currentPosition) > 0)
            return done();
        return currentPosition;
    }

    private long done()
    {
        return currentPosition = Cursor.exhaustedPosition(direction);
    }

    protected boolean atEnd()
    {
        return nextTransition == ByteSource.END_OF_STREAM &&
               !Cursor.isExhausted(currentPosition) &&
               (!presentOnReturnPath || Cursor.isOnReturnPath(currentPosition));
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
        return new SingletonCursor<>(dir, nextTransition, duplicateSource(), byteComparableVersion, presentOnReturnPath, value);
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
        public Range(Direction direction, ByteSource src, ByteComparable.Version byteComparableVersion, boolean presentOnReturnPath, S value)
        {
            super(direction, src, byteComparableVersion, presentOnReturnPath, value);
        }

        public Range(Direction direction, int firstByte, ByteSource src, ByteComparable.Version byteComparableVersion, boolean presentOnReturnPath, S value)
        {
            super(direction, firstByte, src, byteComparableVersion, presentOnReturnPath, value);
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
            return new Range<>(dir, nextTransition, duplicateSource(), byteComparableVersion, presentOnReturnPath, value);
        }
    }

    static class DeletionAware<T, D extends RangeState<D>>
    extends SingletonCursor<T> implements DeletionAwareCursor<T, D>
    {
        DeletionAware(Direction direction, ByteSource src, ByteComparable.Version byteComparableVersion, boolean presentOnReturnPath, T value)
        {
            super(direction, src, byteComparableVersion, presentOnReturnPath, value);
        }

        DeletionAware(Direction direction, int firstByte, ByteSource src, ByteComparable.Version byteComparableVersion, boolean presentOnReturnPath, T value)
        {
            super(direction, firstByte, src, byteComparableVersion, presentOnReturnPath, value);
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return null;
        }

        @Override
        public DeletionAware<T, D> tailCursor(Direction dir)
        {
            return new DeletionAware<>(dir, nextTransition, duplicateSource(), byteComparableVersion, presentOnReturnPath, value);
        }
    }

    static class DeletionBranch<T, D extends RangeState<D>>
    extends SingletonCursor<T> implements DeletionAwareCursor<T, D>
    {
        RangeTrie<D> deletionBranch;

        DeletionBranch(Direction direction, ByteSource src, ByteComparable.Version byteComparableVersion, RangeTrie<D> deletionBranch)
        {
            super(direction, src, byteComparableVersion, false, null);
            this.deletionBranch = deletionBranch;
        }

        DeletionBranch(Direction direction, int firstByte, ByteSource src, ByteComparable.Version byteComparableVersion, RangeTrie<D> deletionBranch)
        {
            super(direction, firstByte, src, byteComparableVersion, false, null);
            this.deletionBranch = deletionBranch;
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
