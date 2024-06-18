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

public abstract class PrefixedCursor<C extends CursorWalkable.Cursor> implements CursorWalkable.Cursor
{
    final Direction direction;
    final C source;
    ByteSource prefixBytes;
    int nextByte;
    int currentByte;
    int depthOfPrefix;

    PrefixedCursor(Direction direction, ByteComparable prefix, C source)
    {
        this.direction = direction;
        this.source = source;
        prefixBytes = prefix.asComparableBytes(TrieImpl.BYTE_COMPARABLE_VERSION);
        currentByte = -1;
        nextByte = prefixBytes.next();
        depthOfPrefix = 0;
    }

    @SuppressWarnings("unchecked")
    PrefixedCursor(PrefixedCursor<C> copyFrom)
    {
        this.direction = copyFrom.direction;
        this.source = (C) copyFrom.source.duplicate();
        nextByte = copyFrom.nextByte;
        currentByte = copyFrom.currentByte;
        depthOfPrefix = copyFrom.depthOfPrefix;
        if (!prefixDone())
            prefixBytes = copyFrom.duplicatePrefix();
        else
            prefixBytes = null;
    }

    @SuppressWarnings("unchecked")
    PrefixedCursor(PrefixedCursor<C> copyFrom, Direction direction)
    {
        assert !prefixDone();
        this.direction = copyFrom.direction;
        this.source = (C) copyFrom.source.duplicate();
        nextByte = copyFrom.nextByte;
        currentByte = -1;
        depthOfPrefix = 0;
        prefixBytes = copyFrom.duplicatePrefix();
    }

    ByteSource duplicatePrefix()
    {
        ByteSource.Duplicatable dupe = ByteSource.duplicatable(prefixBytes);
        prefixBytes = dupe;
        return dupe.duplicate();
    }

    int addPrefixDepthAndCheckDone(int depthInBranch)
    {
        currentByte = source.incomingTransition();
        if (depthInBranch < 0)
            depthOfPrefix = 0;

        return depthInBranch + depthOfPrefix;
    }

    boolean prefixDone()
    {
        return nextByte == ByteSource.END_OF_STREAM;
    }

    @Override
    public int depth()
    {
        if (prefixDone())
            return source.depth() + depthOfPrefix;
        else
            return depthOfPrefix;
    }

    @Override
    public int incomingTransition()
    {
        return currentByte;
    }

    @Override
    public int advance()
    {
        if (prefixDone())
            return addPrefixDepthAndCheckDone(source.advance());

        ++depthOfPrefix;
        currentByte = nextByte;
        nextByte = prefixBytes.next();
        return depthOfPrefix;
    }

    @Override
    public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
    {
        if (prefixDone())
            return addPrefixDepthAndCheckDone(source.advanceMultiple(receiver));

        while (!prefixDone())
        {
            receiver.addPathByte(currentByte);
            ++depthOfPrefix;
            currentByte = nextByte;
            nextByte = prefixBytes.next();
        }
        return depthOfPrefix;
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        // regardless if we exhausted prefix, if caller asks for depth <= prefix depth, we're done.
        if (skipDepth <= depthOfPrefix)
            return exhausted();
        if (prefixDone())
            return addPrefixDepthAndCheckDone(source.skipTo(skipDepth - depthOfPrefix, skipTransition));
        if (skipDepth == depthOfPrefix + 1 && direction.gt(skipTransition, nextByte))
            return exhausted();
        return advance();
    }

    private int exhausted()
    {
        currentByte = -1;
        nextByte = ByteSource.END_OF_STREAM;
        depthOfPrefix = -1;
        return depthOfPrefix;
    }

    public Direction direction()
    {
        return direction;
    }

    static abstract class WithContent<T, C extends TrieImpl.Cursor<T>> extends PrefixedCursor<C> implements TrieImpl.Cursor<T>
    {
        WithContent(Direction direction, ByteComparable prefix, C source)
        {
            super(direction, prefix, source);
        }

        WithContent(WithContent<T, C> copyFrom)
        {
            super(copyFrom);
        }

        WithContent(WithContent<T, C> copyFrom, Direction direction)
        {
            super(copyFrom, direction);
        }

        @Override
        public T content()
        {
            return prefixDone() ? source.content() : null;
        }
    }

    static class Deterministic<T> extends WithContent<T, TrieImpl.Cursor<T>>
    {
        public Deterministic(Direction direction, ByteComparable prefix, TrieImpl.Cursor<T> source)
        {
            super(direction, prefix, source);
        }

        public Deterministic(Deterministic<T> copyFrom)
        {
            super(copyFrom);
        }

        public Deterministic(Deterministic<T> copyFrom, Direction direction)
        {
            super(copyFrom, direction);
        }

        @Override
        public Deterministic<T> duplicate()
        {
            return new Deterministic<>(this);
        }

        @Override
        public TrieImpl.Cursor<T> tailCursor(Direction direction)
        {
            if (prefixDone())
                return source.tailCursor(direction);
            else
                return new Deterministic<>(this, direction);
        }
    }

    static class NonDeterministic<T extends NonDeterministicTrie.Mergeable<T>>
    extends WithContent<T, NonDeterministicTrieImpl.Cursor<T>>
    implements NonDeterministicTrieImpl.Cursor<T>
    {
        public NonDeterministic(Direction direction, ByteComparable prefix, NonDeterministicTrieImpl.Cursor<T> source)
        {
            super(direction, prefix, source);
        }

        public NonDeterministic(NonDeterministic<T> copyFrom)
        {
            super(copyFrom);
        }

        public NonDeterministic(NonDeterministic<T> copyFrom, Direction direction)
        {
            super(copyFrom, direction);
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> alternateBranch()
        {
            return prefixDone() ? source.alternateBranch() : null;
        }

        @Override
        public NonDeterministic<T> duplicate()
        {
            return new NonDeterministic<>(this);
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> tailCursor(Direction direction)
        {
            if (prefixDone())
                return source.tailCursor(direction);
            else
                return new NonDeterministic<>(this, direction);
        }
    }

    static class Range<M extends RangeTrie.RangeMarker<M>>
    extends WithContent<M, RangeTrieImpl.Cursor<M>> implements RangeTrieImpl.Cursor<M>
    {
        public Range(Direction direction, ByteComparable prefix, RangeTrieImpl.Cursor<M> source)
        {
            super(direction, prefix, source);
        }

        public Range(Range<M> copyFrom)
        {
            super(copyFrom);
        }

        public Range(Range<M> copyFrom, Direction direction)
        {
            super(copyFrom, direction);
        }

        @Override
        public M coveringState()
        {
            if (prefixDone() && source.depth() >= 0)
                return source.coveringState();
            return null;
        }

        @Override
        public Range<M> duplicate()
        {
            return new Range<>(this);
        }

        @Override
        public RangeTrieImpl.Cursor<M> tailCursor(Direction direction)
        {
            if (prefixDone())
                return source.tailCursor(direction);
            else
                return new Range<>(this, direction);
        }
    }

    static class TrieSet extends WithContent<TrieSetImpl.RangeState, TrieSetImpl.Cursor> implements TrieSetImpl.Cursor
    {
        public TrieSet(Direction direction, ByteComparable prefix, TrieSetImpl.Cursor source)
        {
            super(direction, prefix, source);
        }

        public TrieSet(TrieSet copyFrom)
        {
            super(copyFrom);
        }

        public TrieSet(TrieSet copyFrom, Direction direction)
        {
            super(copyFrom, direction);
        }

        @Override
        public TrieSetImpl.RangeState state()
        {
            if (prefixDone() && source.depth() >= 0)
                return source.state();
            return TrieSetImpl.RangeState.START_END_PREFIX;
        }

        @Override
        public TrieSetImpl.RangeState coveringState()
        {
            if (prefixDone() && source.depth() >= 0)
                return source.coveringState();
            return TrieSetImpl.RangeState.START_END_PREFIX;
        }

        @Override
        public TrieSet duplicate()
        {
            return new TrieSet(this);
        }

        @Override
        public TrieSetImpl.Cursor tailCursor(Direction direction)
        {
            if (prefixDone())
                return source.tailCursor(direction);
            else
                return new TrieSet(this, direction);
        }
    }

    static class DeletionAware<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends PrefixedCursor.WithContent<T, DeletionAwareTrieImpl.Cursor<T, D>> implements DeletionAwareTrieImpl.Cursor<T, D>
    {

        DeletionAware(Direction direction, ByteComparable prefix, DeletionAwareTrieImpl.Cursor<T, D> source)
        {
            super(direction, prefix, source);
        }

        DeletionAware(DeletionAware<T, D> copyFrom)
        {
            super(copyFrom);
        }

        DeletionAware(DeletionAware<T, D> copyFrom, Direction direction)
        {
            super(copyFrom, direction);
        }

        @Override
        public RangeTrieImpl.Cursor<D> deletionBranch()
        {
            return prefixDone() ? source.deletionBranch() : null;
        }

        @Override
        public DeletionAware<T, D> duplicate()
        {
            return new DeletionAware<>(this);
        }

        @Override
        public DeletionAwareTrieImpl.Cursor<T, D> tailCursor(Direction direction)
        {
            if (prefixDone())
                return source.tailCursor(direction);
            else
                return new DeletionAware<>(this, direction);
        }
    }
}
