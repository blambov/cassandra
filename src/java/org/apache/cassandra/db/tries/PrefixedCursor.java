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

public class PrefixedCursor<C extends CursorWalkable.Cursor> implements CursorWalkable.Cursor
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

    PrefixedCursor(PrefixedCursor<C> copyFrom)
    {
        this.direction = copyFrom.direction;
        this.source = (C) copyFrom.source.duplicate();
        nextByte = copyFrom.nextByte;
        currentByte = copyFrom.currentByte;
        depthOfPrefix = copyFrom.depthOfPrefix;
        if (prefixDone())
        {
            ByteSource.Duplicatable dupe = ByteSource.duplicatable(copyFrom.prefixBytes);
            copyFrom.prefixBytes = dupe;
            prefixBytes = dupe.duplicate();
        }
        else
            prefixBytes = null;
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
        if (prefixDone())
            return addPrefixDepthAndCheckDone(source.skipTo(skipDepth, skipTransition));
        if (skipDepth <= depthOfPrefix || skipDepth == depthOfPrefix + 1 && direction.gt(skipTransition, nextByte))
            return depthOfPrefix = -1;
        return advance();
    }

    @Override
    public CursorWalkable.Cursor duplicate()
    {
        return new PrefixedCursor<>(this);
    }

    public Direction direction()
    {
        return direction;
    }

    static class WithContent<T, C extends TrieImpl.Cursor<T>> extends PrefixedCursor<C> implements TrieImpl.Cursor<T>
    {
        WithContent(Direction direction, ByteComparable prefix, C source)
        {
            super(direction, prefix, source);
        }

        WithContent(WithContent<T, C> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public T content()
        {
            return prefixDone() ? source.content() : null;
        }

        @Override
        public WithContent<T, C> duplicate()
        {
            return new WithContent<>(this);
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

        @Override
        public Deterministic<T> duplicate()
        {
            return new Deterministic<>(this);
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
    }
}
