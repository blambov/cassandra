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

public class TailCursor<C extends CursorWalkable.Cursor> implements CursorWalkable.Cursor
{
    final C source;
    final int initialDepth;
    int depth;

    TailCursor(C source)
    {
        this.source = source;
        initialDepth = source.depth();
        depth = 0;
    }

    @SuppressWarnings("unchecked")
    TailCursor(TailCursor<C> copyFrom)
    {
        this.source = (C) copyFrom.source.duplicate();
        this.initialDepth = copyFrom.initialDepth;
        this.depth = copyFrom.depth;
    }

    int setTailDepthAndCheckDone(int depthInBranch)
    {
        depth = depthInBranch - initialDepth;
        if (depth <= 0)
            return depth = -1;
        else
            return depth;
    }

    @Override
    public int depth()
    {
        return depth;
    }

    @Override
    public int incomingTransition()
    {
        return depth > 0 ? source.incomingTransition() : -1; // need to return -1 for root too
    }

    @Override
    public int advance()
    {
        return setTailDepthAndCheckDone(source.advance());
    }

    @Override
    public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
    {
        return setTailDepthAndCheckDone(source.advanceMultiple(receiver));
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        return setTailDepthAndCheckDone(source.skipTo(skipDepth <= 0 ? -1 : skipDepth + initialDepth, skipTransition));
    }

    @Override
    public CursorWalkable.Cursor duplicate()
    {
        return new TailCursor<>(this);
    }

    static class WithContent<T, C extends TrieImpl.Cursor<T>> extends TailCursor<C> implements TrieImpl.Cursor<T>
    {
        WithContent(C source)
        {
            super(source);
        }

        WithContent(WithContent<T, C> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public T content()
        {
            return depth < 0 ? null : source.content();
        }

        @Override
        public WithContent<T, C> duplicate()
        {
            return new WithContent<>(this);
        }
    }

    static class Deterministic<T> extends WithContent<T, TrieImpl.Cursor<T>>
    {
        public Deterministic(TrieImpl.Cursor<T> source)
        {
            super(source);
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
        public NonDeterministic(NonDeterministicTrieImpl.Cursor<T> source)
        {
            super(source);
        }

        public NonDeterministic(NonDeterministic<T> copyFrom)
        {
            super((WithContent<T, NonDeterministicTrieImpl.Cursor<T>>) copyFrom);
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> alternateBranch()
        {
            return depth < 0 ? null : source.alternateBranch();
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
        public Range(RangeTrieImpl.Cursor<M> source)
        {
            super(source);
        }

        public Range(Range<M> copyFrom)
        {
            super((WithContent<M, RangeTrieImpl.Cursor<M>>) copyFrom);
        }

        @Override
        public M coveringState()
        {
            return depth < 0 ? null : source.coveringState();
        }

        @Override
        public Range<M> duplicate()
        {
            return new Range<>(this);
        }

        @Override
        public Direction direction()
        {
            return source.direction();
        }
    }

    static class TrieSet extends WithContent<TrieSetImpl.RangeState, TrieSetImpl.Cursor> implements TrieSetImpl.Cursor
    {
        public TrieSet(TrieSetImpl.Cursor source)
        {
            super(source);
        }

        public TrieSet(TrieSet copyFrom)
        {
            super((WithContent<TrieSetImpl.RangeState, TrieSetImpl.Cursor>) copyFrom);
        }

        @Override
        public TrieSetImpl.RangeState state()
        {
            return depth < 0 ? source.coveringState() : source.state();  // set may finish and start active
        }

        @Override
        public TrieSetImpl.RangeState coveringState()
        {
            return source.coveringState();
        }

        @Override
        public TrieSet duplicate()
        {
            return new TrieSet(this);
        }

        @Override
        public Direction direction()
        {
            return source.direction();
        }
    }

    static class DeletionAware<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends TailCursor.WithContent<T, DeletionAwareTrieImpl.Cursor<T, D>> implements DeletionAwareTrieImpl.Cursor<T, D>
    {

        DeletionAware(DeletionAwareTrieImpl.Cursor<T, D> source)
        {
            super(source);
        }

        DeletionAware(DeletionAware<T, D> copyFrom)
        {
            super((WithContent<T, DeletionAwareTrieImpl.Cursor<T, D>>) copyFrom);
        }

        @Override
        public RangeTrieImpl.Cursor<D> deletionBranch()
        {
            return depth < 0 ? null : source.deletionBranch();
        }

        @Override
        public DeletionAware<T, D> duplicate()
        {
            return new DeletionAware<>(this);
        }
    }
}
