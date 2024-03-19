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

abstract class MergeCursor<C extends CursorWalkable.Cursor, D extends CursorWalkable.Cursor> implements CursorWalkable.Cursor
{
    final C c1;
    final D c2;

    boolean atC1;
    boolean atC2;

    MergeCursor(C c1, D c2)
    {
        this.c1 = c1;
        this.c2 = c2;
        atC1 = atC2 = true;
    }

    public MergeCursor(MergeCursor<C, D> copyFrom)
    {
        this.c1 = (C) copyFrom.c1.duplicate();
        this.c2 = (D) copyFrom.c2.duplicate();
        this.atC1 = copyFrom.atC1;
        this.atC2 = copyFrom.atC2;
    }

    @Override
    public int advance()
    {
        return checkOrder(atC1 ? c1.advance() : c1.depth(),
                          atC2 ? c2.advance() : c2.depth());
    }

    @Override
    public int skipTo(int depth, int incomingTransition)
    {
        int c1depth = c1.depth();
        int c2depth = c2.depth();
        assert depth <= c1depth + 1 || depth <= c2depth + 1;
        if (atC1 || depth < c1depth || depth == c1depth && incomingTransition > c1.incomingTransition())
            c1depth = c1.skipTo(depth, incomingTransition);
        if (atC2 || depth < c2depth || depth == c2depth && incomingTransition > c2.incomingTransition())
            c2depth = c2.skipTo(depth, incomingTransition);

        return checkOrder(c1depth, c2depth);
    }

    @Override
    public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
    {
        // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
        if (atC1 && atC2)
            return checkOrder(c1.advance(), c2.advance());

        // If we are in a branch that's only covered by one of the sources, we can use its advanceMultiple as it is
        // only different from advance if it takes multiple steps down, which does not change the order of the
        // cursors.
        // Since it might ascend, we still have to check the order after the call.
        if (atC1)
            return checkOrder(c1.advanceMultiple(receiver), c2.depth());
        else // atC2
            return checkOrder(c1.depth(), c2.advanceMultiple(receiver));
    }

    private int checkOrder(int c1depth, int c2depth)
    {
        if (c1depth > c2depth)
        {
            atC1 = true;
            atC2 = false;
            return c1depth;
        }
        if (c1depth < c2depth)
        {
            atC1 = false;
            atC2 = true;
            return c2depth;
        }
        // c1depth == c2depth
        int c1trans = c1.incomingTransition();
        int c2trans = c2.incomingTransition();
        atC1 = c1trans <= c2trans;
        atC2 = c1trans >= c2trans;
        return c1depth;
    }

    @Override
    public int depth()
    {
        return atC1 ? c1.depth() : c2.depth();
    }

    @Override
    public int incomingTransition()
    {
        return atC1 ? c1.incomingTransition() : c2.incomingTransition();
    }

    static abstract class WithContent<T, C extends TrieImpl.Cursor<T>> extends MergeCursor<C, C> implements TrieImpl.Cursor<T>
    {
        final Trie.MergeResolver<T> resolver;

        WithContent(Trie.MergeResolver<T> resolver, C c1, C c2)
        {
            super(c1, c2);
            this.resolver = resolver;
        }

        public WithContent(WithContent<T, C> copyFrom)
        {
            super(copyFrom);
            this.resolver = copyFrom.resolver;
        }

        @Override
        public T content()
        {
            T mc = atC2 ? c2.content() : null;
            T nc = atC1 ? c1.content() : null;
            if (mc == null)
                return nc;
            else if (nc == null)
                return mc;
            else
                return resolver.resolve(nc, mc);
        }
    }

    static class Deterministic<T> extends WithContent<T, TrieImpl.Cursor<T>>
    {
        Deterministic(Trie.MergeResolver<T> resolver, TrieImpl.Cursor<T> c1, TrieImpl.Cursor<T> c2)
        {
            super(resolver, c1, c2);
        }

        public Deterministic(Deterministic<T> copyFrom)
        {
            super(copyFrom);
        }

        Deterministic(Trie.MergeResolver<T> resolver, TrieImpl<T> t1, TrieImpl<T> t2)
        {
            this(resolver, t1.cursor(), t2.cursor());
            assert c1.depth() == 0;
            assert c2.depth() == 0;
        }


        @Override
        public Deterministic<T> duplicate()
        {
            return new Deterministic<>(this);
        }
    }

    static class NonDeterministic<T> extends WithContent<T, NonDeterministicTrieImpl.Cursor<T>> implements NonDeterministicTrieImpl.Cursor<T>
    {
        NonDeterministic(Trie.MergeResolver<T> resolver, NonDeterministicTrieImpl.Cursor<T> c1, NonDeterministicTrieImpl.Cursor<T> c2)
        {
            super(resolver, c1, c2);
        }

        public NonDeterministic(NonDeterministic<T> copyFrom)
        {
            super(copyFrom);
        }

        NonDeterministic(Trie.MergeResolver<T> resolver, NonDeterministicTrieImpl<T> t1, NonDeterministicTrieImpl<T> t2)
        {
            this(resolver, t1.cursor(), t2.cursor());
            assert c1.depth() == 0;
            assert c2.depth() == 0;
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> alternateBranch()
        {
            var ac1 = atC1 ? c1.alternateBranch() : null;
            var ac2 = atC2 ? c2.alternateBranch() : null;
            if (ac1 == null)
                return ac2; // may be null
            if (ac2 == null)
                return ac1;
            return new NonDeterministic<>(resolver, ac1, ac2);
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> duplicate()
        {
            return new NonDeterministic<>(this);
        }
    }

    interface MappedResolver<T, U, Z>
    {
        Z resolve(T t, U u);
    }

    static abstract class WithMappedContent<T, U, C extends TrieImpl.Cursor<T>, D extends TrieImpl.Cursor<U>, Z> extends MergeCursor<C, D> implements TrieImpl.Cursor<Z>
    {
        final MappedResolver<T, U, Z> resolver;

        WithMappedContent(MappedResolver<T, U, Z> resolver, C c1, D c2)
        {
            super(c1, c2);
            this.resolver = resolver;
        }

        public WithMappedContent(WithMappedContent<T, U, C, D, Z> copyFrom)
        {
            super(copyFrom);
            this.resolver = copyFrom.resolver;
        }

        @Override
        public Z content()
        {
            U mc = atC2 ? c2.content() : null;
            T nc = atC1 ? c1.content() : null;
            return resolver.resolve(nc, mc);
        }
    }

    static class DeterministicWithMappedContent<T, U, Z> extends WithMappedContent<T, U, TrieImpl.Cursor<T>, TrieImpl.Cursor<U>, Z>
    {
        DeterministicWithMappedContent(MappedResolver<T, U, Z> resolver, TrieImpl.Cursor<T> c1, TrieImpl.Cursor<U> c2)
        {
            super(resolver, c1, c2);
        }

        public DeterministicWithMappedContent(DeterministicWithMappedContent<T, U, Z> copyFrom)
        {
            super(copyFrom);
        }

        DeterministicWithMappedContent(MappedResolver<T, U, Z> resolver, TrieImpl<T> t1, TrieImpl<U> t2)
        {
            this(resolver, t1.cursor(), t2.cursor());
            assert c1.depth() == 0;
            assert c2.depth() == 0;
        }

        @Override
        public DeterministicWithMappedContent<T, U, Z> duplicate()
        {
            return new DeterministicWithMappedContent<>(this);
        }
    }

    static class Range<M extends RangeTrie.RangeMarker<M>> extends WithContent<M, RangeTrieImpl.Cursor<M>> implements RangeTrieImpl.Cursor<M>
    {
        Range(Trie.MergeResolver<M> resolver, RangeTrieImpl.Cursor<M> c1, RangeTrieImpl.Cursor<M> c2)
        {
            super(resolver, c1, c2);
        }

        public Range(Range<M> copyFrom)
        {
            super(copyFrom);
        }

        Range(Trie.MergeResolver<M> resolver, RangeTrieImpl<M> t1, RangeTrieImpl<M> t2)
        {
            this(resolver, t1.cursor(), t2.cursor());
            assert c1.depth() == 0;
            assert c2.depth() == 0;
        }

        @Override
        public M coveringState()
        {
            M state1 = c1.coveringState();
            M state2 = c2.coveringState();
            if (state1 == null)
                return state2;
            if (state2 == null)
                return state1;
            return resolver.resolve(state1, state2);
        }

        @Override
        public M content()
        {
            M content1 = atC1 ? c1.content() : null;
            M content2 = atC2 ? c2.content() : null;
            if (content1 == null && content2 == null)
                return null;
            if (content1 != null && content2 != null)
                return resolver.resolve(content1, content2);

            // Exactly one is non-null; must apply the state of the other
            if (content1 == null)
            {
                content1 = c1.coveringState();
                if (content1 == null)
                    return content2;
            } else // content2 == null
            {
                content2 = c2.coveringState();
                if (content2 == null)
                    return content1;
            }

            return resolver.resolve(content1, content2);
        }

        @Override
        public RangeTrieImpl.Cursor<M> duplicate()
        {
            return new Range<>(this);
        }
    }

}
