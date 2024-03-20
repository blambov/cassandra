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

import java.util.function.BiFunction;
import javax.annotation.Nullable;

abstract class FlexibleMergeCursor<C extends CursorWalkable.Cursor, D extends CursorWalkable.Cursor> implements CursorWalkable.Cursor
{
    final C c1;
    @Nullable
    D c2;

    enum State
    {
        AT_C1,
        AT_C2,
        AT_BOTH,
        C1_ONLY,    // c2 is null
    }
    State state;

    FlexibleMergeCursor(C c1, D c2)
    {
        this.c1 = c1;
        this.c2 = c2;
        state = c2 != null ? State.AT_BOTH : State.C1_ONLY;
    }

    public FlexibleMergeCursor(FlexibleMergeCursor<C, D> copyFrom)
    {
        this.c1 = (C) copyFrom.c1.duplicate();
        this.c2 = copyFrom.c2 != null ? (D) copyFrom.c2.duplicate() : null;
        this.state = copyFrom.state;
    }

    public void addCursor(D c2)
    {
        assert state == State.C1_ONLY : "Attempting to add further cursors to a cursor that already has two sources";
        assert c2.depth() == c1.depth() && c2.incomingTransition() == c1.incomingTransition()
            : "Only cursors positioned at the current position can be added";
        this.c2 = c2;
        this.state = State.AT_BOTH;
    }

    @Override
    public int advance()
    {
        switch (state)
        {
            case C1_ONLY:
                return c1.advance();
            case AT_C1:
                return checkOrder(c1.advance(), c2.depth());
            case AT_C2:
                return checkOrder(c1.depth(), c1.advance());
            case AT_BOTH:
                return checkOrder(c1.advance(), c2.advance());
            default:
                throw new AssertionError();
        }
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        switch (state)
        {
            case C1_ONLY:
                return c1.skipTo(skipDepth, skipTransition);
            case AT_C1:
                return checkOrder(c1.skipTo(skipDepth, skipTransition), c2.maybeSkipTo(skipDepth, skipTransition));
            case AT_C2:
                return checkOrder(c1.maybeSkipTo(skipDepth, skipTransition), c2.skipTo(skipDepth, skipTransition));
            case AT_BOTH:
                return checkOrder(c1.skipTo(skipDepth, skipTransition), c2.skipTo(skipDepth, skipTransition));
            default:
                throw new AssertionError();
        }
    }

    @Override
    public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
    {
        switch (state)
        {
            case C1_ONLY:
                return c1.advanceMultiple(receiver);
            // If we are in a branch that's only covered by one of the sources, we can use its advanceMultiple as it is
            // only different from advance if it takes multiple steps down, which does not change the order of the
            // cursors.
            // Since it might ascend, we still have to check the order after the call.
            case AT_C1:
                return checkOrder(c1.advanceMultiple(receiver), c2.depth());
            case AT_C2:
                return checkOrder(c1.depth(), c1.advanceMultiple(receiver));
            // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
            case AT_BOTH:
                return checkOrder(c1.advance(), c2.advance());
            default:
                throw new AssertionError();
        }
    }

    private int checkOrder(int c1depth, int c2depth)
    {
        if (c1depth > c2depth)
        {
            state = State.AT_C1;
            return c1depth;
        }
        if (c1depth < c2depth)
        {
            state = state.AT_C2;
            return c2depth;
        }
        // c1depth == c2depth
        int c1trans = c1.incomingTransition();
        int c2trans = c2.incomingTransition();
        boolean c1ahead = c1trans > c2trans;
        boolean c2ahead = c1trans < c2trans;
        state = c2ahead ? State.AT_C1 : c1ahead ? State.AT_C2 : State.AT_BOTH;
        return c1depth;
    }

    @Override
    public int depth()
    {
        switch (state)
        {
            case C1_ONLY:
            case AT_C1:
            case AT_BOTH:
                return c1.depth();
            case AT_C2:
                return c2.depth();
            default:
                throw new AssertionError();
        }
    }

    @Override
    public int incomingTransition()
    {
        switch (state)
        {
            case C1_ONLY:
            case AT_C1:
            case AT_BOTH:
                return c1.incomingTransition();
            case AT_C2:
                return c2.incomingTransition();
            default:
                throw new AssertionError();
        }
    }

    static abstract class WithContent<T, C extends TrieImpl.Cursor<T>> extends FlexibleMergeCursor<C, C> implements TrieImpl.Cursor<T>
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
            switch (state)
            {
                case C1_ONLY:
                case AT_C1:
                    return c1.content();
                case AT_C2:
                    return c2.content();
                case AT_BOTH:
                {
                    T mc = c1.content();
                    T nc = c2.content();
                    if (mc == null)
                        return nc;
                    else if (nc == null)
                        return mc;
                    else
                        return resolver.resolve(nc, mc);
                }
                default:
                    throw new AssertionError();
            }
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

    static abstract class WithMappedContent<T, U, C extends TrieImpl.Cursor<T>, D extends TrieImpl.Cursor<U>, Z> extends FlexibleMergeCursor<C, D> implements TrieImpl.Cursor<Z>
    {
        final BiFunction<T, U, Z> resolver;

        WithMappedContent(BiFunction<T, U, Z> resolver, C c1, D c2)
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
            U mc = null;
            T nc = null;
            switch (state)
            {
                case C1_ONLY:
                case AT_C1:
                    nc = c1.content();
                    break;
                case AT_C2:
                    mc = c2.content();
                    break;
                case AT_BOTH:
                    mc = c2.content();
                    nc = c1.content();
                    break;
                default:
                    throw new AssertionError();
            }
            if (nc == null && mc == null)
                return null;
            return resolver.apply(nc, mc);
        }
    }

    static class DeterministicWithMappedContent<T, U, Z> extends WithMappedContent<T, U, TrieImpl.Cursor<T>, TrieImpl.Cursor<U>, Z>
    {
        DeterministicWithMappedContent(BiFunction<T, U, Z> resolver, TrieImpl.Cursor<T> c1, TrieImpl.Cursor<U> c2)
        {
            super(resolver, c1, c2);
        }

        public DeterministicWithMappedContent(DeterministicWithMappedContent<T, U, Z> copyFrom)
        {
            super(copyFrom);
        }

        DeterministicWithMappedContent(BiFunction<T, U, Z> resolver, TrieImpl<T> t1, TrieImpl<U> t2)
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
            M content2 = null;
            M content1 = null;
            switch (state)
            {
                case C1_ONLY:
                case AT_C1:
                    content1 = c1.content();
                    break;
                case AT_C2:
                    content2 = c2.content();
                    break;
                case AT_BOTH:
                    content2 = c2.content();
                    content1 = c1.content();
                    break;
                default:
                    throw new AssertionError();
            }
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

    static class RangeOnTrie<T, M extends RangeTrie.RangeMarker<M>>
    extends FlexibleMergeCursor<TrieImpl.Cursor<T>, RangeTrieImpl.Cursor<M>>
    implements TrieImpl.Cursor<T>
    {
        final BiFunction<T, M, T> resolver;

        RangeOnTrie(BiFunction<T, M, T> resolver, TrieImpl.Cursor<T> c1, RangeTrieImpl.Cursor<M> c2)
        {
            super(c1, c2);
            this.resolver = resolver;
        }

        public RangeOnTrie(RangeOnTrie<T, M> copyFrom)
        {
            super(copyFrom);
            this.resolver = copyFrom.resolver;
        }

        RangeOnTrie(BiFunction<T, M, T> resolver, TrieImpl<T> t1, RangeTrieImpl<M> t2)
        {
            this(resolver, t1.cursor(), t2.cursor());
            assert c1.depth() == 0;
            assert c2.depth() == 0;
        }

        @Override
        public T content()
        {
            M applicableRange = null;
            T content = null;
            switch (state)
            {
                case C1_ONLY:
                    return c1.content();
                case AT_C1:
                    content = c1.content();
                    applicableRange = c2.coveringState();
                    break;
                case AT_C2:
                    return null;
                case AT_BOTH:
                    content = c1.content();
                    applicableRange = c2.content();
                    if (applicableRange == null)
                        applicableRange = c2.coveringState();
                    break;
                default:
                    throw new AssertionError();
            }

            if (applicableRange == null)
                return content;

            return resolver.apply(content, applicableRange);
        }

        @Override
        public RangeOnTrie<T, M> duplicate()
        {
            return new RangeOnTrie<>(this);
        }
    }

}
