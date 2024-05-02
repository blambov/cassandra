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
    final Direction direction;
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

    FlexibleMergeCursor(Direction direction, C c1, D c2)
    {
        this.direction = direction;
        this.c1 = c1;
        this.c2 = c2;
        state = c2 != null ? State.AT_BOTH : State.C1_ONLY;
    }

    @SuppressWarnings("unchecked")
    public FlexibleMergeCursor(FlexibleMergeCursor<C, D> copyFrom)
    {
        this.direction = copyFrom.direction;
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

    public boolean hasSecondCursor()
    {
        return state != State.C1_ONLY;
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
                return checkOrder(c1.depth(), c2.advance());
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
                return checkOrder(c1.depth(), c2.advanceMultiple(receiver));
            // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
            case AT_BOTH:
                return checkOrder(c1.advance(), c2.advance());
            default:
                throw new AssertionError();
        }
    }

    int checkOrder(int c1depth, int c2depth)
    {
        if (c1depth > c2depth)
        {
            if (c2depth < 0)
            {
                c2 = null;
                state = State.C1_ONLY;
            }
            else
                state = State.AT_C1;
            return c1depth;
        }
        if (direction.lt(c1depth, c2depth))
        {
            state = State.AT_C2;
            return c2depth;
        }
        // c1depth == c2depth
        int c1trans = c1.incomingTransition();
        int c2trans = c2.incomingTransition();
        boolean c1ahead = direction.gt(c1trans, c2trans);
        boolean c2ahead = direction.lt(c1trans, c2trans);
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

    static abstract class WithMappedContent<T, U, C extends TrieImpl.Cursor<T>, D extends TrieImpl.Cursor<U>, Z> extends FlexibleMergeCursor<C, D> implements TrieImpl.Cursor<Z>
    {
        final BiFunction<T, U, Z> resolver;

        WithMappedContent(Direction direction, BiFunction<T, U, Z> resolver, C c1, D c2)
        {
            super(direction, c1, c2);
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

    static class RangeOnTrie<T, D extends RangeTrie.RangeMarker<D>, C extends TrieImpl.Cursor<T>>
    extends FlexibleMergeCursor<C, RangeTrieImpl.Cursor<D>>
    implements TrieImpl.Cursor<T>
    {
        final BiFunction<D, T, T> resolver;

        RangeOnTrie(Direction direction, BiFunction<D, T, T> resolver, C c1, RangeTrieImpl.Cursor<D> c2)
        {
            super(direction, c1, c2);
            this.resolver = resolver;
        }

        public RangeOnTrie(RangeOnTrie<T, D, C> copyFrom)
        {
            super(copyFrom);
            this.resolver = copyFrom.resolver;
        }

        // The advancement methods are overridden so that we skip all sections where only c2 is active.
        @Override
        public int advance()
        {
            switch (state)
            {
                case C1_ONLY:
                    return c1.advance();
                case AT_C1:
                {
                    int c1depth = c1.advance();
                    int c1trans = c1.incomingTransition();
                    return checkOrder(c1depth, c2.maybeSkipTo(c1depth, c1trans));
                }
                case AT_BOTH:
                {
                    int c1depth = c1.advance();
                    int c1trans = c1.incomingTransition();
                    return checkOrder(c1depth, c2.skipTo(c1depth, c1trans));
                }
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
                {
                    int c1depth = c1.skipTo(skipDepth, skipTransition);
                    int c1trans = c1.incomingTransition();
                    return checkOrder(c1depth, c2.maybeSkipTo(c1depth, c1trans));
                }
                case AT_BOTH:
                {
                    int c1depth = c1.skipTo(skipDepth, skipTransition);
                    int c1trans = c1.incomingTransition();
                    return checkOrder(c1depth, c2.skipTo(c1depth, c1trans));
                }
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
                {
                    int c1depth = c1.advanceMultiple(receiver);
                    int c1trans = c1.incomingTransition();
                    return checkOrder(c1depth, c2.maybeSkipTo(c1depth, c1trans));
                }
                // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
                case AT_BOTH:
                {
                    int c1depth = c1.advance();
                    int c1trans = c1.incomingTransition();
                    return checkOrder(c1depth, c2.skipTo(c1depth, c1trans));
                }
                default:
                    throw new AssertionError();
            }
        }

        @Override
        int checkOrder(int c1depth, int c2depth)
        {
            if (c1depth > c2depth)
            {
                if (c2depth < 0)
                {
                    c2 = null;
                    state = State.C1_ONLY;
                }
                else
                    state = State.AT_C1;
                return c1depth;
            }

            assert c1depth == c2depth;
            int c1trans = c1.incomingTransition();
            int c2trans = c2.incomingTransition();
            assert direction.le(c1trans, c2trans);
            boolean c2ahead = direction.lt(c1trans, c2trans);
            state = c2ahead ? State.AT_C1 : State.AT_BOTH;
            return c1depth;
        }

        @Override
        public T content()
        {
            D applicableRange;
            T content;
            switch (state)
            {
                case C1_ONLY:
                    return c1.content();
                case AT_C1:
                    content = c1.content();
                    if (content == null)
                        return null;
                    applicableRange = c2.coveringState();
                    break;
                case AT_BOTH:
                    content = c1.content();
                    if (content == null)
                        return null;
                    applicableRange = c2.content();
                    if (applicableRange == null)
                        applicableRange = c2.coveringState();
                    break;
                default:
                    throw new AssertionError();
            }

            if (applicableRange == null)
                return content;

            return resolver.apply(applicableRange, content);
        }

        @Override
        public RangeOnTrie<T, D, C> duplicate()
        {
            return new RangeOnTrie<>(this);
        }
    }

    static class DeletionAwareSource<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends RangeOnTrie<T, D, DeletionAwareTrieImpl.Cursor<T, D>> implements DeletionAwareTrieImpl.Cursor<T, D>
    {

        DeletionAwareSource(Direction direction, DeletionAwareTrieImpl.Cursor<T, D> c1, BiFunction<D, T, T> resolver)
        {
            super(direction, resolver, c1, null);
        }

        public DeletionAwareSource(DeletionAwareSource<T, D> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public RangeTrieImpl.Cursor<D> deletionBranch()
        {
            // No processing, MergeCursor will handle that.
            return c1.deletionBranch();
        }

        @Override
        public DeletionAwareSource<T, D> duplicate()
        {
            return new DeletionAwareSource<>(this);
        }
    }
}
