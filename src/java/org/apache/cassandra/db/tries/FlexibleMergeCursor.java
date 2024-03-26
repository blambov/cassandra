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

    @SuppressWarnings("unchecked")
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

    private int checkOrder(int c1depth, int c2depth)
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
        if (c1depth < c2depth)
        {
            state = State.AT_C2;
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

}
