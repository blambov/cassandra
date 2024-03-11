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

public class RangeIntersectionCursor<C extends RangeTrieImpl.RangeMarker<C>, D extends RangeTrieImpl.RangeMarker<D>, Z extends RangeTrieImpl.RangeMarker<Z>> implements RangeTrieImpl.Cursor<Z>
{
    interface IntersectionController<C extends RangeTrieImpl.RangeMarker<C>, D extends RangeTrieImpl.RangeMarker<D>, Z extends RangeTrieImpl.RangeMarker<Z>>
    {
        Z combineState(C lState, D rState);

        boolean includeLesserLeft(C lState);
        boolean includeLesserRight(D rState);

        Z combineStateCoveringLeft(D rState, C lCoveringState);
        Z combineStateCoveringRight(C lState, D rCoveringState);
    }

    enum State
    {
        MATCHING,
        C1_AHEAD,
        C2_AHEAD;

        State swap()
        {
            switch(this)
            {
                case C1_AHEAD:
                    return C2_AHEAD;
                case C2_AHEAD:
                    return C1_AHEAD;
                default:
                    throw new AssertionError();
            }
        }
    }

    final IntersectionController<C, D, Z> controller;
    final RangeTrieImpl.Cursor<C> c1;
    final RangeTrieImpl.Cursor<D> c2;
    int currentDepth;
    int currentTransition;
    Z currentRangeState;
    State state;

    public RangeIntersectionCursor(IntersectionController<C, D, Z> controller, RangeTrieImpl.Cursor<C> c1, RangeTrieImpl.Cursor<D> c2)
    {
        this.controller = controller;
        this.c1 = c1;
        this.c2 = c2;
        matchingPosition(0, -1);
    }

    public RangeIntersectionCursor(RangeIntersectionCursor<C, D, Z> copyFrom)
    {
        this.controller = copyFrom.controller;
        this.c1 = copyFrom.c1.duplicate();
        this.c2 = copyFrom.c2.duplicate();
        this.currentDepth = copyFrom.currentDepth;
        this.currentTransition = copyFrom.currentTransition;
        this.currentRangeState = copyFrom.currentRangeState;
        this.state = copyFrom.state;
    }

    @Override
    public int depth()
    {
        return currentDepth;
    }

    @Override
    public int incomingTransition()
    {
        return currentTransition;
    }

    @Override
    public Z state()
    {
        return currentRangeState;
    }

    @Override
    public int advance()
    {
        switch(state)
        {
            case MATCHING:
            {
                int ldepth = c1.advance();
                if (controller.includeLesserLeft(c1.state()))
                    return advanceWithSetAhead(c2.advance(), c2, c1, State.C1_AHEAD);
                else
                    return advanceToIntersection(ldepth, c1, c2, State.C1_AHEAD);
            }
            case C1_AHEAD:
                return advanceWithSetAhead(c2.advance(), c2, c1, state);
            case C2_AHEAD:
                return advanceWithSetAhead(c1.advance(), c1, c2, state);
            default:
                throw new AssertionError();
        }
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        switch(state)
        {
            case MATCHING:
            {
                int ldepth = c1.skipTo(skipDepth, skipTransition);
                if (controller.includeLesserLeft(c1.state()))
                    return advanceWithSetAhead(c2.skipTo(skipDepth, skipTransition), c2, c1, State.C1_AHEAD);
                else
                    return advanceToIntersection(ldepth, c1, c2, State.C1_AHEAD);
            }
            case C1_AHEAD:
                return advanceWithSetAhead(c2.skipTo(skipDepth, skipTransition), c2, c1, state);
            case C2_AHEAD:
                return advanceWithSetAhead(c1.skipTo(skipDepth, skipTransition), c1, c2, state);
            default:
                throw new AssertionError();
        }
    }

    @Override
    public int advanceMultiple(TrieImpl.TransitionsReceiver receiver)
    {
        switch(state)
        {
            case MATCHING:
            {
                // Cannot do multi-advance when cursors are at the same position. Applying advance().
                int ldepth = c1.advance();
                if (controller.includeLesserLeft(c1.state()))
                    return advanceWithSetAhead(c2.advance(), c2, c1, State.C1_AHEAD);
                else
                    return advanceToIntersection(ldepth, c1, c2, State.C1_AHEAD);
            }
            case C1_AHEAD:
                return advanceWithSetAhead(c2.advanceMultiple(receiver), c2, c1, state);
            case C2_AHEAD:
                return advanceWithSetAhead(c1.advanceMultiple(receiver), c1, c2, state);
            default:
                throw new AssertionError();
        }
    }

    boolean lesserInSet(State state)
    {
        switch (state)
        {
            case C1_AHEAD:
                return controller.includeLesserLeft(c1.state());
            case C2_AHEAD:
                return controller.includeLesserRight(c2.state());
            default:
                throw new AssertionError();
        }
    }

    private int advanceWithSetAhead(int advDepth, RangeTrieImpl.Cursor<?> advancing, RangeTrieImpl.Cursor<?> ahead, State state)
    {
        int aheadDepth = ahead.depth();
        int aheadTransition = ahead.incomingTransition();
        int advTransition = advancing.incomingTransition();
        if (advDepth > aheadDepth)
            return coveredAreaWithSetAhead(advDepth, advTransition, state);
        if (advDepth == aheadDepth)
        {
            if (advTransition < aheadTransition)
                return coveredAreaWithSetAhead(advDepth, advTransition, state);
            if (advTransition == aheadTransition)
                return matchingPosition(advDepth, advTransition);
        }

        // Advancing cursor moved beyond the ahead cursor. Check if roles have reversed.
        final State swapped = state.swap();
        if (lesserInSet(swapped))
            return coveredAreaWithSetAhead(aheadDepth, aheadTransition, swapped);
        else
            return advanceToIntersection(advDepth, advancing, ahead, swapped);
    }

    private int advanceToIntersection(int aheadDepth, RangeTrieImpl.Cursor<?> ahead, RangeTrieImpl.Cursor<?> other, State state)
    {
        // at this point ahead is beyond other's position, but outside the covered area.
        int aheadTransition = ahead.incomingTransition();
        while (true)
        {
            // Other is ahead of advancing, but outside the covered area. Skip source to the set's position.
            int otherDepth = other.skipTo(aheadDepth, aheadTransition);
            int otherTransition = other.incomingTransition();
            if (otherDepth == aheadDepth && otherTransition == aheadTransition)
                return matchingPosition(aheadDepth, aheadTransition);
            final State swapped = state.swap();
            if (lesserInSet(swapped))
                return coveredAreaWithSetAhead(aheadDepth, aheadTransition, swapped);

            // otherwise roles have reversed, swap everything and repeat
            aheadDepth = otherDepth;
            aheadTransition = otherTransition;
            state = swapped;
            RangeTrieImpl.Cursor t = ahead;
            ahead = other;
            other = t;
        }
    }

    Z combineWithCovering(State state)
    {
        switch (state)
        {
            case C1_AHEAD:
                return controller.combineStateCoveringLeft(c2.state(), c1.state());
            case C2_AHEAD:
                return controller.combineStateCoveringRight(c1.state(), c2.state());
            default:
                throw new AssertionError();
        }
    }

    private int coveredAreaWithSetAhead(int depth, int transition, State state)
    {
        this.currentDepth = depth;
        this.currentTransition = transition;
        this.currentRangeState = combineWithCovering(state);
        this.state = state;
        return depth;
    }

    private int matchingPosition(int depth, int transition)
    {
        state = State.MATCHING;
        currentDepth = depth;
        currentTransition = transition;
        currentRangeState = controller.combineState(c1.state(), c2.state());
        // TODO: Optimize.... maybe one call for both activeBefore and content
        return depth;
    }

    @Override
    public RangeTrieImpl.Cursor duplicate()
    {
        return new RangeIntersectionCursor(this);
    }

    static class TrieSet extends RangeIntersectionCursor<TrieSetImpl.RangeState, TrieSetImpl.RangeState, TrieSetImpl.RangeState> implements TrieSetImpl.Cursor
    {
        public TrieSet(IntersectionController<TrieSetImpl.RangeState, TrieSetImpl.RangeState, TrieSetImpl.RangeState> controller, RangeTrieImpl.Cursor<TrieSetImpl.RangeState> c1, RangeTrieImpl.Cursor<TrieSetImpl.RangeState> c2)
        {
            super(controller, c1, c2);
        }

        public TrieSet(RangeIntersectionCursor<TrieSetImpl.RangeState, TrieSetImpl.RangeState, TrieSetImpl.RangeState> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public TrieSet duplicate()
        {
            return new TrieSet(this);
        }
    }
}
