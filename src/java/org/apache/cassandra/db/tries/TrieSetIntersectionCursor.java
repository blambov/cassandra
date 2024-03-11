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

public class TrieSetIntersectionCursor implements TrieSetImpl.Cursor
{
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

    final TrieSetImpl.Cursor c1;
    final TrieSetImpl.Cursor c2;
    int currentDepth;
    int currentTransition;
    TrieSetImpl.RangeState currentRangeState;
    State state;

    public TrieSetIntersectionCursor(TrieSetImpl.Cursor c1, TrieSetImpl.Cursor c2)
    {
        this.c1 = c1;
        this.c2 = c2;
        matchingPosition(0, -1);
    }

    public TrieSetIntersectionCursor(TrieSetIntersectionCursor copyFrom)
    {
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
    public TrieSetImpl.RangeState state()
    {
        return currentRangeState;
    }

    boolean lesserInSet(TrieSetImpl.Cursor cursor)
    {
        return cursor.state().applicableBefore() != null;
    }

    @Override
    public int advance()
    {
        switch(state)
        {
            case MATCHING:
            {
                int ldepth = c1.advance();
                if (lesserInSet(c1))
                    return advanceWithSetAhead(c2.advance(), c2, c1, State.C1_AHEAD);
                else
                    return advanceToIntersection(ldepth, c1, c2, State.C2_AHEAD);
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
                if (lesserInSet(c1))
                    return advanceWithSetAhead(c2.skipTo(skipDepth, skipTransition), c2, c1, State.C1_AHEAD);
                else
                    return advanceToIntersection(ldepth, c1, c2, State.C2_AHEAD);
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
                if (lesserInSet(c1))
                    return advanceWithSetAhead(c2.advance(), c2, c1, State.C1_AHEAD);
                else
                    return advanceToIntersection(ldepth, c1, c2, State.C2_AHEAD);
            }
            case C1_AHEAD:
                return advanceWithSetAhead(c2.advanceMultiple(receiver), c2, c1, state);
            case C2_AHEAD:
                return advanceWithSetAhead(c1.advanceMultiple(receiver), c1, c2, state);
            default:
                throw new AssertionError();
        }
    }

    private int advanceWithSetAhead(int advDepth, TrieSetImpl.Cursor advancing, TrieSetImpl.Cursor ahead, State state)
    {
        int aheadDepth = ahead.depth();
        int aheadTransition = ahead.incomingTransition();
        int advTransition = advancing.incomingTransition();
        if (advDepth > aheadDepth)
            return coveredAreaWithSetAhead(advDepth, advTransition, advancing, state);
        if (advDepth == aheadDepth)
        {
            if (advTransition < aheadTransition)
                return coveredAreaWithSetAhead(advDepth, advTransition, advancing, state);
            if (advTransition == aheadTransition)
                return matchingPosition(advDepth, advTransition);
        }

        // Advancing cursor moved beyond the ahead cursor. Check if roles have reversed.
        if (lesserInSet(advancing))
            return coveredAreaWithSetAhead(aheadDepth, aheadTransition, ahead, state.swap());
        else
            return advanceToIntersection(advDepth, advancing, ahead, state.swap());
    }

    private int advanceToIntersection(int aheadDepth, TrieSetImpl.Cursor ahead, TrieSetImpl.Cursor other, State state)
    {
        // at this point ahead is beyond other's position, but outside the covered area.
        int advTransition = ahead.incomingTransition();
        while (true)
        {
            // Other is ahead of advancing, but outside the covered area. Skip source to the set's position.
            int otherDepth = other.skipTo(aheadDepth, advTransition);
            int otherTransition = other.incomingTransition();
            if (otherDepth == aheadDepth && otherTransition == advTransition)
                return matchingPosition(aheadDepth, advTransition);
            if (lesserInSet(other))
                return coveredAreaWithSetAhead(aheadDepth, advTransition, ahead, state);

            // otherwise roles have reversed, swap everything and repeat
            aheadDepth = otherDepth;
            advTransition = otherTransition;
            state = state.swap();
            TrieSetImpl.Cursor t = ahead;
            ahead = other;
            other = t;
        }
    }

    private int coveredAreaWithSetAhead(int depth, int transition, TrieSetImpl.Cursor advancing, State state)
    {
        this.currentDepth = depth;
        this.currentTransition = transition;
        this.currentRangeState = advancing.state();
        this.state = state;
        return depth;
    }

    private int matchingPosition(int depth, int transition)
    {
        state = State.MATCHING;
        currentDepth = depth;
        currentTransition = transition;
        currentRangeState = combineState(c1.state(), c2.state());
        // TODO: Optimize.... maybe one call for both activeBefore and content
        return depth;
    }

    TrieSetImpl.RangeState combineState(TrieSetImpl.RangeState cl, TrieSetImpl.RangeState cr)
    {
        if (cl == TrieSetImpl.RangeState.OUTSIDE_PREFIX || cr == TrieSetImpl.RangeState.OUTSIDE_PREFIX)
            return TrieSetImpl.RangeState.OUTSIDE_PREFIX;
        else if (cl == TrieSetImpl.RangeState.INSIDE_PREFIX)
            return cr;
        else if (cr == TrieSetImpl.RangeState.INSIDE_PREFIX)
            return cl;
        else if (cl == cr)
            return cl;
        else // start and end combination
            return TrieSetImpl.RangeState.OUTSIDE_PREFIX;
    }

    @Override
    public TrieSetImpl.Cursor duplicate()
    {
        return new TrieSetIntersectionCursor(this);
    }

    static class UnionCursor extends TrieSetIntersectionCursor
    {
        public UnionCursor(TrieSetImpl.Cursor c1, TrieSetImpl.Cursor c2)
        {
            super(c1, c2);
        }

        public UnionCursor(UnionCursor copyFrom)
        {
            super(copyFrom);
        }

        @Override
        boolean lesserInSet(TrieSetImpl.Cursor cursor)
        {
            return cursor.state().applicableBefore() == null;
        }

        @Override
        TrieSetImpl.RangeState combineState(TrieSetImpl.RangeState cl, TrieSetImpl.RangeState cr)
        {
            if (cl == TrieSetImpl.RangeState.INSIDE_PREFIX || cr == TrieSetImpl.RangeState.INSIDE_PREFIX)
                return TrieSetImpl.RangeState.INSIDE_PREFIX;
            else if (cl == TrieSetImpl.RangeState.OUTSIDE_PREFIX)
                return cr;
            else if (cr == TrieSetImpl.RangeState.OUTSIDE_PREFIX)
                return cl;
            else if (cl == cr)
                return cl;
            else // start and end combination
                return TrieSetImpl.RangeState.INSIDE_PREFIX;
        }

        @Override
        public TrieSetImpl.Cursor duplicate()
        {
            return new UnionCursor(this);
        }
    }
}
