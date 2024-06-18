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

    final Direction direction;
    final TrieSetImpl.Cursor c1;
    final TrieSetImpl.Cursor c2;
    int currentDepth;
    int currentTransition;
    TrieSetImpl.RangeState currentRangeState;
    State state;

    public TrieSetIntersectionCursor(Direction direction, TrieSetImpl.Cursor c1, TrieSetImpl.Cursor c2)
    {
        this.direction = direction;
        this.c1 = c1;
        this.c2 = c2;
        matchingPosition(0, -1);
    }

    public TrieSetIntersectionCursor(TrieSetIntersectionCursor copyFrom)
    {
        this.direction = copyFrom.direction;
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
    public Direction direction()
    {
        return direction;
    }

    @Override
    public TrieSetImpl.RangeState state()
    {
        return currentRangeState;
    }

    boolean lesserInSet(TrieSetImpl.Cursor cursor)
    {
        return cursor.state().precedingIncluded(direction);
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
                if (lesserInSet(c1))
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
                if (lesserInSet(c1))
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

    private int advanceWithSetAhead(int advDepth, TrieSetImpl.Cursor advancing, TrieSetImpl.Cursor ahead, State state)
    {
        int aheadDepth = ahead.depth();
        int aheadTransition = ahead.incomingTransition();
        int advTransition = advancing.incomingTransition();
        if (advDepth > aheadDepth)
            return coveredAreaWithSetAhead(advDepth, advTransition, advancing, state);
        if (advDepth == aheadDepth)
        {
            if (direction.lt(advTransition, aheadTransition))
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
        int aheadTransition = ahead.incomingTransition();
        while (true)
        {
            // Other is ahead of advancing, but outside the covered area. Skip source to the set's position.
            int otherDepth = other.skipTo(aheadDepth, aheadTransition);
            int otherTransition = other.incomingTransition();
            if (otherDepth == aheadDepth && otherTransition == aheadTransition)
                return matchingPosition(aheadDepth, aheadTransition);
            if (lesserInSet(other))
                return coveredAreaWithSetAhead(aheadDepth, aheadTransition, ahead, state.swap());

            // otherwise roles have reversed, swap everything and repeat
            aheadDepth = otherDepth;
            aheadTransition = otherTransition;
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
        return depth;
    }

    TrieSetImpl.RangeState combineState(TrieSetImpl.RangeState cl, TrieSetImpl.RangeState cr)
    {
        return cl.intersect(cr);
    }

    @Override
    public TrieSetImpl.Cursor duplicate()
    {
        return new TrieSetIntersectionCursor(this);
    }

    @Override
    public TrieSetImpl.Cursor tailCursor(Direction direction)
    {
        switch (state)
        {
            case MATCHING:
                return new TrieSetIntersectionCursor(direction, c1.tailCursor(direction), c2.tailCursor(direction));
            case C1_AHEAD:
                return c2.tailCursor(direction);
            case C2_AHEAD:
                return c1.tailCursor(direction);
            default:
                throw new AssertionError();
        }
    }

    static class UnionCursor extends TrieSetIntersectionCursor
    {
        public UnionCursor(Direction direction, TrieSetImpl.Cursor c1, TrieSetImpl.Cursor c2)
        {
            super(direction, c1, c2);
        }

        public UnionCursor(UnionCursor copyFrom)
        {
            super(copyFrom);
        }

        @Override
        boolean lesserInSet(TrieSetImpl.Cursor cursor)
        {
            return !cursor.state().precedingIncluded(direction);
        }

        @Override
        TrieSetImpl.RangeState combineState(TrieSetImpl.RangeState cl, TrieSetImpl.RangeState cr)
        {
            return cl.union(cr);
        }

        @Override
        public TrieSetImpl.Cursor duplicate()
        {
            return new UnionCursor(this);
        }

        @Override
        public TrieSetImpl.Cursor tailCursor(Direction direction)
        {
            switch (state)
            {
                case MATCHING:
                    return new UnionCursor(direction, c1.tailCursor(direction), c2.tailCursor(direction));
                case C1_AHEAD:
                    return c2.tailCursor(direction);
                case C2_AHEAD:
                    return c1.tailCursor(direction);
                default:
                    throw new AssertionError();
            }
        }
    }
}
