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

import static org.apache.cassandra.db.tries.TrieSet.Contained;
import static org.apache.cassandra.db.tries.TrieSet.Cursor;

public abstract class CombinationTrieSetCursor implements Cursor
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

    final Cursor c1;
    final Cursor c2;
    int currentDepth;
    int currentTransition;
    Contained currentContained;
    State state;

    public CombinationTrieSetCursor(Cursor c1, Cursor c2)
    {
        this.c1 = c1;
        this.c2 = c2;
        matchingPosition(0, -1);
    }

    public CombinationTrieSetCursor(CombinationTrieSetCursor copyFrom)
    {
        this.c1 = copyFrom.c1.duplicate();
        this.c2 = copyFrom.c2.duplicate();
        this.currentDepth = copyFrom.currentDepth;
        this.currentTransition = copyFrom.currentTransition;
        this.currentContained = copyFrom.currentContained;
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
    public Contained contained()
    {
        return currentContained;
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

    private int advanceWithSetAhead(int advDepth, Cursor advancing, Cursor ahead, State state)
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
            return advanceToIntersection(ahead.skipTo(advDepth, advTransition), ahead, advancing, state.swap());
    }

    private int advanceToIntersection(int advDepth, Cursor advancing, Cursor other, State state)
    {
        int advTransition = advancing.incomingTransition();
        while (true)
        {
            // Set is ahead of source, but outside the covered area. Skip source to the set's position.
            int otherDepth = other.skipTo(advDepth, advTransition);
            int otherTransition = other.incomingTransition();
            if (otherDepth == advDepth && otherTransition == advTransition)
                return matchingPosition(advDepth, advTransition);
            if (lesserInSet(other))
                return coveredAreaWithSetAhead(advDepth, advTransition, advancing, state);

            // otherwise roles have reversed, swap everything and repeat
            advDepth = otherDepth;
            advTransition = otherTransition;
            state = state.swap();
            Cursor t = advancing;
            advancing = other;
            other = t;
        }
    }

    private int coveredAreaWithSetAhead(int depth, int transition, Cursor advancing, State state)
    {
        this.currentDepth = depth;
        this.currentTransition = transition;
        this.currentContained = advancing.contained();
        this.state = state;
        return depth;
    }

    private int matchingPosition(int depth, int transition)
    {
        state = State.MATCHING;
        currentDepth = depth;
        currentTransition = transition;
        currentContained = combineContained(c1.contained(), c2.contained());
        return depth;
    }

    abstract boolean lesserInSet(Cursor cursor);

    abstract protected Contained combineContained(Contained cl, Contained cr);
}
