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

public class IntersectionTrieSet implements TrieSetImpl
{
    final TrieSetImpl set1;
    final TrieSetImpl set2;

    public IntersectionTrieSet(TrieSetImpl set1, TrieSetImpl set2)
    {
        this.set1 = set1;
        this.set2 = set2;
    }

    @Override
    public Cursor cursor()
    {
        return new IntersectionCursor(set1.cursor(), set2.cursor());
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

    static class IntersectionCursor implements Cursor
    {
        final Cursor c1;
        final Cursor c2;
        int currentDepth;
        int currentTransition;
        Contained currentContained;
        State state;

        public IntersectionCursor(Cursor c1, Cursor c2)
        {
            this.c1 = c1;
            this.c2 = c2;
            matchingPosition(0, -1);
        }

        public IntersectionCursor(IntersectionCursor copyFrom)
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

        boolean lesserInSet(Cursor cursor)
        {
            return cursor.contained().lesserInSet();
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

        Contained combineContained(Contained cl, Contained cr)
        {
            if (cl == Contained.OUTSIDE_PREFIX || cr == Contained.OUTSIDE_PREFIX)
                return Contained.OUTSIDE_PREFIX;
            else if (cl == Contained.INSIDE_PREFIX)
                return cr;
            else if (cr == Contained.INSIDE_PREFIX)
                return cl;
            else if (cl == cr)
                return cl;
            else // start and end combination
                return Contained.OUTSIDE_PREFIX;
        }

        @Override
        public Cursor duplicate()
        {
            return new IntersectionCursor(this);
        }
    }
}
