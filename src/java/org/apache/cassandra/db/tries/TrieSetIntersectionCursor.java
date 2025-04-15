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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Implementation of the intersection of two sets.
public class TrieSetIntersectionCursor implements TrieSetCursor
{
    enum State
    {
        MATCHING,
        C1_AHEAD,
        C1_COVERED_BRANCH,
        C2_AHEAD,
        C2_COVERED_BRANCH;

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

        State swapAndConvertCoveringToAhead()
        {
            switch(this)
            {
                case C1_COVERED_BRANCH:
                    return C2_AHEAD;
                case C2_COVERED_BRANCH:
                    return C1_AHEAD;
                default:
                    throw new AssertionError();
            }
        }
    }

    final Direction direction;
    final TrieSetCursor c1;
    final TrieSetCursor c2;
    int currentDepth;
    int currentTransition;
    TrieSetCursor.RangeState currentRangeState;
    State state;

    public TrieSetIntersectionCursor(TrieSetCursor c1, TrieSetCursor c2)
    {
        this.direction = c1.direction();
        this.c1 = c1;
        this.c2 = c2;
        matchingPosition(0, -1);
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
    public ByteComparable.Version byteComparableVersion()
    {
        return c1.byteComparableVersion();
    }

    @Override
    public TrieSetCursor.RangeState state()
    {
        return currentRangeState;
    }

    /// Whether the preceding positions are in the set. Overridden by [UnionCursor].
    boolean precedingInSet(TrieSetCursor cursor)
    {
        return cursor.precedingIncluded();
    }

    @Override
    public int advance()
    {
        switch(state)
        {
            case MATCHING:
            {
                int ldepth = c1.advance();
                if (precedingInSet(c1))
                    return advanceWithSetAhead(c2.advance(), c2, c1, State.C1_AHEAD);
                else
                    return advanceToIntersection(ldepth, c1, c2, State.C1_AHEAD);
            }
            case C1_AHEAD:
                return advanceWithSetAhead(c2.advance(), c2, c1, state);
            case C1_COVERED_BRANCH:
                return advanceWithSetCovering(c2.advance(), c2, c1, state);
            case C2_AHEAD:
                return advanceWithSetAhead(c1.advance(), c1, c2, state);
            case C2_COVERED_BRANCH:
                return advanceWithSetCovering(c1.advance(), c1, c2, state);
            default:
                throw new AssertionError();
        }
    }

    /// Called to check the state and carry out any necessary advances in the case when the `ahead` cursor was known to
    /// be ahead before an operation was carried out to advance the `advancing` cursor.
    private int advanceWithSetAhead(int advDepth, TrieSetCursor advancing, TrieSetCursor ahead, State state)
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
        if (precedingInSet(advancing))
            return coveredAreaWithSetAhead(aheadDepth, aheadTransition, ahead, state.swap());
        else
            return advanceToIntersection(advDepth, advancing, ahead, state.swap());
    }

    /// Called to check the state and carry out any necessary advances in the case when the `ahead` cursor was known to
    /// be covering the other branch before an operation was carried out to advance the `advancing` cursor.
    private int advanceWithSetCovering(int advDepth, TrieSetCursor advancing, TrieSetCursor ahead, State state)
    {
        int aheadDepth = ahead.depth();
        if (advDepth > aheadDepth)
            return coveredAreaWithSetAhead(advDepth, advancing.incomingTransition(), advancing, state);

        // Advancing cursor moved beyond the ahead cursor. Check if roles have reversed.
        if (advancing.precedingIncluded())
        {
            return coveredAreaWithSetAhead(ahead.advance(), ahead.incomingTransition(), ahead, state.swapAndConvertCoveringToAhead());
        }
        else
            return advanceToIntersection(advDepth, advancing, ahead, state.swapAndConvertCoveringToAhead());
    }

    // Sets don't implement advanceMultiple as they are only meant to limit data tries.

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        switch(state)
        {
            case MATCHING:
                return skipBoth(skipDepth, skipTransition);
            case C1_AHEAD:
                return skipWithSetAhead(skipDepth, skipTransition, c1, c2);
            case C1_COVERED_BRANCH:
                return skipWithSetCovering(skipDepth, skipTransition, c1, c2);
            case C2_AHEAD:
                return skipWithSetAhead(skipDepth, skipTransition, c2, c1);
            case C2_COVERED_BRANCH:
                return skipWithSetCovering(skipDepth, skipTransition, c2, c1);
            default:
                throw new AssertionError();
        }
    }

    private int skipBoth(int skipDepth, int skipTransition)
    {
        int ldepth = c1.skipTo(skipDepth, skipTransition);
        if (precedingInSet(c1))
            return advanceWithSetAhead(c2.skipTo(skipDepth, skipTransition), c2, c1, State.C1_AHEAD);
        else
            return advanceToIntersection(ldepth, c1, c2, State.C1_AHEAD);
    }

    private int skipWithSetAhead(int skipDepth, int skipTransition, TrieSetCursor ahead, TrieSetCursor other)
    {
        // if the cursor ahead is at the skip point or beyond, we can advance the other cursor to the skip point
        int aheadDepth = ahead.depth();
        if (aheadDepth < skipDepth || aheadDepth == skipDepth && direction.ge(ahead.incomingTransition(), skipTransition))
            return advanceWithSetAhead(other.skipTo(skipDepth, skipTransition), other, ahead, state);
        // otherwise we must perform a full advance
        return skipBoth(skipDepth, skipTransition);
    }

    private int skipWithSetCovering(int skipDepth, int skipTransition, TrieSetCursor ahead, TrieSetCursor other)
    {
        // if the cursor ahead is at the skip point or beyond, we can advance the other cursor to the skip point
        int aheadDepth = ahead.depth();
        if (aheadDepth < skipDepth)
            return advanceWithSetCovering(other.skipTo(skipDepth, skipTransition), other, ahead, state);
        // otherwise we must perform a full advance
        return skipBoth(skipDepth, skipTransition);
    }

    /// Called to advance both cursors to an intersection. When called, the `ahead` cursor is known to be ahead of the
    /// `other` cursor, and the range preceding its position (which includes the current position of `other`) is not
    /// contained.
    private int advanceToIntersection(int aheadDepth, TrieSetCursor ahead, TrieSetCursor other, State state)
    {
        int aheadTransition = ahead.incomingTransition();
        while (true)
        {
            // At this point `ahead` is beyond `other`'s position, but the latter is outside the former's covered area.
            // Skip `other` to `ahead`'s position.
            int otherDepth = other.skipTo(aheadDepth, aheadTransition);
            int otherTransition = other.incomingTransition();
            if (otherDepth == aheadDepth && otherTransition == aheadTransition)
                return matchingPosition(aheadDepth, aheadTransition);
            if (precedingInSet(other))
                return coveredAreaWithSetAhead(aheadDepth, aheadTransition, ahead, state.swap());

            // If there's no match or coverage, the roles have reversed, swap everything and repeat.
            aheadDepth = otherDepth;
            aheadTransition = otherTransition;
            state = state.swap();
            TrieSetCursor t = ahead;
            ahead = other;
            other = t;
        }
    }

    private int coveredAreaWithSetAhead(int depth, int transition, TrieSetCursor advancing, State state)
    {
        this.currentDepth = depth;
        this.currentTransition = transition;
        this.currentRangeState = advancing.state();
        this.state = state;
        return depth;
    }

    private int matchingPosition(int depth, int transition)
    {

        currentDepth = depth;
        currentTransition = transition;
        final RangeState c1state = c1.state();
        final RangeState c2state = c2.state();
        currentRangeState = combineState(c1state, c2state);
        if ((c1state.branchIncluded() != null) != (c2state.branchIncluded() != null))
        {
            // If one of the sets has this as a "branch included" position, setting it as ahead makes sure that
            // we return the other's content until it ascends above the current position.
            // Note that because we don't allow intersections to result in boundaries that are prefixes of other
            // boundaries, when the covered set returns above this point it can't have `precedingIncluded()` true.
            if (c1state.branchIncluded() != null)
                state = State.C1_COVERED_BRANCH;
            else
                state = State.C2_COVERED_BRANCH;
        }
        else
            state = State.MATCHING;
        return depth;
    }

    TrieSetCursor.RangeState combineState(TrieSetCursor.RangeState cl, TrieSetCursor.RangeState cr)
    {
        return cl.intersect(cr);
    }

    @Override
    public TrieSetCursor tailCursor(Direction direction)
    {
        switch (state)
        {
            case MATCHING:
                return new TrieSetIntersectionCursor(c1.tailCursor(direction), c2.tailCursor(direction));
            case C1_AHEAD:
            case C1_COVERED_BRANCH:
                return c2.tailCursor(direction);
            case C2_AHEAD:
            case C2_COVERED_BRANCH:
                return c1.tailCursor(direction);
            default:
                throw new AssertionError();
        }
    }

    /// Implementation of the union of two sets. This is a direct application of DeMorgan's law, done by inverting the
    /// meaning of [#precedingInSet] and the combination of states.
    static class UnionCursor extends TrieSetIntersectionCursor
    {
        public UnionCursor(TrieSetCursor c1, TrieSetCursor c2)
        {
            super(c1, c2);
        }

        @Override
        boolean precedingInSet(TrieSetCursor cursor)
        {
            return !cursor.precedingIncluded();
        }

        @Override
        TrieSetCursor.RangeState combineState(TrieSetCursor.RangeState cl, TrieSetCursor.RangeState cr)
        {
            return cl.union(cr);
        }

        @Override
        public TrieSetCursor tailCursor(Direction direction)
        {
            switch (state)
            {
                case MATCHING:
                    return new UnionCursor(c1.tailCursor(direction), c2.tailCursor(direction));
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
