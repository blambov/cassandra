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

class RangeIntersectionCursor<M extends RangeMarker<M>> implements RangeCursor<M>
{
    enum State
    {
        MATCHING,
        SET_AHEAD,
        SOURCE_AHEAD;
    }

    final Direction direction;
    final RangeCursor<M> src;
    final TrieSetCursor set;
    int currentDepth;
    int currentTransition;
    M currentState;
    State state;

    public RangeIntersectionCursor(RangeCursor<M> src, TrieSetCursor set)
    {
        this.direction = src.direction();
        this.set = set;
        this.src = src;
        matchingPosition(set.depth(), set.incomingTransition());
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
        return set.byteComparableVersion();
    }

    @Override
    public M state()
    {
        return currentState;
    }

    @Override
    public int advance()
    {
        switch(state)
        {
            case MATCHING:
            {
                int ldepth = set.advance();
                if (set.precedingIncluded())
                    return advanceWithSetAhead(src.advance());
                else
                    return advanceSourceToIntersection(ldepth);
            }
            case SET_AHEAD:
                return advanceWithSetAhead(src.advance());
            case SOURCE_AHEAD:
                return advanceWithSourceAhead(set.advance());
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
                return skipBoth(skipDepth, skipTransition);
            case SET_AHEAD:
            {
                // if the cursor ahead is at the skip point or beyond, we can advance the other cursor to the skip point
                int leftDepth = set.depth();
                if (leftDepth < skipDepth || leftDepth == skipDepth && direction.ge(set.incomingTransition(), skipTransition))
                    return advanceWithSetAhead(src.skipTo(skipDepth, skipTransition));
                // otherwise we must perform a full advance
                return skipBoth(skipDepth, skipTransition);
            }
            case SOURCE_AHEAD:
            {
                // if the cursor ahead is at the skip point or beyond, we can advance the other cursor to the skip point
                int rightDepth = src.depth();
                if (rightDepth < skipDepth || rightDepth == skipDepth && direction.ge(src.incomingTransition(), skipTransition))
                    return advanceWithSourceAhead(set.skipTo(skipDepth, skipTransition));
                // otherwise we must perform a full advance
                return skipBoth(skipDepth, skipTransition);
            }
            default:
                throw new AssertionError();
        }
    }

    private int skipBoth(int skipDepth, int skipTransition)
    {
        int ldepth = set.skipTo(skipDepth, skipTransition);
        if (set.precedingIncluded())
            return advanceWithSetAhead(src.skipTo(skipDepth, skipTransition));
        else
            return advanceSourceToIntersection(ldepth);
    }

    @Override
    public int advanceMultiple(Cursor.TransitionsReceiver receiver)
    {
        switch(state)
        {
            case MATCHING:
            {
                // Cannot do multi-advance when cursors are at the same position. Applying advance().
                int ldepth = set.advance();
                if (set.precedingIncluded())
                    return advanceWithSetAhead(src.advance());
                else
                    return advanceSourceToIntersection(ldepth);
            }
            case SET_AHEAD:
                return advanceWithSetAhead(src.advanceMultiple(receiver));
            case SOURCE_AHEAD:
                return advanceWithSourceAhead(set.advanceMultiple(receiver));
            default:
                throw new AssertionError();
        }
    }

    private int advanceWithSetAhead(int rightDepth)
    {
        int rightTransition = src.incomingTransition();
        int leftDepth = set.depth();
        int leftTransition = set.incomingTransition();
        if (rightDepth > leftDepth)
            return coveredAreaWithSetAhead(rightDepth, rightTransition);
        if (rightDepth == leftDepth)
        {
            if (direction.lt(rightTransition, leftTransition))
                return coveredAreaWithSetAhead(rightDepth, rightTransition);
            if (rightTransition == leftTransition)
                return matchingPosition(rightDepth, rightTransition);
        }

        // Advancing cursor moved beyond the ahead cursor. Check if roles have reversed.
        if (src.precedingState() != null)
            return coveredAreaWithSourceAhead(leftDepth, leftTransition);
        else
            return advanceSetToIntersection(rightDepth);
    }

    private int advanceWithSourceAhead(int leftDepth)
    {
        int leftTransition = set.incomingTransition();
        int rightDepth = src.depth();
        int rightTransition = src.incomingTransition();
        if (leftDepth > rightDepth)
            return coveredAreaWithSourceAhead(leftDepth, leftTransition);
        if (leftDepth == rightDepth)
        {
            if (direction.lt(leftTransition, rightTransition))
                return coveredAreaWithSourceAhead(leftDepth, leftTransition);
            if (leftTransition == rightTransition)
                return matchingPosition(leftDepth, leftTransition);
        }

        // Advancing cursor moved beyond the ahead cursor. Check if roles have reversed.
        if (set.precedingIncluded())
            return coveredAreaWithSetAhead(rightDepth, rightTransition);
        else
            return advanceSourceToIntersection(leftDepth);
    }

    private int advanceSourceToIntersection(int leftDepth)
    {
        int leftTransition = set.incomingTransition();
        while (true)
        {
            // Set is ahead of right, but outside the covered area. Skip right to left's position.
            int rightDepth = src.skipTo(leftDepth, leftTransition);
            int rightTransition = src.incomingTransition();
            if (rightDepth == leftDepth && rightTransition == leftTransition)
                return matchingPosition(leftDepth, leftTransition);
            if (src.precedingState() != null)
                return coveredAreaWithSourceAhead(leftDepth, leftTransition);

            // Source is ahead of left, but outside the covered area. Skip left to right's position.
            leftDepth = set.skipTo(rightDepth, rightTransition);
            leftTransition = set.incomingTransition();
            if (leftDepth == rightDepth && leftTransition == rightTransition)
                return matchingPosition(rightDepth, rightTransition);
            if (set.precedingIncluded())
                return coveredAreaWithSetAhead(rightDepth, rightTransition);
        }
    }

    private int advanceSetToIntersection(int rightDepth)
    {
        int rightTransition = src.incomingTransition();
        while (true)
        {
            // Source is ahead of left, but outside the covered area. Skip left to right's position.
            int leftDepth = set.skipTo(rightDepth, rightTransition);
            int leftTransition = set.incomingTransition();
            if (leftDepth == rightDepth && leftTransition == rightTransition)
                return matchingPosition(rightDepth, rightTransition);
            if (set.precedingIncluded())
                return coveredAreaWithSetAhead(rightDepth, rightTransition);

            // Set is ahead of right, but outside the covered area. Skip right to left's position.
            rightDepth = src.skipTo(leftDepth, leftTransition);
            rightTransition = src.incomingTransition();
            if (rightDepth == leftDepth && rightTransition == leftTransition)
                return matchingPosition(leftDepth, leftTransition);
            if (src.precedingState() != null)
                return coveredAreaWithSourceAhead(leftDepth, leftTransition);
        }
    }

    private int coveredAreaWithSetAhead(int depth, int transition)
    {
        return setState(State.SET_AHEAD, depth, transition, src.state());
    }

    private int coveredAreaWithSourceAhead(int depth, int transition)
    {
        return setState(State.SOURCE_AHEAD, depth, transition, restrict(src.precedingState(), set.state()));
    }

    private int matchingPosition(int depth, int transition)
    {
        return setState(State.MATCHING, depth, transition, restrict(src.state(), set.state()));
    }

    private M restrict(M srcState, TrieSetCursor.RangeState setState)
    {
        if (srcState == null)
            return null;
        return srcState.restrict(setState.applicableBefore, setState.applicableAfter);
    }

    private int setState(State state, int depth, int transition, M cursorState)
    {
        this.state = state;
        this.currentDepth = depth;
        this.currentTransition = transition;
        this.currentState = cursorState;
        return depth;
    }

    @Override
    public RangeCursor<M> tailCursor(Direction direction)
    {
        switch (state)
        {
            case MATCHING:
                return new RangeIntersectionCursor<>(src.tailCursor(direction), set.tailCursor(direction));
            case SET_AHEAD:
                return src.tailCursor(direction);
            case SOURCE_AHEAD:
                return new RangeIntersectionCursor<>(src.precedingStateCursor(direction), set.tailCursor(direction));
            default:
                throw new AssertionError();
        }
    }
}
