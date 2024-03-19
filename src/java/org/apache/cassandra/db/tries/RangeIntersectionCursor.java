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

class RangeIntersectionCursor<C extends RangeTrieImpl.RangeMarker<C>, D extends RangeTrieImpl.RangeMarker<D>, Z extends RangeTrieImpl.RangeMarker<Z>> implements RangeTrieImpl.Cursor<Z>
{
    interface IntersectionController<C extends RangeTrieImpl.RangeMarker<C>, D extends RangeTrieImpl.RangeMarker<D>, Z extends RangeTrieImpl.RangeMarker<Z>>
    {
        Z combineState(C lState, D rState);

        default boolean includeLesserLeft(RangeTrieImpl.Cursor<C> cursor)
        {
            C lState = cursor.coveringState();
            return lState != null ? lState.lesserIncluded() : false;
        }

        default boolean includeLesserRight(RangeTrieImpl.Cursor<D> cursor)
        {
            D rState = cursor.coveringState();
            return rState != null ? rState.lesserIncluded() : false;
        }

        default Z combineCoveringState(RangeTrieImpl.Cursor<C> lCursor, RangeTrieImpl.Cursor<D> rCursor)
        {
            return combineState(lCursor.coveringState(), rCursor.coveringState());
        }

        default Z combineContent(RangeTrieImpl.Cursor<C> lCursor, RangeTrieImpl.Cursor<D> rCursor)
        {
            C lContent = lCursor.content();
            D rContent = rCursor.content();
            if (lContent == null && rContent == null)
                return null;
            if (lContent != null && rContent != null)
                return combineState(lContent, rContent);

            if (lContent == null)
                lContent = lCursor.coveringState();
            else if (rContent == null)
                rContent = rCursor.coveringState();

            return combineState(lContent, rContent);
        }

        default Z combineContentLeftAhead(RangeTrieImpl.Cursor<C> lCursor, RangeTrieImpl.Cursor<D> rCursor)
        {
            D rContent = rCursor.content();
            if (rContent == null)
                return null;
            C lContent = lCursor.coveringState();

            return combineState(lContent, rContent);
        }


        default Z combineContentRightAhead(RangeTrieImpl.Cursor<C> lCursor, RangeTrieImpl.Cursor<D> rCursor)
        {
            C lContent = lCursor.content();
            if (lContent == null)
                return null;
            D rContent = rCursor.coveringState();

            return combineState(lContent, rContent);
        }
    }

    enum State
    {
        MATCHING,
        C1_AHEAD,
        C2_AHEAD;
    }

    final IntersectionController<C, D, Z> controller;
    final RangeTrieImpl.Cursor<C> c1;
    final RangeTrieImpl.Cursor<D> c2;
    int currentDepth;
    int currentTransition;
    Z currentCoveringState;
    boolean currentCoversingStateSet;
    Z currentContent;
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
        this.currentCoveringState = copyFrom.currentCoveringState;
        this.currentCoversingStateSet = copyFrom.currentCoversingStateSet;
        this.currentContent = copyFrom.currentContent;
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
    public Z coveringState()
    {
        if (!currentCoversingStateSet)
        {
            currentCoveringState = controller.combineCoveringState(c1, c2);
            currentCoversingStateSet = true;
        }
        return currentCoveringState;
    }

    @Override
    public Z content()
    {
        return currentContent;
    }

    @Override
    public int advance()
    {
        switch(state)
        {
            case MATCHING:
            {
                int ldepth = c1.advance();
                if (controller.includeLesserLeft(c1))
                    return advanceWithLeftAhead(c2.advance());
                else
                    return advanceRightToIntersection(ldepth);
            }
            case C1_AHEAD:
                return advanceWithLeftAhead(c2.advance());
            case C2_AHEAD:
                return advanceWithRightAhead(c1.advance());
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
            case C1_AHEAD:
            {
                // if the cursor ahead is at the skip point or beyond, we can advance the other cursor to the skip point
                int leftDepth = c1.depth();
                if (leftDepth < skipDepth || leftDepth == skipDepth && c1.incomingTransition() >= skipTransition)
                    return advanceWithLeftAhead(c2.skipTo(skipDepth, skipTransition));
                // otherwise we must perform a full advance
                return skipBoth(skipDepth, skipTransition);
            }
            case C2_AHEAD:
            {
                // if the cursor ahead is at the skip point or beyond, we can advance the other cursor to the skip point
                int rightDepth = c2.depth();
                if (rightDepth < skipDepth || rightDepth == skipDepth && c2.incomingTransition() >= skipTransition)
                    return advanceWithRightAhead(c1.skipTo(skipDepth, skipTransition));
                // otherwise we must perform a full advance
                return skipBoth(skipDepth, skipTransition);
            }
            default:
                throw new AssertionError();
        }
    }

    private int skipBoth(int skipDepth, int skipTransition)
    {
        int ldepth = c1.skipTo(skipDepth, skipTransition);
        if (controller.includeLesserLeft(c1))
            return advanceWithLeftAhead(c2.skipTo(skipDepth, skipTransition));
        else
            return advanceRightToIntersection(ldepth);
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
                if (controller.includeLesserLeft(c1))
                    return advanceWithLeftAhead(c2.advance());
                else
                    return advanceRightToIntersection(ldepth);
            }
            case C1_AHEAD:
                return advanceWithLeftAhead(c2.advanceMultiple(receiver));
            case C2_AHEAD:
                return advanceWithRightAhead(c1.advanceMultiple(receiver));
            default:
                throw new AssertionError();
        }
    }

    private int advanceWithLeftAhead(int rightDepth)
    {
        int rightTransition = c2.incomingTransition();
        int leftDepth = c1.depth();
        int leftTransition = c1.incomingTransition();
        if (rightDepth > leftDepth)
            return coveredAreaWithLeftAhead(rightDepth, rightTransition);
        if (rightDepth == leftDepth)
        {
            if (rightTransition < leftTransition)
                return coveredAreaWithLeftAhead(rightDepth, rightTransition);
            if (rightTransition == leftTransition)
                return matchingPosition(rightDepth, rightTransition);
        }

        // Advancing cursor moved beyond the ahead cursor. Check if roles have reversed.
        if (controller.includeLesserRight(c2))
            return coveredAreaWithRightAhead(leftDepth, leftTransition);
        else
            return advanceLeftToIntersection(rightDepth);
    }

    private int advanceWithRightAhead(int leftDepth)
    {
        int leftTransition = c1.incomingTransition();
        int rightDepth = c2.depth();
        int rightTransition = c2.incomingTransition();
        if (leftDepth > rightDepth)
            return coveredAreaWithRightAhead(leftDepth, leftTransition);
        if (leftDepth == rightDepth)
        {
            if (leftTransition < rightTransition)
                return coveredAreaWithRightAhead(leftDepth, leftTransition);
            if (leftTransition == rightTransition)
                return matchingPosition(leftDepth, leftTransition);
        }

        // Advancing cursor moved beyond the ahead cursor. Check if roles have reversed.
        if (controller.includeLesserLeft(c1))
            return coveredAreaWithLeftAhead(rightDepth, rightTransition);
        else
            return advanceRightToIntersection(leftDepth);
    }

    private int advanceRightToIntersection(int leftDepth)
    {
        int leftTransition = c1.incomingTransition();
        while (true)
        {
            // Left is ahead of right, but outside the covered area. Skip right to left's position.
            int rightDepth = c2.skipTo(leftDepth, leftTransition);
            int rightTransition = c2.incomingTransition();
            if (rightDepth == leftDepth && rightTransition == leftTransition)
                return matchingPosition(leftDepth, leftTransition);
            if (controller.includeLesserRight(c2))
                return coveredAreaWithRightAhead(leftDepth, leftTransition);

            // Right is ahead of left, but outside the covered area. Skip left to right's position.
            leftDepth = c1.skipTo(rightDepth, rightTransition);
            leftTransition = c1.incomingTransition();
            if (leftDepth == rightDepth && leftTransition == rightTransition)
                return matchingPosition(rightDepth, rightTransition);
            if (controller.includeLesserLeft(c1))
                return coveredAreaWithLeftAhead(rightDepth, rightTransition);
        }
    }

    private int advanceLeftToIntersection(int rightDepth)
    {
        int rightTransition = c2.incomingTransition();
        while (true)
        {
            // Right is ahead of left, but outside the covered area. Skip left to right's position.
            int leftDepth = c1.skipTo(rightDepth, rightTransition);
            int leftTransition = c1.incomingTransition();
            if (leftDepth == rightDepth && leftTransition == rightTransition)
                return matchingPosition(rightDepth, rightTransition);
            if (controller.includeLesserLeft(c1))
                return coveredAreaWithLeftAhead(rightDepth, rightTransition);

            // Left is ahead of right, but outside the covered area. Skip right to left's position.
            rightDepth = c2.skipTo(leftDepth, leftTransition);
            rightTransition = c2.incomingTransition();
            if (rightDepth == leftDepth && rightTransition == leftTransition)
                return matchingPosition(leftDepth, leftTransition);
            if (controller.includeLesserRight(c2))
                return coveredAreaWithRightAhead(leftDepth, leftTransition);
        }
    }

    private int coveredAreaWithLeftAhead(int depth, int transition)
    {
        return setState(State.C1_AHEAD, depth, transition, controller.combineContentLeftAhead(c1, c2));
    }

    private int coveredAreaWithRightAhead(int depth, int transition)
    {
        return setState(State.C2_AHEAD, depth, transition, controller.combineContentRightAhead(c1, c2));
    }

    private int matchingPosition(int depth, int transition)
    {
        return setState(State.MATCHING, depth, transition, controller.combineContent(c1, c2));
    }

    private int setState(State state, int depth, int transition, Z content)
    {
        this.state = state;
        this.currentDepth = depth;
        this.currentTransition = transition;
        this.currentContent = content;
        this.currentCoversingStateSet = false;
        this.currentCoveringState = null;
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
        public TrieSetImpl.RangeState state()
        {
            TrieSetImpl.RangeState content = content();
            if (content != null)
                return content;
            return coveringState();
        }

        @Override
        public TrieSet duplicate()
        {
            return new TrieSet(this);
        }
    }
}
