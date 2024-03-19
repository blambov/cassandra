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

public interface TrieSetImpl extends CursorWalkable<TrieSetImpl.Cursor>
{
    enum RangeState implements RangeTrieImpl.RangeMarker<RangeState>
    {
        OUTSIDE_PREFIX(false, false, false),
        INSIDE_PREFIX(true, true, false),
        START(false, true, true),
        END(true, false, true);

        final boolean applicableBefore;
        final boolean applicableAt;
        final RangeState asContent;

        RangeState(boolean applicableBefore, boolean applicableAt, boolean reportAsContent)
        {
            this.applicableBefore = applicableBefore;
            this.applicableAt = applicableAt;
            this.asContent = reportAsContent ? this : null;
        }


        @Override
        public boolean lesserIncluded()
        {
            return applicableBefore;
        }

        public boolean matchingIncluded()
        {
            return applicableAt;
        }

        @Override
        public RangeState toContent()
        {
            return asContent;
        }

        @Override
        public RangeState leftSideAsCovering()
        {
            switch (this)
            {
                case END:
                    return INSIDE_PREFIX;
                case START:
                    return OUTSIDE_PREFIX;
                default:
                    return this;
            }
        }

        @Override
        public RangeState rightSideAsCovering()
        {
            switch (this)
            {
                case END:
                    return OUTSIDE_PREFIX;
                case START:
                    return INSIDE_PREFIX;
                default:
                    return this;
            }
        }

        @Override
        public RangeState asReportableStart()
        {
            switch (this)
            {
                case END:
                case OUTSIDE_PREFIX:
                    return OUTSIDE_PREFIX;
                case START:
                case INSIDE_PREFIX:
                    return START;
                default:
                    return this;
            }
        }

        @Override
        public RangeState asReportableEnd()
        {
            switch (this)
            {
                case START:
                case OUTSIDE_PREFIX:
                    return OUTSIDE_PREFIX;
                case END:
                case INSIDE_PREFIX:
                    return END;
                default:
                    return this;
            }
        }
    }

    interface Cursor extends RangeTrieImpl.Cursor<RangeState>
    {
        /**
         * Combined state. The following hold:
         *   state() == content() != null ? content() : coveringState()
         *   content() == state().toContent()
         *   coveringState() == state().leftSideAsCovering()
         */
        RangeState state();

        default RangeState coveringState()
        {
            return state().leftSideAsCovering();
        }

        default RangeState content()
        {
            return state().toContent();
        }

        @Override
        Cursor duplicate();
    }

    /**
     * Process the trie using the given Walker.
     */
    default <R> R process(TrieImpl.Walker<RangeState, R> walker)
    {
        return TrieImpl.process(walker, cursor());
    }


    static TrieSetImpl impl(TrieSet trieSet)
    {
        return (TrieSetImpl) trieSet;
    }

    static RangeIntersectionCursor.IntersectionController<RangeState, RangeState, RangeState> INTERSECTION_CONTROLLER =
    new RangeIntersectionCursor.IntersectionController<RangeState, RangeState, RangeState>()
    {
        @Override
        public RangeState combineState(RangeState lState, RangeState rState)
        {
            if (lState == RangeState.OUTSIDE_PREFIX || rState == RangeState.OUTSIDE_PREFIX)
                return RangeState.OUTSIDE_PREFIX;
            else if (lState == RangeState.INSIDE_PREFIX)
                return rState;
            else if (rState == RangeState.INSIDE_PREFIX)
                return lState;
            else if (lState == rState)
                return lState;
            else // start and end combination
                return null;
        }

        @Override
        public boolean includeLesserLeft(RangeTrieImpl.Cursor<RangeState> cursor)
        {
            return cursor.coveringState().lesserIncluded();
        }

        @Override
        public boolean includeLesserRight(RangeTrieImpl.Cursor<RangeState> cursor)
        {
            return cursor.coveringState().lesserIncluded();
        }

        @Override
        public RangeState combineContentLeftAhead(RangeTrieImpl.Cursor<RangeState> lCursor, RangeTrieImpl.Cursor<RangeState> rCursor)
        {
            if (lCursor.coveringState().lesserIncluded())
                return rCursor.content();
            else
                return null;
        }

        @Override
        public RangeState combineContentRightAhead(RangeTrieImpl.Cursor<RangeState> lCursor, RangeTrieImpl.Cursor<RangeState> rCursor)
        {
            if (rCursor.coveringState().lesserIncluded())
                return lCursor.content();
            else
                return null;
        }
    };

    static RangeIntersectionCursor.IntersectionController<RangeState, RangeState, RangeState> UNION_CONTROLLER =
    new RangeIntersectionCursor.IntersectionController<RangeState, RangeState, RangeState>()
    {
        @Override
        public RangeState combineState(RangeState lState, RangeState rState)
        {
            if (lState == RangeState.INSIDE_PREFIX || rState == RangeState.INSIDE_PREFIX)
                return RangeState.INSIDE_PREFIX;
            else if (lState == RangeState.OUTSIDE_PREFIX)
                return rState;
            else if (rState == RangeState.OUTSIDE_PREFIX)
                return lState;
            else if (lState == rState)
                return lState;
            else // start and end combination
                return null;
        }

        @Override
        public boolean includeLesserLeft(RangeTrieImpl.Cursor<RangeState> cursor)
        {
            return !cursor.coveringState().lesserIncluded();
        }

        @Override
        public boolean includeLesserRight(RangeTrieImpl.Cursor<RangeState> cursor)
        {
            return !cursor.coveringState().lesserIncluded();
        }

        @Override
        public RangeState combineContentLeftAhead(RangeTrieImpl.Cursor<RangeState> lCursor, RangeTrieImpl.Cursor<RangeState> rCursor)
        {
            if (!lCursor.coveringState().lesserIncluded())
                return rCursor.content();
            else
                return null;
        }

        @Override
        public RangeState combineContentRightAhead(RangeTrieImpl.Cursor<RangeState> lCursor, RangeTrieImpl.Cursor<RangeState> rCursor)
        {
            if (!rCursor.coveringState().lesserIncluded())
                return lCursor.content();
            else
                return null;
        }
    };
}
