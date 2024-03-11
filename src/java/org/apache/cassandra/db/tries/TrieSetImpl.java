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
    // TODO: Make it non-null
    enum RangeState implements RangeTrieImpl.RangeMarker<RangeState>
    {
        OUTSIDE_PREFIX(false, false, false),
        INSIDE_PREFIX(true, true, false),
        START(false, true, true),
        END(true, false, true);

        final boolean applicableBefore;
        final boolean applicableAt;
        final boolean reportAsContent;

        RangeState(boolean applicableBefore, boolean applicableAt, boolean reportAsContent)
        {
            this.applicableBefore = applicableBefore;
            this.applicableAt = applicableAt;
            this.reportAsContent = reportAsContent;
        }


        @Override
        public RangeState applicableBefore()
        {
            return applicableBefore ? this : null;
        }

        @Override
        public RangeState applicableAt()
        {
            return applicableAt ? this : null;
        }

        @Override
        public RangeState toContent()
        {
            return reportAsContent ? this : null;
        }
    }

    interface Cursor extends RangeTrieImpl.Cursor<RangeState>
    {
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
                return RangeState.OUTSIDE_PREFIX;
        }

        @Override
        public boolean includeLesserLeft(RangeState lState)
        {
            return lState.applicableBefore() != null;
        }

        @Override
        public boolean includeLesserRight(RangeState rState)
        {
            return rState.applicableBefore() != null;
        }

        @Override
        public RangeState combineStateCoveringLeft(RangeState rState, RangeState lCoveringState)
        {
            return rState;
        }

        @Override
        public RangeState combineStateCoveringRight(RangeState lState, RangeState rCoveringState)
        {
            return lState;
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
                return RangeState.INSIDE_PREFIX;
        }

        @Override
        public boolean includeLesserLeft(RangeState lState)
        {
            return lState.applicableBefore() == null;
        }

        @Override
        public boolean includeLesserRight(RangeState rState)
        {
            return rState.applicableBefore() == null;
        }

        @Override
        public RangeState combineStateCoveringLeft(RangeState rState, RangeState lCoveringState)
        {
            return rState;
        }

        @Override
        public RangeState combineStateCoveringRight(RangeState lState, RangeState rCoveringState)
        {
            return lState;
        }
    };
}
