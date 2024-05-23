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
    enum RangeState implements RangeTrie.RangeMarker<RangeState>
    {
        // TODO: Define masks and comment
        START_END_PREFIX(false, false, false),
        END_PREFIX(true, false, false),
        START_PREFIX(false, true, false),
        END_START_PREFIX(true, true, false),
        POINT(false, false, true),
        END(true, false, true),
        START(false, true, true),
        COVERED(true, true, true); // end and start, does not change anything

        final boolean applicableBefore;
        final boolean applicableAfter;
        final RangeState asContent;

        RangeState(boolean applicableBefore, boolean applicableAfter, boolean reportAsContent)
        {
            this.applicableBefore = applicableBefore;
            this.applicableAfter = applicableAfter;
            this.asContent = reportAsContent ? this : null;
        }


        @Override
        public boolean precedingIncluded(Direction direction)
        {
            return direction.select(applicableBefore, applicableAfter);
        }

        @Override
        public RangeState toContent()
        {
            return asContent;
        }

        @Override
        public RangeState asCoveringState(Direction direction)
        {
            return direction.select(applicableBefore, applicableAfter) ? END_START_PREFIX : START_END_PREFIX;
        }

        @Override
        public RangeState asReportablePoint(boolean includeBefore, boolean includeAfter)
        {
            int ord = (includeBefore ? 1 : 0) | (includeAfter ? 2 : 0) | 4;
            ord &= ordinal();
            return (ord & 4) != 0 ? values()[ord] : null;
        }

        public RangeState intersect(RangeState other)
        {
            return values()[ordinal() & other.ordinal()];
        }

        public RangeState union(RangeState other)
        {
            return values()[ordinal() | other.ordinal()];
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
            return state().asCoveringState(direction());
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
    default <R> R process(TrieImpl.Walker<RangeState, R> walker, Direction direction)
    {
        return TrieImpl.process(walker, cursor(direction));
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
            return lState.intersect(rState);
        }

        @Override
        public boolean includeLesserLeft(RangeTrieImpl.Cursor<RangeState> cursor)
        {
            return cursor.coveringState().applicableBefore;
        }

        @Override
        public boolean includeLesserRight(RangeTrieImpl.Cursor<RangeState> cursor)
        {
            return cursor.coveringState().applicableBefore;
        }

        @Override
        public RangeState combineContentLeftAhead(RangeTrieImpl.Cursor<RangeState> lCursor, RangeTrieImpl.Cursor<RangeState> rCursor)
        {
            if (lCursor.coveringState().applicableBefore)
                return rCursor.content();
            else
                return null;
        }

        @Override
        public RangeState combineContentRightAhead(RangeTrieImpl.Cursor<RangeState> lCursor, RangeTrieImpl.Cursor<RangeState> rCursor)
        {
            if (rCursor.coveringState().applicableBefore)
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
            return lState.union(rState);
        }

        @Override
        public boolean includeLesserLeft(RangeTrieImpl.Cursor<RangeState> cursor)
        {
            return !cursor.coveringState().applicableBefore;
        }

        @Override
        public boolean includeLesserRight(RangeTrieImpl.Cursor<RangeState> cursor)
        {
            return !cursor.coveringState().applicableBefore;
        }

        @Override
        public RangeState combineContentLeftAhead(RangeTrieImpl.Cursor<RangeState> lCursor, RangeTrieImpl.Cursor<RangeState> rCursor)
        {
            if (!lCursor.coveringState().applicableBefore)
                return rCursor.content();
            else
                return null;
        }

        @Override
        public RangeState combineContentRightAhead(RangeTrieImpl.Cursor<RangeState> lCursor, RangeTrieImpl.Cursor<RangeState> rCursor)
        {
            if (!rCursor.coveringState().applicableBefore)
                return lCursor.content();
            else
                return null;
        }
    };
}
