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

interface RangeCursor<M extends RangeMarker<M>> extends Cursor<M>
{
    /// Returns a range that covers positions before this, including this position if `content()` is null.
    /// This is the range that is active at (i.e. covers) a position that was skipped to, when the range trie jumps
    /// past the requested position or does not have content.
    /// Cannot be a reportable range (i.e. `coveringState().toContent()` must be null) and must be a state that is the
    /// for preceding and succeeding positions (see e.g. [#asCoveringState(Direction)]).
    /// Note that this may also be non-null when the cursor is in an exhausted state, as well as immediately
    /// after cursor construction, signifying, respectively, right and left unbounded ranges.
    M coveringState();

    /// Content is only returned for positions where the ranges change.
    /// Note that if `content()` is non-null, `coveringState()` does not apply to this exact position.
    @Override
    M content();

    @Override
    RangeCursor<M> tailCursor(Direction direction);

    /**
     * Corresponding method to tailCursor above applicable when this cursor is ahead.
     * Returns a full-range cursor returning coveringState().
     */
    default RangeCursor<M> coveringStateCursor(Direction direction)
    {
        return new Empty<>(coveringState(), byteComparableVersion(), direction);
    }

    class Empty<M extends RangeMarker<M>> extends Cursor.Empty<M> implements RangeCursor<M>
    {
        final M coveringState;

        public Empty(M coveringState, ByteComparable.Version version, Direction direction)
        {
            super(direction, version);
            this.coveringState = coveringState;
        }

        @Override
        public M coveringState()
        {
            return coveringState;
        }

        @Override
        public M content()
        {
            return null;
        }

        @Override
        public RangeCursor<M> tailCursor(Direction direction)
        {
            return new RangeCursor.Empty<>(coveringState, byteComparableVersion(), direction);
        }
    }

    static <M extends RangeMarker<M>> RangeCursor<M> empty(Direction direction, ByteComparable.Version version)
    {
        return new Empty<M>(null, version, direction);
    }

    static <M extends RangeMarker<M>> RangeIntersectionCursor.IntersectionController<TrieSetCursor.RangeState, M, M> rangeAndSetIntersectionController()
    {
        return new RangeIntersectionCursor.IntersectionController<>()
        {
            @Override
            public M combineState(TrieSetCursor.RangeState lState, M rState)
            {
                if (rState == null)
                    return null;

                return rState.asReportablePoint(lState.applicableBefore, lState.applicableAfter);
            }

            @Override
            public boolean includeLesserLeft(RangeCursor<TrieSetCursor.RangeState> cursor)
            {
                return cursor.coveringState().applicableBefore;
            }

            @Override
            public M combineContentLeftAhead(RangeCursor<TrieSetCursor.RangeState> lCursor, RangeCursor<M> rCursor)
            {
                if (lCursor.coveringState().applicableBefore)
                    return rCursor.content();
                else
                    return null;
            }
        };
    }
}
