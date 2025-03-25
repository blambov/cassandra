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
    /// The range state at the current position. This is combination of any reportable marker, as well as the state
    /// of the range for the positions preceding and succeeding this one.
    /// This can only be null when no range is active on any side of the current position. In particular, if the range
    /// is open, the state may be non-null when the cursor is exhausted or when it has been just created.
    M state();

    /// Returns a range that covers positions before this in iteration order, including this position if `content()` is
    /// null. This is the range that is active at (i.e. covers) a position that was skipped to, when the range trie
    /// jumps past the requested position or does not have content.
    /// Cannot be a reportable range (i.e. `precedingState().toContent()` must be null) and must be return itself
    /// for its `precedingState` in both directions.
    /// Note that this may also be non-null when the cursor is in an exhausted state, as well as immediately
    /// after cursor construction, signifying, respectively, right and left unbounded ranges.
    default M precedingState()
    {
        final M state = state();
        if (state == null)
            return null;
        return state.precedingState(direction());
    }

    /// Content is only returned for positions where the ranges change.
    /// Note that if `content()` is non-null, `precedingState()` does not apply to this exact position.
    @Override
    default M content()
    {
        final M state = state();
        if (state == null)
            return null;
        return state.toContent();
    }

    @Override
    RangeCursor<M> tailCursor(Direction direction);

    /// Corresponding method to tailCursor above applicable when this cursor is ahead.
    /// Returns a full-range cursor returning precedingState().
    default RangeCursor<M> precedingStateCursor(Direction direction)
    {
        return new Empty<>(precedingState(), byteComparableVersion(), direction);
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
        public M state()
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
}
