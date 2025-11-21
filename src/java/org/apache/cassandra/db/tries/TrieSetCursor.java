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

/// The implementation of a [TrieSet].
///
/// In addition to the functionality of normal trie cursors, set cursors also produce a [#state] that describes the
/// coverage of trie sections to the left and right of the cursor position. This is necessary to be able to identify
/// coverage after a [#skipTo] operation, where the set cursor jumps to a position beyond the requested one.
interface TrieSetCursor extends RangeCursor<TrieSetCursor.RangeState>
{
    /// This type describes the state at a given cursor position. It describes the coverage of the positions before and
    /// after the current in forward order, whether the node is boundary (and thus applies to this point and all its
    /// descendants) and also describes the type of boundary (e.g. start/end).
    enum RangeState implements org.apache.cassandra.db.tries.RangeState<RangeState>
    {
        // Note: the states must be ordered so that
        //   `values()[applicableBefore * APPLICABLE_BEFORE + applicableAfter * APPLICABLE_AFTER]`
        // produces a state with the requested flags

        /// The cursor is at a prefix of some start boundary, and the branches before it as well as the current point
        /// are not included in the set.
        NOT_CONTAINED(false, false),
        /// The cursor is positioned at an end boundary. Branches before this position in iteration order are covered
        /// by the set. The current position and any position in iteration order until the next boundary are excluded.
        END(true, false),
        /// The cursor is positioned at a start boundary. The current position as well as any position in iteration
        /// order up to the next boundary are covered by the set. Branches before this position are excluded.
        START(false, true),
        /// The cursor is positioned inside a covered range, on a prefix of an end position.
        CONTAINED(true, true);

        public static final int APPLICABLE_BEFORE = 1 << 0;
        public static final int APPLICABLE_AFTER  = 1 << 1;

        /// Whether the set applied to positions before the cursor's in iteration order.
        final boolean applicableBefore;
        /// Whether the set applied to positions after the cursor's in iteration order, starting with the children
        /// of the current node.
        final boolean applicableAfter;

        RangeState(boolean applicableBefore, boolean applicableAfter)
        {
            this.applicableBefore = applicableBefore;
            this.applicableAfter = applicableAfter;
        }

        /// Whether the positions preceding the current in iteration order are included in the set.
        public boolean precedingIncluded()
        {
            return applicableBefore;
        }

        /// Whether the current position is a range boundary. This also means that the descendant branch is fully
        /// included in the set.
        public boolean isBoundary()
        {
            return applicableBefore != applicableAfter;
        }

        public RangeState toContent()
        {
            return isBoundary() ? this : null;
        }

        /// Return an "intersection" state for the combination of two states, i.e. the ranges covered by both states.
        public RangeState intersect(RangeState other)
        {
            return values()[ordinal() & other.ordinal()];
        }

        /// Return a "union" state for the combination of two states, i.e. the ranges covered by at least one of the states.
        public RangeState union(RangeState other)
        {
            return values()[ordinal() | other.ordinal()];
        }

        /// Return the "weakly negated" state, i.e. the state that corresponds to flipped areas of coverage to the left
        /// and right, and the boundary points. See [TrieSet#weakNegation] for more details.
        public RangeState weakNegation()
        {
            return values()[ordinal() ^ (APPLICABLE_BEFORE | APPLICABLE_AFTER)];
        }

        public static RangeState fromProperties(boolean applicableBefore, boolean applicableAfter)
        {
            return values()[(applicableBefore ? APPLICABLE_BEFORE : 0) |
                            (applicableAfter ? APPLICABLE_AFTER : 0)];
        }

        // RangeState implementations (used for verification)

        @Override
        public RangeState precedingState(Direction direction)
        {
            return applicableBefore ? CONTAINED : null;
        }

        @Override
        public RangeState succedingState(Direction direction)
        {
            return applicableAfter ? CONTAINED : null;
        }

        @Override
        public RangeState restrict(boolean applicableBefore, boolean applicableAfter)
        {
            return fromProperties(this.applicableBefore && applicableBefore,
                                  this.applicableAfter && applicableAfter);
        }

        @Override
        public RangeState asBoundary(Direction direction)
        {
            final boolean isForward = direction.isForward();
            return fromProperties(this.applicableBefore && !isForward,
                                  this.applicableAfter && isForward);
        }


        public <S extends org.apache.cassandra.db.tries.RangeState<S>>
        S applyToCoveringState(S srcState, Direction direction)
        {
            switch (this)
            {
                case START:
                    return srcState.asBoundary(Direction.FORWARD);
                case END:
                    return srcState.asBoundary(Direction.REVERSE);
                case CONTAINED:
                    return srcState;
                case NOT_CONTAINED:
                    return null;
                default:
                    throw new AssertionError();
            }
        }
    }

    /// The range state of the trie cursor at this point.
    RangeState state();

    /// Returns whether the set includes the positions before the current in iteration order, but after any earlier
    /// position of this cursor, including any position requested by a [#skipTo] call, where this cursor advanced beyond
    /// that position.
    ///
    /// Note that this may also be true when the cursor is in an exhausted state, as well as immediately
    /// after cursor construction, signifying, respectively, right and left unbounded ranges.
    default boolean precedingIncluded()
    {
        return state().applicableBefore;
    }

    @Override
    default RangeState content()
    {
        return state().toContent();
    }

    @Override
    TrieSetCursor tailCursor(Direction direction);

    /// Returns a negated version of this cursor (where every returned state is inverted).
    default TrieSetCursor negated()
    {
        return new Negated(this);
    }

    /// Negation of trie set cursors.
    ///
    /// Achieved by simply inverting the [#state()] values.
    class Negated implements TrieSetCursor
    {
        final TrieSetCursor source;

        Negated(TrieSetCursor source)
        {
            this.source = source;
        }

        @Override
        public long encodedPosition()
        {
            return source.encodedPosition();
        }

        @Override
        public Direction direction()
        {
            return source.direction();
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return source.byteComparableVersion();
        }

        @Override
        public RangeState state()
        {
            return source.state().weakNegation();
        }

        @Override
        public long advance()
        {
            return source.advance();
        }

        @Override
        public long skipTo(long encodedSkipPosition)
        {
            return source.skipTo(encodedSkipPosition);
        }

        // Sets don't implement advanceMultiple as they are only meant to limit data tries.

        @Override
        public TrieSetCursor tailCursor(Direction direction)
        {
            return new Negated(source.tailCursor(direction));
        }
    }

    static TrieSetCursor empty(Direction direction, ByteComparable.Version version)
    {
        return new Empty(TrieSetCursor.RangeState.NOT_CONTAINED, version, direction);
    }

    class Empty extends Cursor.Empty<RangeState> implements TrieSetCursor
    {
        final RangeState coveringState;

        public Empty(RangeState coveringState, ByteComparable.Version version, Direction direction)
        {
            super(direction, version);
            this.coveringState = coveringState;
        }

        @Override
        public RangeState state()
        {
            return coveringState;
        }

        @Override
        public RangeState content()
        {
            return null;
        }

        @Override
        public TrieSetCursor tailCursor(Direction direction)
        {
            return new TrieSetCursor.Empty(coveringState, byteComparableVersion(), direction);
        }
    }
}
