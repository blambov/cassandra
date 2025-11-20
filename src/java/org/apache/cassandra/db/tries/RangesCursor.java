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
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/// A cursor for a [TrieSet] that represents a set of ranges.
///
/// Ranges in this trie design always include all prefixes and all descendants of the start and end points. That is,
/// the range of ("abc", "afg") includes "a", "ab", "abc", "af, "afg", as well as any string staring with "abc", "ac",
/// "ad", "ae", "afg". The range of ("abc", "abc") includes "a", "ab", "abc" and any string starting with "abc".
///
/// Ranges that contain prefixes (e.g. ("ab", "abc")) are invalid as they cannot be specified without violating order in
/// one of the directions (i.e. "ab" is before "abc" in forward order, but not after it in reverse).
/// The reason for these restrictions is to ensure that all individual ranges are contiguous spans when iterated in
/// forward as well as backward order. This in turn makes it possible to have simple range trie and intersection
/// implementations where the representations of range trie sets do not differ when iterated in the two directions.
///
/// These types of ranges are actually preferable to us, because we use prefix-free keys with terminators that leave
/// room for greater- and less-than positions, and at prefix nodes we store metadata applicable to the whole branch.
///
/// The ranges are specified by passing a sequence of boundaries, where each even boundary is the opening boundary, and
/// the odd ones are the closing boundary for a range. The boundaries must be in order and cannot overlap, but it is
/// possible to repeat a boundary, e.g.
/// - `[a, a] == point a`: an even number of repeats starting at an even position specifies a point
///    (i.e. the set of all prefixes and all descendants of that key)
/// - `[a, b, b, c] == [a, c]`: an even number of repeats which starts at an odd position is ignored
/// - `[a, a, a, b] == [a, b]`: an odd number of repeats is the same as a single copy
///
/// If the first boundary is null, the range is open on the left, and if the last non-null boundary is on an even
/// position (i.e. if the array has an even length and its last boundary is null, or if that last boundary is cut off),
/// the range is open on the right:
/// - `[null, a]` covers all keys smaller than `a`, plus the prefixes and descendants of `a`
/// - `[a, null]` or `[a]` covers all keys greater than `a`, plus the prefixes and descendants of `a`
class RangesCursor implements TrieSetCursor
{
    private final ByteComparable.Version byteComparableVersion;

    /// The current index in the boundaries array. This is the input that will be advanced on the next [#advance] call.
    /// When the current boundary is exhausted, it will advance. When it reaches [#completedIdx], the cursor is
    /// exhausted.
    private int currentIdx;
    /// The index at which the ranges end. The end or start of the array, adjusted to remove null boundaries.
    private final int endIdx;
    /// The next position for all boundaries.
    long[] nextPositions;

    /// Byte sources producing the rest of the bytes of the boundaries.
    ByteSource.Peekable[] sources;
    /// The current position (reported to the user). This is usually formed from `depths[currentIdx] - 1` and `nexts[currentIdx]`.
    long currentPosition;
    /// Current range state, returned by [#state].
    RangeState currentState;

    final boolean endsInclusive;

    public static RangesCursor create(Direction direction, ByteComparable.Version byteComparableVersion, boolean endsInclusive, ByteComparable... boundaries)
    {
        long rootPosition = Cursor.rootPosition(direction);

        // handle empty array (== full range) and nulls at the ends (same as not there, odd length == open end range)
        int length = boundaries.length;

        if (length > 1 && boundaries[length - 1] == null)
            --length;

        int first = 0;
        if (length > 0 && boundaries[0] == null)
            first = 1;

        if (first >= length) // no boundaries on either side, report END_START_PREFIX on root and exhausted state
            return new RangesCursor(byteComparableVersion,
                                    endsInclusive,
                                    null, null,
                                    1, 1,
                                    rootPosition,
                                    RangeState.END_START_PREFIX);

        int arrayLength = (length + 1) & ~1;
        long[] nextPositions = new long[arrayLength];

        ByteSource.Peekable[] sources = new ByteSource.Peekable[arrayLength];
        for (int i = first; i < length; ++i)
        {
            if (boundaries[i] == null)
                throw new AssertionError("Null can only be used as the first or last boundary.");

            int destIndex = direction.select(i, arrayLength - i - 1);
            sources[destIndex] = boundaries[i].asPeekableBytes(byteComparableVersion);
            nextPositions[destIndex] = rootPosition;
        }

        int startIdx;
        int endIdx;
        if (direction.isForward())
        {
            startIdx = first;
            endIdx = length;
        }
        else
        {
            startIdx = arrayLength - length;
            endIdx = arrayLength - first;
        }

        RangesCursor cursor = new RangesCursor(byteComparableVersion,
                                               endsInclusive,
                                               nextPositions, sources,
                                               startIdx, endIdx,
                                               rootPosition,
                                               RangeState.START_END_PREFIX);
        cursor.advanceBoundariesAndSelectState(endIdx);
        return cursor;
    }

    private RangesCursor(ByteComparable.Version byteComparableVersion,
                         boolean endsInclusive,
                         long[] nextPositions,
                         ByteSource.Peekable[] sources,
                         int startIdx,
                         int endIdxExclusive,
                         long currentPosition,
                         RangeState currentState)
    {
        this.byteComparableVersion = byteComparableVersion;
        this.nextPositions = nextPositions;
        this.sources = sources;
        this.currentIdx = startIdx;
        this.endIdx = endIdxExclusive;
        this.currentPosition = currentPosition;
        this.currentState = currentState;
        this.endsInclusive = endsInclusive;
    }

    @Override
    public long encodedPosition()
    {
        return currentPosition;
    }

    @Override
    public RangeState state()
    {
        return currentState;
    }

    @Override
    public Direction direction()
    {
        return Cursor.direction(currentPosition);
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return byteComparableVersion;
    }

    @Override
    public long advance()
    {
        if (currentIdx >= endIdx)
            return exhausted();

        // Advance the current idx
        currentPosition = nextPositions[currentIdx];

        // Also advance all others that are at the same position and have the same next byte.
        int endIdx = currentIdx + 1;
        while (endIdx < this.endIdx)
        {
            long cmp = Cursor.compare(currentPosition, nextPositions[endIdx]);
            assert cmp <= 0 : "Invalid order of range boundaries";
            if (cmp < 0)
                break;
            endIdx++;
        }

        return advanceBoundariesAndSelectState(endIdx);
    }

    private long advanceBoundariesAndSelectState(int endIdxExclusive)
    {
        int containedSelection = 0;
        // in reverse direction the roles of current and end idx are swapped
        if ((currentIdx & 1) != 0) // even left index means not valid before
            containedSelection |= direction().select(RangeState.APPLICABLE_BEFORE, RangeState.APPLICABLE_AFTER);

        int applicableEndExclusive = endIdxExclusive;

        if (currentIdx < endIdxExclusive)
        {
            int nextByte = sources[currentIdx].next();
            if (nextByte == ByteSource.END_OF_STREAM)
            {
                containedSelection |= RangeState.IS_BOUNDARY; // exact match, point and children included; reportable node

                while (nextByte == ByteSource.END_OF_STREAM && ++currentIdx < endIdxExclusive)
                    nextByte = sources[currentIdx].next();

                applicableEndExclusive = currentIdx;
            }
            if (currentIdx < endIdxExclusive)
            {
                int advancingIdx = currentIdx;
                nextPositions[advancingIdx] = maybeOnReturnPath(Cursor.positionForDescentWithByte(currentPosition, nextByte), advancingIdx);
                while (++advancingIdx < endIdxExclusive)
                {
                    nextByte = sources[advancingIdx].next();
                    nextPositions[advancingIdx] = maybeOnReturnPath(Cursor.positionForDescentWithByte(currentPosition, nextByte), advancingIdx);
                }
            }
        }

        if ((applicableEndExclusive & 1) != 0) // even end index means not valid after
            containedSelection |= direction().select(RangeState.APPLICABLE_AFTER, RangeState.APPLICABLE_BEFORE);

        currentState = RangeState.values()[containedSelection];
        return currentPosition;
    }

    long maybeOnReturnPath(long nextPosition, int index)
    {
        if (sources[index].peek() != ByteSource.END_OF_STREAM)
            return nextPosition;

        if (endsInclusive)
        {
            if ((index & 1) != 0)
                return nextPosition | ON_RETURN_PATH_BIT;
            else
                return nextPosition;
        }
        else if (!direction().isForward())
            return nextPosition | ON_RETURN_PATH_BIT;
        else
            return nextPosition;
    }

    // Note: Sets don't need `advanceMultiple` because they are meant to apply as a restriction on other tries,
    // and the combined walks necessary to implement such restrictions can only proceed one step at a time.
    // Once the restriction identifies that a branch in covered by the set, it can use the trie's `advanceMultiple`
    // method.

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        while (currentIdx < endIdx
               && Cursor.compare(nextPositions[currentIdx], encodedSkipPosition) < 0)
            currentIdx++;
        return advance();
    }

    private long exhausted()
    {
        currentPosition = Cursor.exhaustedPosition(currentPosition);
        return advanceBoundariesAndSelectState(endIdx);
    }

    @Override
    public TrieSetCursor tailCursor(Direction direction)
    {
        return tailCopyOf(this, direction);
    }

    private static RangesCursor tailCopyOf(RangesCursor copyFrom, Direction newDirection)
    {
        assert !Cursor.isOnReturnPath(copyFrom.currentPosition)
            : "Cannot take tail of a position " + Cursor.toString(copyFrom.currentPosition) + " on the return path.";
        boolean directionMatches = newDirection == copyFrom.direction();
        int startInclusive = copyFrom.currentIdx;
        int endExclusive = findEndOfMatchingValues(startInclusive, copyFrom.endIdx, copyFrom.nextPositions, copyFrom.currentPosition | ON_RETURN_PATH_BIT);

        if (startInclusive == endExclusive)
            return boundaryMatchingCursor(copyFrom, newDirection);

        int arrayStart = startInclusive & ~1;
        int arrayEnd = ((endExclusive + 1) & ~1);

        final long depthDiff = Cursor.depthCorrectionValue(copyFrom.currentPosition);
        ByteSource.Peekable[] sources = new ByteSource.Peekable[arrayEnd - arrayStart];
        final long[] nextPositions = new long[arrayEnd - arrayStart];

        RangeState state = copyFrom.currentState;
        // Get the state applicable to the tail branch. This is the succeeding side of the current state.
        if (state.isBoundary)
            state = state.precedingState(copyFrom.direction().opposite());
        if (state == null)
            state = RangeState.START_END_PREFIX;

        // Duplicate all boundaries that are positioned at the current point (if they are not, they cannot affect the
        // tail trie).
        // We can only drop an even number of boundaries on either size.
        if (directionMatches)
        {
            for (int i = startInclusive; i < endExclusive; ++i)
            {
                if (copyFrom.sources[i] != null)
                {
                    ByteSource.Duplicatable dupe = ByteSource.duplicatable(copyFrom.sources[i]);
                    copyFrom.sources[i] = dupe;
                    sources[i - arrayStart] = dupe.duplicate();
                }
                nextPositions[i - arrayStart] = copyFrom.nextPositions[i] - depthDiff;
            }

            return new RangesCursor(copyFrom.byteComparableVersion,
                                    copyFrom.endsInclusive,
                                    nextPositions,
                                    sources,
                                    startInclusive - arrayStart,
                                    endExclusive - arrayStart,
                                    Cursor.rootPosition(newDirection),
                                    state);
        }
        else
        {
            for (int i = startInclusive; i < endExclusive; ++i)
            {
                if (copyFrom.sources[i] != null)
                {
                    ByteSource.Duplicatable dupe = ByteSource.duplicatable(copyFrom.sources[i]);
                    copyFrom.sources[i] = dupe;
                    sources[arrayEnd - 1 - i] = dupe.duplicate();
                }
                nextPositions[arrayEnd - 1 - i] = (copyFrom.nextPositions[i] - depthDiff) ^ TRANSITION_MASK;
                if (sources[arrayEnd - 1 - i].peek() == ByteSource.END_OF_STREAM)
                    nextPositions[arrayEnd - 1 - i] ^= ON_RETURN_PATH_BIT;
            }

            return new RangesCursor(copyFrom.byteComparableVersion,
                                    copyFrom.endsInclusive,
                                    nextPositions,
                                    sources,
                                    arrayEnd - endExclusive,
                                    arrayEnd - startInclusive,
                                    Cursor.rootPosition(newDirection),
                                    state); // state is not dependent on the direction
        }

    }

    private static int findEndOfMatchingValues(int startInclusive, int completedExclusive, long[] nextPositions, long positionLimit)
    {
        int endExclusive;
        for (endExclusive = startInclusive;
             endExclusive < completedExclusive && Cursor.compare(nextPositions[endExclusive], positionLimit) < 0;
             endExclusive++)
        {
        }
        return endExclusive;
    }

    private static RangesCursor boundaryMatchingCursor(RangesCursor copyFrom, Direction newDirection)
    {
        // There are no further ranges to follow. The current state is the only thing we need to present, but
        // we need to make sure we present the right final state after advancing over the current state.
        // Prepare a combination of current and completed index that produces true or false precedingIncluded() result
        // on the exhausted() call.
        final RangeState state = copyFrom.currentState;
        // This gives us the included/excluded state after the current position.
        boolean included = state.precedingIncluded(copyFrom.direction().opposite());

        return new RangesCursor(copyFrom.byteComparableVersion,
                                copyFrom.endsInclusive,
                                null,
                                null,
                                included ? 1 : 0,
                                included ? 1 : 0,
                                Cursor.rootPosition(newDirection),
                                included ? RangeState.END_START_PREFIX : RangeState.START_END_PREFIX);
    }
}
