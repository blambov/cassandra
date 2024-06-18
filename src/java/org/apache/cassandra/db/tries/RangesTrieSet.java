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

import java.util.Arrays;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 * A trie set that represents a set of ranges.
 *
 * Ranges in this trie design always include all prefixes and all descendants of the start and end points. That is,
 * the range of ("abc", "afg") includes "a", "ab", "abc", "af, "afg", as well as any string staring with "abc", "ac",
 * "ad", "ae", "afg". The range of ("abc", "abc") includes "a", "ab", "abc" and any string starting with "abc".
 * Ranges that contain prefixes (e.g. ("ab", "abc")) are invalid as they cannot be specified without violating order in
 * one of the directions (i.e. "ab" is before "abc" in forward order, but not after it in reverse).
 *
 * The reason for these restrictions is to ensure that all individual ranges are contiguous spans when iterated in
 * forward as well as backward order. This in turn makes it possible to have simple range trie and intersection
 * implementations where the representations of range trie sets do not differ when iterated in the two directions.
 *
 * This type of ranges are actually preferable to us, because we use prefix-free keys with terminators that leave
 * room for greater- and less-than positions, and at prefix nodes we store metadata applicable to the whole branch.
 *
 * [a, a, a, b] == [a, b] -- same as single when odd count
 * [a, b, b, c] == [a, c] -- ignore if start odd, even count
 * [a, a, c, d] == point a + [c, d] -- point if start even, even count
 */
public class RangesTrieSet implements TrieSetWithImpl
{
    final ByteComparable[] boundaries;  // start, end, start, end, ...

    public RangesTrieSet(ByteComparable... boundaries)
    {
        this.boundaries = boundaries;
    }

    public static TrieSetWithImpl create(ByteComparable... boundaries)
    {
        return new RangesTrieSet(boundaries);
    }

    @Override
    public Cursor makeCursor(Direction direction)
    {
        return new RangesCursor(direction, boundaries);
    }

    static final RangeState CONTAINED_SELECTIONS[] = new RangeState[]
    {
    RangeState.START_END_PREFIX,    // excluded before, excluded after
    RangeState.END_PREFIX,          // included before, excluded after
    RangeState.START_PREFIX,        // excluded before, included after
    RangeState.END_START_PREFIX,    // included before, included after
    RangeState.POINT,               // excluded before, excluded after, point and children included
    RangeState.END,                 // included before, excluded after, point and children included
    RangeState.START,               // excluded before, included after, point and children included
    RangeState.COVERED              // included before, included after, point and children included
    };

    private static class RangesCursor implements Cursor
    {
        private final Direction direction;
        private final int completedIdx;
        ByteSource[] sources;
        int[] nexts;
        int[] depths;
        int currentIdx;
        int currentDepth;
        int currentTransition;
        RangeState currentState;

        public RangesCursor(Direction direction, ByteComparable[] boundaries)
        {
            this.direction = direction;
            // handle empty array (== full range) and nulls at the end (same as not there, odd length == open end range)
            int length = boundaries.length;
            if (length == 0)
            {
                boundaries = new ByteComparable[]{ null };
                length = 1;
            }
            if (length > 1 && boundaries[length - 1] == null)
                --length;

            nexts = new int[length];
            depths = new int[length];
            sources = new ByteSource[length];
            int first = 0;
            if (boundaries[0] == null)
            {
                first = 1;
                sources[0] = null;
                nexts[0] = ByteSource.END_OF_STREAM;
            }
            currentIdx = direction.select(first, length - 1);
            for (int i = first; i < length; ++i)
            {
                depths[i] = 1;
                if (boundaries[i] != null)
                {
                    sources[i] = boundaries[i].asComparableBytes(BYTE_COMPARABLE_VERSION);
                    nexts[i] = sources[i].next();
                }
                else
                    throw new AssertionError("Null can only be used as the first or last boundary.");
            }
            currentDepth = 0;
            currentTransition = -1;
            completedIdx = direction.select(length - 1, first);
            skipCompletedAndSelectContained(currentIdx < length ? nexts[currentIdx] : ByteSource.END_OF_STREAM,
                                            completedIdx);
        }

        RangesCursor(RangesCursor copyFrom)
        {
            this.direction = copyFrom.direction;
            // In forward direction, an even number of completed sources can be dropped.
            int first = direction.select(copyFrom.currentIdx & -2, 0);
            // In reverse, any completed one can be dropped.
            int last = direction.select(copyFrom.sources.length, copyFrom.currentIdx + 1);
            this.nexts = Arrays.copyOfRange(copyFrom.nexts, first, last);
            this.depths = Arrays.copyOfRange(copyFrom.depths, first, last);
            this.sources = new ByteSource[last - first];
            for (int i = copyFrom.currentIdx;
                 direction.inLoop(i,  first, last - 1);
                 i += direction.increase)
                if (copyFrom.sources[i] != null)
                {
                    ByteSource.Duplicatable dupe = ByteSource.duplicatable(copyFrom.sources[i]);
                    copyFrom.sources[i] = dupe;
                    sources[i - first] = dupe.duplicate();
                }
            this.currentIdx = copyFrom.currentIdx - first;
            this.completedIdx = copyFrom.completedIdx - first;
            this.currentDepth = copyFrom.currentDepth;
            this.currentTransition = copyFrom.currentTransition;
            this.currentState = copyFrom.currentState;
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
        public RangeState state()
        {
            return currentState;
        }

        @Override
        public Direction direction()
        {
            return direction;
        }

        @Override
        public int advance()
        {
            if (direction.gt(currentIdx, completedIdx))
                return exhausted();
            currentTransition = nexts[currentIdx];
            currentDepth = depths[currentIdx]++;
            int next = currentTransition != ByteSource.END_OF_STREAM
                       ? nexts[currentIdx] = sources[currentIdx].next()
                       : ByteSource.END_OF_STREAM;

            int endIdx = currentIdx + direction.increase;
            while (direction.le(endIdx, completedIdx)
                   && depths[endIdx] == currentDepth && nexts[endIdx] == currentTransition)
            {
                depths[endIdx]++;
                nexts[endIdx] = sources[endIdx].next();
                endIdx += direction.increase;
            }

            return skipCompletedAndSelectContained(next, endIdx - direction.increase);
        }

        private int skipCompletedAndSelectContained(int next, int endIdx)
        {
            int containedSelection = 0;
            // in reverse direction the roles of current and end idx are swapped
            containedSelection |= (direction.select(currentIdx, endIdx) & 1); // even left index means not valid before
            containedSelection |= ((direction.select(endIdx, currentIdx) & 1) ^ 1) << 1; // even end index means not valid after
            if (next == ByteSource.END_OF_STREAM)
            {
                containedSelection |= 4; // exact match, point and children included; reportable node
                while (direction.le(currentIdx, endIdx))
                {
                    assert nexts[currentIdx] == ByteSource.END_OF_STREAM : "Prefixes are not allowed in trie ranges.";
                    currentIdx += direction.increase;
                }
            }
            currentState = CONTAINED_SELECTIONS[containedSelection];
            return currentDepth;
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            while (direction.le(currentIdx, completedIdx)
                   && (depths[currentIdx] > skipDepth ||
                       depths[currentIdx] == skipDepth && direction.lt(nexts[currentIdx], skipTransition)))
                currentIdx += direction.increase;
            return advance();
        }

        private int exhausted()
        {
            currentDepth = -1;
            currentTransition = -1;
            return skipCompletedAndSelectContained(0, completedIdx);
        }

        @Override
        public Cursor duplicate()
        {
            return new RangesCursor(this);
        }

        // TODO: Implement tailCursor
    }
}
