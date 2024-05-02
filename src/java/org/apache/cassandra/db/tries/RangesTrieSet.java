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

public class RangesTrieSet implements TrieSetWithImpl
{
    final ByteComparable[] boundaries;  // start, end, start, end, ...

    public RangesTrieSet(ByteComparable... boundaries)
    {
        this.boundaries = boundaries;
    }

    public static TrieSetWithImpl create(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        if (!includeLeft && left != null)
            left = add0(left);
        if (includeRight && right != null)
            right = add0(right);
        return create(left, right);
    }

    public static TrieSetWithImpl create(ByteComparable left, ByteComparable right)
    {
        return new RangesTrieSet(left, right);
    }

    private static ByteComparable add0(ByteComparable v)
    {
        return version -> add0(v.asComparableBytes(version));
    }

    private static ByteSource add0(ByteSource src)
    {
        return new ByteSource()
        {
            boolean done = false;
            @Override
            public int next()
            {
                if (done)
                    return END_OF_STREAM;
                int v = src.next();
                if (v != END_OF_STREAM)
                    return v;
                done = true;
                return 0;
            }
        };
    }

    @Override
    public Cursor makeCursor(Direction direction)
    {
        return new RangesCursor(direction, boundaries);
    }

    static final RangeState CONTAINED_SELECTIONS[] = new RangeState[]
    {
    RangeState.OUTSIDE_PREFIX, // even index, no match: before a start
    RangeState.INSIDE_PREFIX,  // odd index, no match: prefix of an end
    RangeState.END,            // even index, match: went over an end
    RangeState.START           // odd index, match: went over a start
    };

    private static class RangesCursor implements Cursor
    {
        private final Direction direction;
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
            while (length > 1 && boundaries[length - 1] == null)
                --length;

            nexts = new int[length];
            depths = new int[length];
            sources = new ByteSource[length];
            currentIdx = direction.select(0, length - 1);
            for (int i = 0; i < length; ++i)
            {
                depths[i] = 1;
                if (boundaries[i] != null)
                {
                    sources[i] = boundaries[i].asComparableBytes(BYTE_COMPARABLE_VERSION);
                    nexts[i] = sources[i].next();
                }
                else if (i == 0)
                {
                    sources[i] = null;
                    nexts[i] = ByteSource.END_OF_STREAM;
                }
                else
                    throw new AssertionError("Null can only be used as the first or last boundary.");
            }
            currentDepth = 0;
            currentTransition = -1;
            skipCompletedAndSelectContained(nexts[currentIdx], direction.select(length - 1, 0));
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
        public int advance()
        {
            if (!direction.inLoop(currentIdx, 0, nexts.length - 1))
                return exhausted();
            currentTransition = nexts[currentIdx];
            currentDepth = depths[currentIdx]++;
            int next = nexts[currentIdx] = sources[currentIdx].next();
            int endIdx = currentIdx + direction.increase;
            while (direction.inLoop(endIdx, 0, nexts.length - 1)
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
            if (next == ByteSource.END_OF_STREAM)
            {
                while (direction.le(currentIdx, endIdx) && nexts[currentIdx] == ByteSource.END_OF_STREAM)
                {
                    currentIdx += direction.increase;
                    containedSelection ^= 2;    // only an odd number of completed sources has any effect
                }
            }
            containedSelection |= (currentIdx & 1) ^ (direction.isForward() ? 0 : 1); // 1 if odd index
            currentState = CONTAINED_SELECTIONS[containedSelection];
            return currentDepth;
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            while (direction.inLoop(currentIdx, 0, nexts.length - 1)
                   && (depths[currentIdx] > skipDepth ||
                       depths[currentIdx] == skipDepth && direction.lt(nexts[currentIdx], skipTransition)))
                currentIdx += direction.increase;
            return advance();
        }

        private int exhausted()
        {
            currentDepth = -1;
            currentTransition = -1;
            return skipCompletedAndSelectContained(0, nexts.length - 1);
        }

        @Override
        public Cursor duplicate()
        {
            return new RangesCursor(this);
        }
    }
}
