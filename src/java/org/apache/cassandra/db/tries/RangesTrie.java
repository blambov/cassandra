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

public class RangesTrie extends Trie<Trie.Contained>
{
    final ByteComparable[] boundaries;  // start, end, start, end, ...

    public RangesTrie(ByteComparable... boundaries)
    {
        this.boundaries = boundaries;
    }

    public static Trie<Contained> create(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        if (!includeLeft && left != null)
            left = add0(left);
        if (includeRight && right != null)
            right = add0(right);
        return create(left, right);
    }

    public static Trie<Contained> create(ByteComparable left, ByteComparable right)
    {
        return new RangesTrie(left, right);
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
    protected Cursor<Contained> cursor()
    {
        return new RangesCursor(boundaries);
    }

    private static class RangesCursor implements Cursor<Contained>
    {
        ByteSource[] sources;
        int[] nexts;
        int[] depths;
        int currentIdx;
        int currentDepth;
        int currentTransition;

        public RangesCursor(ByteComparable[] boundaries)
        {
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
            currentIdx = 0;
        }

        RangesCursor(RangesCursor copyFrom)
        {
            // An even number of completed sources can be dropped.
            int toDrop = currentIdx & -1;
            this.nexts = Arrays.copyOfRange(copyFrom.nexts, toDrop, copyFrom.nexts.length);
            this.depths = Arrays.copyOfRange(copyFrom.depths, toDrop, copyFrom.depths.length);
            this.sources = new ByteSource[copyFrom.sources.length - toDrop];
            for (int i = currentIdx; i < sources.length; i++)
                if (nexts[i] != ByteSource.END_OF_STREAM)
                {
                    ByteSource.Duplicatable dupe = ByteSource.duplicatable(copyFrom.sources[i]);
                    copyFrom.sources[i] = dupe;
                    sources[i - toDrop] = dupe.duplicate();
                }
            this.currentIdx = copyFrom.currentIdx - toDrop;
            this.currentDepth = copyFrom.currentDepth;
            this.currentTransition = copyFrom.currentTransition;
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
        public Contained content()
        {
            // there may be multiple sources that end on the same position; the last is the one that tells us our state
            int firstNonEnding = currentIdx;    // this may be length
            while (firstNonEnding < nexts.length && nexts[firstNonEnding] == ByteSource.END_OF_STREAM)
                ++firstNonEnding;

            if (firstNonEnding > currentIdx)
                return (firstNonEnding & 1) == 1 ? Contained.START : Contained.END;
            else
                return (firstNonEnding & 1) == 1 ? Contained.INSIDE_PREFIX : null;

        }

        @Override
        public int advance()
        {
            while (currentIdx < nexts.length && nexts[currentIdx] == ByteSource.END_OF_STREAM)
                ++currentIdx;
            return completeAdvance();
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            while (currentIdx < nexts.length &&
                    (depths[currentIdx] > skipDepth ||
                     depths[currentIdx] == skipDepth && nexts[currentIdx] < skipTransition))
                ++currentIdx;
            return completeAdvance();
        }

        private int completeAdvance()
        {
            if (currentIdx == nexts.length)
                return exhausted();
            currentTransition = nexts[currentIdx];
            currentDepth = depths[currentIdx]++;
            nexts[currentIdx] = sources[currentIdx].next();
            int endIdx = currentIdx + 1;
            while (endIdx < nexts.length && depths[endIdx] == currentDepth && nexts[endIdx] == currentTransition)
            {
                depths[endIdx]++;
                nexts[endIdx] = sources[endIdx].next();
                ++endIdx;
            }
            return currentDepth;
        }

        private int exhausted()
        {
            return currentDepth = -1;
        }

        @Override
        public Cursor<Contained> duplicate()
        {
            return new RangesCursor(this);
        }
    }
}
