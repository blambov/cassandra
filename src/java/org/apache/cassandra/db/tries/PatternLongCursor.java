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
import java.util.BitSet;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

class PatternLongCursor<T> implements Cursor<T>
{
    // TODO: Use skipTo on source when the state doesn't contain a wildcard position
    // TODO: javadoc/comments


    final Cursor<T> wrapped;
    final int numLongs;
    // Positions that can be repeated (any-string wildcards and UTF-8 continuation loops)
    final long[] isLoop;
    // Which positions are a match for the given byte (taking wildcards into account)
    final long[][] patternMatch;

    final long matchBit;
    final boolean hasTrailingStar;

    long[][] statesHistory;
    boolean isMatch;

    int trailingStarMatchDepth = Integer.MAX_VALUE;

    public static <T> PatternLongCursor<T> create(Cursor<T> wrapped, TriePattern pattern)
    {
        int[] elements = pattern.elements;
        int length = elements.length;
        boolean hasTrailingStar = length > 0 && elements[length - 1] == TriePattern.ANY_STRING;
        if (hasTrailingStar)
            --length;
        int numLongs = Integer.divideUnsigned(length, 64) + 1;
        long[] isLoop = new long[numLongs];
        long[][] patternMatch = new long[256][];
        for (int i = 0; i < elements.length; ++i)
        {
            int element = elements[i];
            int pos = i >>> 6;
            long bit = 1L << (i & 63);
            if (TriePattern.isLoop(element))
                isLoop[pos] |= bit;
            for (int b = 0; b < 256; ++b)
            {
                if (TriePattern.accepts(element, b))
                {
                    long[] patternEntry = patternMatch[b];
                    if (patternEntry == null)
                        patternMatch[b] = patternEntry = new long[numLongs];
                    patternEntry[pos] |= bit;
                }
            }
        }
        long[] noMatch = new long[numLongs];
        for (int b = 0; b < 256; ++b)
            if (patternMatch[b] == null)
                patternMatch[b] = noMatch;
        return new PatternLongCursor<>(wrapped, length, hasTrailingStar, isLoop, patternMatch);
    }

    private PatternLongCursor(Cursor<T> wrapped,
                            int length,
                            boolean hasTrailingStar,
                            long[] isLoop,
                            long[][] patternMatch)
    {
        this.wrapped = wrapped;
        this.isLoop = isLoop;
        this.patternMatch = patternMatch;
        this.hasTrailingStar = hasTrailingStar;
        this.numLongs = isLoop.length;
        matchBit = 1L << (length % 64); // bit position in the last long
        statesHistory = new long[32][];
        statesHistory[0] = new long[numLongs];
        // Entering a loop position also enters the next one, as the loop can be skipped.
        long state = 1 | (isLoop[0] & 1) << 1;
        statesHistory[0][0] = state;
        isMatch = (statesHistory[0][numLongs - 1] & matchBit) != 0;
    }

    private PatternLongCursor(PatternLongCursor<T> source,
                            Cursor<T> wrappedTail)
    {
        this.wrapped = wrappedTail;
        this.isLoop = source.isLoop;
        this.patternMatch = source.patternMatch;
        this.hasTrailingStar = source.hasTrailingStar;
        this.numLongs = source.numLongs;
        this.matchBit = source.matchBit;
        this.statesHistory = new long[32][];
        // Copy, because the source will reuse its state array when it moves to another node at that depth.
        long[] state = source.statesHistory[Cursor.depth(source.encodedPosition())].clone();
        this.statesHistory[0] = state;
        this.isMatch = (state[numLongs - 1] & matchBit) != 0;
    }

    @Override
    public long encodedPosition()
    {
        return isMatch ? wrapped.encodedPosition() : wrapped.encodedPosition() & ~MAY_HAVE_CONTENT_BIT;
    }

    @Override
    public T content()
    {
        return isMatch ? wrapped.content() : null;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return wrapped.byteComparableVersion();
    }

    private long advanceTo(long encodedPosition)
    {
        while (true)
        {
            int depth = Cursor.depth(encodedPosition);
            if (depth > trailingStarMatchDepth || depth < 0)
                return encodedPosition;

            trailingStarMatchDepth = Integer.MAX_VALUE;
            long[] next = stateAt(depth);
            boolean nonNull = true;
            if (depth > 0)
            {
                long[] prev = statesHistory[depth - 1];
                nonNull = setNextState(Cursor.incomingTransition(encodedPosition), next, prev);
            }

            if (nonNull)
            {
                isMatch = (next[numLongs - 1] & matchBit) != 0;
                if (isMatch && hasTrailingStar)
                    trailingStarMatchDepth = depth;
                return isMatch ? encodedPosition : encodedPosition & ~MAY_HAVE_CONTENT_BIT;
            }

            encodedPosition = wrapped.skipTo(Cursor.positionForSkippingBranch(encodedPosition));
        }
    }

    @Override
    public long advance()
    {
        return advanceTo(wrapped.advance());
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        if (isMatch && hasTrailingStar)
            return advanceTo(wrapped.advanceMultiple(receiver));
        else
            return advanceTo(wrapped.advance());
    }

    @Override
    public long skipTo(long skipPosition)
    {
        return advanceTo(wrapped.skipTo(skipPosition));
    }

    private long[] stateAt(int depth)
    {
        if (depth >= statesHistory.length)
            statesHistory = Arrays.copyOf(statesHistory, statesHistory.length * 2);
        long[] s = statesHistory[depth];
        if (s == null)
            statesHistory[depth] = s = new long[numLongs];
        return s;
    }


    private boolean setNextState(int b, long[] next, long[] current)
    {
        // positions that accept the byte (wildcards included, all encoded in patternMatch):
        // - set the same bit if the position is a loop
        // - set the next bit
        // - if the next bit is a loop, also set the one after it (the loop can be skipped)
        long carry = 0;
        long sum = 0;
        long[] byteMatch = patternMatch[b];
        for (int i = 0; i < numLongs; ++i)
        {
            long match = current[i] & byteMatch[i];
            long matchShifted = match << 1 | carry;
            long nextIsLoop = matchShifted & isLoop[i]; // matchShifted also has the effect of nextIsLoop, but this is okay because loops are never adjacent
            sum |= next[i] = match & isLoop[i] | matchShifted | nextIsLoop << 1;
            carry = (match | nextIsLoop) >>> 63;
        }
        return sum != 0;
    }

    @Override
    public Cursor<T> tailCursor(Direction direction)
    {
        if (isMatch && hasTrailingStar)
            return wrapped.tailCursor(direction);
        return new PatternLongCursor(this, wrapped.tailCursor(direction));
    }
}
