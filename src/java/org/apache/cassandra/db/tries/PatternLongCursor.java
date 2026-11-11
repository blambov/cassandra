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
    // Whether the wildcard is a * (% in SQL like syntax)
    final long[] isStar;
    // Which positions are a match for the given byte (taking wildcards into account)
    final long[][] patternMatch;

    final long matchBit;
    final boolean hasTrailingStar;

    long[][] statesHistory;
    boolean isMatch;

    int trailingStarMatchDepth = Integer.MAX_VALUE;

    public static <T> PatternLongCursor<T> create(Cursor<T> wrapped, byte[] patternBytes)
    {
        int length = patternBytes.length;
        boolean hasTrailingStar = length > 0 && patternBytes[length - 1] == '*';
        if (hasTrailingStar)
            --length;
        int numBytes = Integer.divideUnsigned(length, 64) + 1;
        long[] isWildcard = new long[numBytes];
        long[] isStar = new long[numBytes];
        long[][] patternMatch = new long[256][];
        long bit = 1;
        int pos = 0;
        for (int i = 0; i < patternBytes.length; ++i)
        {
            int patternByte = patternBytes[i] & 0xFF;
            if (patternByte == '*')
            {
                isStar[pos] |= bit;
                isWildcard[pos] |= bit;
            }
            else if (patternByte == '?')
                isWildcard[pos] |= bit;
            else
            {
                long[] patternEntry = patternMatch[patternByte];
                if (patternEntry == null)
                    patternMatch[patternByte] = patternEntry = new long[numBytes];
                patternEntry[pos] |= bit;
            }

            bit <<= 1;
            if (bit == 0)
            {
                ++pos;
                bit = 1;
            }
        }
        for (int i = 0; i < 256; ++i)
        {
            long[] patternEntry = patternMatch[i];
            if (patternEntry == null)
                patternMatch[i] = isWildcard;
            else for (int j = 0; j < numBytes; ++j)
                patternEntry[j] |= isWildcard[j];
        }
        return new PatternLongCursor<>(wrapped, length, hasTrailingStar, isStar, patternMatch);
    }

    private PatternLongCursor(Cursor<T> wrapped,
                            int length,
                            boolean hasTrailingStar,
                            long[] isStar,
                            long[][] patternMatch)
    {
        this.wrapped = wrapped;
        this.isStar = isStar;
        this.patternMatch = patternMatch;
        this.hasTrailingStar = hasTrailingStar;
        this.numLongs = isStar.length;
        matchBit = 1L << (length % 64); // bit position in the last long
        statesHistory = new long[32][];
        statesHistory[0] = new long[numLongs];
        long state = 1 | (isStar[0] & 1) << 1;
        statesHistory[0][0] = state;
        isMatch = (statesHistory[0][numLongs - 1] & matchBit) != 0;
    }

    private PatternLongCursor(PatternLongCursor<T> source,
                            Cursor<T> wrappedTail)
    {
        this.wrapped = wrappedTail;
        this.isStar = source.isStar;
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
        // set same bit if isStar
        // set next bit if isWildcard or byte is a match (both encoded in patternMatch)
        // if next bit gets set, also set one after next if next char is a star
        long carry = 0;
        long sum = 0;
        long[] byteMatch = patternMatch[b];
        for (int i = 0; i < numLongs; ++i)
        {
            long match = current[i] & byteMatch[i];
            long matchShifted = match << 1 | carry;
            long nextIsStar = matchShifted & isStar[i]; // matchShifted also has the effect of nextIsStar, but this is okay because we don't have double stars
            sum |= next[i] = current[i] & isStar[i] | matchShifted | nextIsStar << 1;
            carry = (match | nextIsStar) >>> 63;
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
