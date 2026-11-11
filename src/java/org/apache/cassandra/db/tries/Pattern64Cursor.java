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

class Pattern64Cursor<T> implements Cursor<T>
{
    // TODO: Use skipTo on source when the state doesn't contain a wildcard position
    // TODO: javadoc/comments


    final Cursor<T> wrapped;
    // Whether the wildcard is a * (% in SQL like syntax)
    final long isStar;
    // Which positions are a match for the given byte (taking wildcards into account)
    final long[] patternMatch;

    final long matchBit;
    final boolean hasTrailingStar;

    long[] statesHistory;
    boolean isMatch;

    int trailingStarMatchDepth = Integer.MAX_VALUE;

    public static <T> Pattern64Cursor<T> create(Cursor<T> wrapped, byte[] patternBytes)
    {
        assert patternBytes.length < 64 || patternBytes.length == 64 && patternBytes[63] == '*';
        long isWildcard = 0;
        long isStar = 0;
        long[] patternMatch = new long[256];
        long bit = 1;
        for (int i = 0; i < patternBytes.length; ++i)
        {
            int patternByte = patternBytes[i] & 0xFF;
            if (patternByte == '*')
            {
                isStar |= bit;
                isWildcard |= bit;
            }
            else if (patternByte == '?')
                isWildcard |= bit;
            else
                patternMatch[patternByte] |= bit;
            bit <<= 1;
        }
        for (int i = 0; i < 256; ++i)
            patternMatch[i] |= isWildcard;
        return new Pattern64Cursor<>(wrapped, patternBytes, isStar, patternMatch);
    }

    private Pattern64Cursor(Cursor<T> wrapped,
                            byte[] pattern,
                            long isStar,
                            long[] patternMatch)
    {
        this.wrapped = wrapped;
        this.isStar = isStar;
        this.patternMatch = patternMatch;
        hasTrailingStar = pattern.length > 0 && get(isStar, pattern.length - 1);
        matchBit = 1L << (hasTrailingStar ? pattern.length - 1 : pattern.length);
        statesHistory = new long[32];
        long state = 1 | (isStar & 1) << 1;
        statesHistory[0] = state;
        isMatch = (state & matchBit) != 0;
    }

    private static boolean get(long mask, int index)
    {
        return (mask & (1 << index)) != 0;
    }

    private Pattern64Cursor(Pattern64Cursor<T> source,
                            Cursor<T> wrappedTail)
    {
        this.wrapped = wrappedTail;
        this.isStar = source.isStar;
        this.patternMatch = source.patternMatch;
        hasTrailingStar = source.hasTrailingStar;
        matchBit = source.matchBit;
        statesHistory = new long[32];
        long state = source.statesHistory[Cursor.depth(source.encodedPosition())];
        statesHistory[0] = state;
        isMatch = (state & matchBit) != 0;
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
            long next = 1;
            if (depth > 0)
            {
                long prev = statesHistory[depth - 1];
                next = setNextState(Cursor.incomingTransition(encodedPosition), prev);
            }
            if (depth >= statesHistory.length)
                statesHistory = Arrays.copyOf(statesHistory, statesHistory.length * 2);
            statesHistory[depth] = next;

            if (next != 0)
            {
                isMatch = (next & matchBit) != 0;
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

    private long setNextState(int b, long current)
    {
        // set same bit if isStar
        // set next bit if isWildcard or byte is a match (both encoded in patternMatch)
        // if next bit gets set, also set one after next if next char is a star
        long match = current & patternMatch[b];
        return current & isStar | (match | (match << 1 & isStar)) << 1;
    }

    @Override
    public Cursor<T> tailCursor(Direction direction)
    {
        if (isMatch && hasTrailingStar)
            return wrapped.tailCursor(direction);
        return new Pattern64Cursor(this, wrapped.tailCursor(direction));
    }
}
