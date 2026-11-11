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

import org.agrona.DirectBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

class Pattern64Cursor<T> implements Cursor<T>, Cursor.TransitionsReceiver
{
    // TODO: javadoc/comments


    final Cursor<T> wrapped;
    // Whether the wildcard is a * (% in SQL like syntax)
    final long isStar;
    // Which positions are a match for the given byte (taking wildcards into account)
    final long[] patternMatch;

    final long matchBit;

    long[] statesHistory;
    long currentState;
    boolean isMatch;

    TransitionsReceiver chainedReceiver;
    int curDepth;
    int trailingStarMatchDepth = Integer.MAX_VALUE;

    public static <T> Pattern64Cursor<T> create(Cursor<T> wrapped, byte[] patternBytes)
    {
        assert patternBytes.length < 64;
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
        long matchBit = 1L << pattern.length;
        if ((isStar & matchBit >>> 1) != 0)
            matchBit >>>= 1; // we can end early on trailing stars
        this.matchBit = matchBit;
        statesHistory = new long[32];
        long state = 1 | (isStar & 1) << 1;
        statesHistory[0] = state;
        curDepth = 0;
        setMatchFields(state);
    }

    private void setMatchFields(long state)
    {
        assert state != 0;
        currentState = state;
        isMatch = (state & matchBit) != 0;
    }

    private Pattern64Cursor(Pattern64Cursor<T> source,
                            Cursor<T> wrappedTail)
    {
        this.wrapped = wrappedTail;
        this.isStar = source.isStar;
        this.patternMatch = source.patternMatch;
        matchBit = source.matchBit;
        statesHistory = new long[32];
        long state = source.statesHistory[Cursor.depth(source.encodedPosition())];
        statesHistory[0] = state;
        curDepth = 0;
        setMatchFields(state);
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
            int depth = curDepth = Cursor.depth(encodedPosition);
            if (depth > trailingStarMatchDepth || depth < 0)
                return encodedPosition;

            trailingStarMatchDepth = Integer.MAX_VALUE;
            long next;
            long prev = 0;
            if (depth > 0)
            {
                prev = statesHistory[depth - 1];
                next = advanceWith(prev, Cursor.incomingTransition(encodedPosition), depth);
            }
            else
                next = statesHistory[0]; // root return path, keep the initial (or inherited for tails) state

            if (next != 0)
            {
                setMatchFields(next);
                if ((next & isStar & matchBit) != 0)
                    trailingStarMatchDepth = depth;
                return isMatch ? encodedPosition : encodedPosition & ~MAY_HAVE_CONTENT_BIT;
            }

            // We won't end up here very often (only if there's no star match). If we do, skip to a byte that is
            // allowed by the pattern.
            int nextByte = Cursor.incomingTransition(encodedPosition);
            Direction direction = direction();
            do
            {
                nextByte += direction.increase;
            }
            while (direction.inLoop(nextByte, 0, 255) && (patternMatch[nextByte] & prev) == 0);

            encodedPosition = wrapped.skipTo(Cursor.encodeWithOverflow(depth, nextByte, direction));
        }
    }

    private long advanceWith(long prev, int transition, int depth)
    {
        long next = setNextState(transition, prev);
        if (depth >= statesHistory.length)
            statesHistory = Arrays.copyOf(statesHistory, statesHistory.length * 2);
        statesHistory[depth] = next;
        return next;
    }

    @Override
    public long advance()
    {
        return advanceTo(wrapped.advance());
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        if ((currentState & isStar) != 0)
        {
            // we don't need to track anything after a trailing star, only check if we have left the branch
            if ((currentState & isStar & matchBit) != 0)
                return advanceTo(wrapped.advanceMultiple(receiver));

            // If there is a star in our state, we have to walk the full trie, so take advantage of advanceMultiple,
            // updating our state for each character.
            chainedReceiver = receiver;
            return advanceTo(wrapped.advanceMultiple(this));
        }
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
        if ((currentState & isStar & matchBit) != 0)
            return wrapped.tailCursor(direction);
        return new Pattern64Cursor(this, wrapped.tailCursor(direction));
    }

    @Override
    public void addPathByte(int nextByte)
    {
        currentState = advanceWith(currentState, nextByte, ++curDepth);
        if (chainedReceiver != null)
            chainedReceiver.addPathByte(nextByte);
    }

    @Override
    public void addPathBytes(DirectBuffer buffer, int pos, int count)
    {
        for (int i = 0; i < count; ++i)
            currentState = advanceWith(currentState, buffer.getByte(pos + i) & 0xFF, ++curDepth);
        if (chainedReceiver != null)
            chainedReceiver.addPathBytes(buffer, pos, count);
    }
}
