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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

class PatternCursor<T> implements Cursor<T>
{
    // TODO: Handle UTF8 multi-byte characters
    // TODO: Escaping and SQL % and _ wildcards
    // TODO: Use skipTo on source when the state doesn't contain a wildcard position
    // TODO: javadoc/comments


    final Cursor<T> wrapped;
    // The pattern bytes, unescaped. Ignored for wildcard positions.
    final byte[] pattern;
    // Whether we have a wildcard at this position (*/% or ?/_)
    final BitSet isWildcard;
    // Whether the wildcard is a * (% in SQL like syntax)
    final BitSet isStar;

    final int matchPosition;
    final boolean hasTrailingStar;

    BitSet[] statesHistory;

    int trailingStarMatchDepth = Integer.MAX_VALUE;
    boolean isMatch;

    public static <T> Cursor<T> create(Cursor<T> wrapped, String pattern)
    {
        return create(wrapped, pattern, 0);
    }

    public static <T> Cursor<T> create(Cursor<T> wrapped, String pattern, int algoChoice)
    {
        // We can do some trivial optimizations (move ? before *, collapse multiple stars)
        String simplified = pattern;
        do
        {
            pattern = simplified;
            simplified = pattern.replaceAll("\\*+(\\?*)", "$1*");
        }
        while (!simplified.equals(pattern));

        // POC without escaping
        byte[] patternBytes = pattern.getBytes(StandardCharsets.UTF_8);
        if (algoChoice == 1)
            return PatternLongCursor.create(wrapped, patternBytes);
        if (algoChoice != 2 && patternBytes.length < 64)
            return Pattern64Cursor.create(wrapped, patternBytes);

        BitSet isWildcard = new BitSet(patternBytes.length);
        BitSet isStar = new BitSet(patternBytes.length);
        for (int i = 0; i < patternBytes.length; ++i)
        {
            if (patternBytes[i] == '*')
            {
                isStar.set(i);
                isWildcard.set(i);
            }
            else if (patternBytes[i] == '?')
                isWildcard.set(i);
        }
        return new PatternCursor<>(wrapped, patternBytes, isWildcard, isStar);
    }

    private PatternCursor(Cursor<T> wrapped,
                          byte[] pattern,
                          BitSet isWildcard,
                          BitSet isStar)
    {
        this.wrapped = wrapped;
        this.pattern = pattern;
        this.isWildcard = isWildcard;
        this.isStar = isStar;
        hasTrailingStar = pattern.length > 0 && isStar.get(pattern.length - 1);
        matchPosition = hasTrailingStar ? pattern.length - 1 : pattern.length;
        statesHistory = new BitSet[32];
        BitSet state = new BitSet(pattern.length);
        state.set(0);
        if (isStar.get(0))
            state.set(1);
        statesHistory[0] = state;
        isMatch = state.get(matchPosition);
    }

    private PatternCursor(PatternCursor<T> source,
                          Cursor<T> wrappedTail)
    {
        this.wrapped = wrappedTail;
        this.pattern = source.pattern;
        this.isWildcard = source.isWildcard;
        this.isStar = source.isStar;
        hasTrailingStar = source.hasTrailingStar;
        matchPosition = source.matchPosition;
        statesHistory = new BitSet[32];
        // Copy, because the source will reuse its state object when it moves to another node at that depth.
        BitSet state = (BitSet) source.statesHistory[Cursor.depth(source.encodedPosition())].clone();
        statesHistory[0] = state;
        isMatch = state.get(matchPosition);
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
            BitSet next = stateAt(depth);
            if (depth > 0)
            {
                BitSet prev = statesHistory[depth - 1];
                setNextState(Cursor.incomingTransition(encodedPosition), next, prev);
            }

            if (!next.isEmpty())
            {
                isMatch = next.get(matchPosition);
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

    private BitSet stateAt(int depth)
    {
        if (depth >= statesHistory.length)
            statesHistory = Arrays.copyOf(statesHistory, statesHistory.length * 2);
        BitSet s = statesHistory[depth];
        if (s == null)
            statesHistory[depth] = s = new BitSet(matchPosition + 1);
        return s;
    }

    private void setNextState(int b, BitSet next, BitSet current)
    {
        next.clear();
        // next = current & isStar | (current & isWildcard | (current << 1) & isStar | current & compare(b, pattern) << 1)

        for (int i = current.previousSetBit(matchPosition); i >= 0; i = current.previousSetBit(i - 1))
        {
            if (isWildcard.get(i))
            {
                next.set(i + 1);
                if (isStar.get(i))
                {
                    next.set(i);
                    // There's no need to process anything further as all previous structure has already been matched.
                    return;
                }
                else if (isStar.get(i + 1))
                    next.set(i + 2);
            }
            else if (i < pattern.length && (pattern[i] & 0xFF) == b)
            {
                next.set(i + 1);
                if (isStar.get(i + 1))
                    next.set(i + 2);
            }
        }
    }

    @Override
    public Cursor<T> tailCursor(Direction direction)
    {
        if (isMatch && hasTrailingStar)
            return wrapped.tailCursor(direction);
        return new PatternCursor(this, wrapped.tailCursor(direction));
    }
}
