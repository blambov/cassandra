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

/// [PatternMatcher] for patterns of up to 63 positions, whose states are stored in a single `long`: bit `i` is set
/// if position `i` is active, and bit `length` is the match position (the end of the pattern).
final class PatternMatcher64 extends PatternMatcher
{
    // Positions that are not a literal byte
    final long isWildcard;
    // Positions that can be repeated (any-string wildcards and UTF-8 continuation loops)
    final long isLoop;
    // Positions that are any-string wildcards (* in glob, % in SQL like syntax)
    final long isStar;
    // The positions that accept each byte
    final long[] patternMatch;
    // The bit that marks a match: the end of the pattern, or a trailing any-string wildcard, which matches everything
    // from the point it is reached
    final long matchBit;

    // The state at each depth of the current path
    long[] states;

    PatternMatcher64(TriePattern pattern)
    {
        super(pattern.elements);
        assert elements.length < 64;
        long isWildcard = 0;
        long isLoop = 0;
        long isStar = 0;
        long[] patternMatch = new long[256];
        long bit = 1;
        for (int element : elements)
        {
            if (TriePattern.isLiteral(element))
                patternMatch[element] |= bit;
            else
            {
                isWildcard |= bit;
                if (TriePattern.isLoop(element))
                    isLoop |= bit;
                if (element == TriePattern.ANY_STRING)
                    isStar |= bit;
                for (int b = 0; b < 256; ++b)
                    if (TriePattern.accepts(element, b))
                        patternMatch[b] |= bit;
            }
            bit <<= 1;
        }
        this.isWildcard = isWildcard;
        this.isLoop = isLoop;
        this.isStar = isStar;
        this.patternMatch = patternMatch;
        long matchBit = 1L << elements.length;
        if ((isStar & matchBit >>> 1) != 0)
            matchBit >>>= 1; // we can end early on trailing stars
        this.matchBit = matchBit;
        states = new long[32];
        // Entering a loop position also enters the next one, as the loop can be skipped.
        states[0] = 1 | (isLoop & 1) << 1;
    }

    private PatternMatcher64(PatternMatcher64 source, long rootState)
    {
        super(source.elements);
        isWildcard = source.isWildcard;
        isLoop = source.isLoop;
        isStar = source.isStar;
        patternMatch = source.patternMatch;
        matchBit = source.matchBit;
        states = new long[32];
        states[0] = rootState;
    }

    private long next(long state, int transition)
    {
        // positions that accept the byte (wildcards included, all encoded in patternMatch):
        // - set the same bit if the position is a loop
        // - set the next bit
        // - if the next bit is a loop, also set the one after it (the loop can be skipped; loops are never adjacent)
        long match = state & patternMatch[transition];
        return match & isLoop | (match | (match << 1 & isLoop)) << 1;
    }

    @Override
    public boolean descend(int depth, int transition)
    {
        long next = next(states[depth - 1], transition);
        if (depth >= states.length)
            states = Arrays.copyOf(states, states.length * 2);
        states[depth] = next;
        return next != 0;
    }

    @Override
    public void descend(int depth, DirectBuffer bytes, int pos, int count)
    {
        int end = depth + count;
        if (end > states.length)
            states = Arrays.copyOf(states, Math.max(states.length * 2, end));
        long[] states = this.states;
        long state = states[depth - 1];
        for (int i = depth; i < end; ++i)
            states[i] = state = next(state, bytes.getByte(pos++) & 0xFF);
    }

    @Override
    public int flags(int depth)
    {
        long state = states[depth];
        int flags = 0;
        if ((state & matchBit) != 0)
            flags |= MATCH;
        if ((state & isStar) != 0)
        {
            flags |= HAS_ANY_STRING;
            if ((state & isStar & matchBit) != 0)
                flags |= MATCHES_ALL_EXTENSIONS;
        }
        if ((state & isWildcard) != 0)
            flags |= HAS_WILDCARD;
        return flags;
    }

    @Override
    public int singleLiteral(int depth)
    {
        long state = states[depth];
        if ((state & (state - 1)) != 0 || (state & isWildcard) != 0)
            return -1;
        int position = Long.numberOfTrailingZeros(state);
        return position < elements.length ? elements[position] : -1;
    }

    @Override
    public int nextAcceptedByte(int depth, int afterByte, Direction direction)
    {
        long state = states[depth];
        int nextByte = direction.select(256, -1);
        if ((state & isWildcard) == 0)
        {
            // Only literal positions are active (usually just one): the next candidate is the closest of their bytes.
            for (long bits = state; bits != 0; bits &= bits - 1)
            {
                int position = Long.numberOfTrailingZeros(bits);
                if (position >= elements.length)
                    continue; // match position, nothing can follow it
                int b = elements[position];
                if (direction.lt(afterByte, b) && direction.lt(b, nextByte))
                    nextByte = b;
            }
            return nextByte;
        }

        // A byte class is active. Look for the next byte any of the active positions accepts.
        nextByte = afterByte;
        do
        {
            nextByte += direction.increase;
        }
        while (direction.inLoop(nextByte, 0, 255) && (patternMatch[nextByte] & state) == 0);
        return nextByte;
    }

    @Override
    public PatternMatcher tail(int depth)
    {
        return new PatternMatcher64(this, states[depth]);
    }
}
