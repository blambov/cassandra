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

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.agrona.DirectBuffer;

/// [PatternMatcher] for patterns of any length, whose states are stored in an array of `long`s: bit `i % 64` of word
/// `i / 64` is set if position `i` is active. Shifts carry the top bit of each word into the next one.
///
/// A trailing any-string wildcard is used as the match position, which keeps patterns of exactly 64 positions that end
/// in one within a single word.
final class PatternMatcherLong extends PatternMatcher
{
    final int numLongs;
    // Positions that are not a literal byte
    final long[] isWildcard;
    // Positions that can be repeated (any-string wildcards and UTF-8 continuation loops)
    final long[] isLoop;
    // Positions that are any-string wildcards (* in glob, % in SQL like syntax)
    final long[] isStar;
    // The positions that accept each byte
    final long[][] patternMatch;

    // The match position is bit matchBit of the last word.
    final long matchBit;
    final boolean hasTrailingStar;

    // The state at depth 0
    final long[] rootState;
    // The state at each depth of the current path, and its flags (see PatternMatcher.flags)
    long[][] states;
    int[] stateFlags;

    PatternMatcherLong(TriePattern pattern)
    {
        super(pattern.elements);
        int length = elements.length;
        hasTrailingStar = length > 0 && elements[length - 1] == TriePattern.ANY_STRING;
        if (hasTrailingStar)
            --length;
        numLongs = length / 64 + 1;
        isWildcard = new long[numLongs];
        isLoop = new long[numLongs];
        isStar = new long[numLongs];
        patternMatch = new long[256][];
        for (int i = 0; i < elements.length; ++i)
        {
            int element = elements[i];
            int word = i >>> 6;
            long bit = 1L << (i & 63);
            if (!TriePattern.isLiteral(element))
                isWildcard[word] |= bit;
            if (TriePattern.isLoop(element))
                isLoop[word] |= bit;
            if (element == TriePattern.ANY_STRING)
                isStar[word] |= bit;
            for (int b = 0; b < 256; ++b)
            {
                if (TriePattern.accepts(element, b))
                {
                    if (patternMatch[b] == null)
                        patternMatch[b] = new long[numLongs];
                    patternMatch[b][word] |= bit;
                }
            }
        }
        long[] noMatch = new long[numLongs];
        for (int b = 0; b < 256; ++b)
            if (patternMatch[b] == null)
                patternMatch[b] = noMatch;

        matchBit = 1L << (length % 64);
        states = new long[32][];
        stateFlags = new int[32];
        long[] state = new long[numLongs];
        // Entering a loop position also enters the next one, as the loop can be skipped.
        state[0] = 1 | (isLoop[0] & 1) << 1;
        rootState = state.clone();
        states[0] = state;
        stateFlags[0] = computeFlags(state);
    }

    private PatternMatcherLong(PatternMatcherLong source, long[] rootState)
    {
        super(source.elements);
        numLongs = source.numLongs;
        isWildcard = source.isWildcard;
        isLoop = source.isLoop;
        isStar = source.isStar;
        patternMatch = source.patternMatch;
        matchBit = source.matchBit;
        hasTrailingStar = source.hasTrailingStar;
        this.rootState = rootState.clone();
        states = new long[32][];
        stateFlags = new int[32];
        states[0] = rootState;
        stateFlags[0] = source.computeFlags(rootState);
    }

    @Override
    public int descend(int depth, int transition)
    {
        // positions that accept the byte (wildcards included, all encoded in patternMatch):
        // - set the same bit if the position is a loop
        // - set the next bit
        // - if the next bit is a loop, also set the one after it (the loop can be skipped)
        long[] current = states[depth - 1];
        long[] next = stateAt(depth);
        long[] byteMatch = patternMatch[transition];
        long carry = 0;
        long sum = 0;
        long stars = 0;
        long wildcards = 0;
        for (int i = 0; i < numLongs; ++i)
        {
            long match = current[i] & byteMatch[i];
            long matchShifted = match << 1 | carry;
            long nextIsLoop = matchShifted & isLoop[i]; // matchShifted also has the effect of nextIsLoop, but this is okay because loops are never adjacent
            long state = next[i] = match & isLoop[i] | matchShifted | nextIsLoop << 1;
            sum |= state;
            stars |= state & isStar[i];
            wildcards |= state & isWildcard[i];
            carry = (match | nextIsLoop) >>> 63;
        }
        // Compute the flags while the words are at hand, so that flags() doesn't need to go over them again.
        int flags = stateFlags[depth] = matchFlags(next) | (stars != 0 ? HAS_ANY_STRING : 0) | (wildcards != 0 ? HAS_WILDCARD : 0);
        return sum != 0 ? flags : EMPTY;
    }

    private long[] stateAt(int depth)
    {
        if (depth >= states.length)
        {
            states = Arrays.copyOf(states, states.length * 2);
            stateFlags = Arrays.copyOf(stateFlags, states.length);
        }
        long[] state = states[depth];
        if (state == null)
            states[depth] = state = new long[numLongs];
        return state;
    }

    private int matchFlags(long[] state)
    {
        if ((state[numLongs - 1] & matchBit) == 0)
            return 0;
        return hasTrailingStar ? MATCH | MATCHES_ALL_EXTENSIONS : MATCH;
    }

    private int computeFlags(long[] state)
    {
        return matchFlags(state)
               | (intersects(state, isStar) ? HAS_ANY_STRING : 0)
               | (intersects(state, isWildcard) ? HAS_WILDCARD : 0);
    }

    @Override
    public void descend(int depth, DirectBuffer bytes, int pos, int count)
    {
        for (int i = 0; i < count; ++i)
            descend(depth + i, bytes.getByte(pos + i) & 0xFF);
    }

    @Override
    public int flags(int depth)
    {
        return stateFlags[depth];
    }

    private boolean intersects(long[] state, long[] mask)
    {
        for (int i = 0; i < numLongs; ++i)
            if ((state[i] & mask[i]) != 0)
                return true;
        return false;
    }

    @Override
    public int singleLiteral(int depth)
    {
        long[] state = states[depth];
        int position = -1;
        for (int i = 0; i < numLongs; ++i)
        {
            long bits = state[i];
            if (bits == 0)
                continue;
            if (position >= 0 || (bits & (bits - 1)) != 0 || (bits & isWildcard[i]) != 0)
                return -1;
            position = i * 64 + Long.numberOfTrailingZeros(bits);
        }
        return position >= 0 && position < elements.length ? elements[position] : -1;
    }

    @Override
    public int nextAcceptedByte(int depth, int afterByte, Direction direction)
    {
        long[] state = states[depth];
        int nextByte = direction.select(256, -1);
        if ((stateFlags[depth] & HAS_WILDCARD) == 0)
        {
            // Only literal positions are active (usually just one): the next candidate is the closest of their bytes.
            for (int i = 0; i < numLongs; ++i)
            {
                for (long bits = state[i]; bits != 0; bits &= bits - 1)
                {
                    int position = i * 64 + Long.numberOfTrailingZeros(bits);
                    if (position >= elements.length)
                        continue; // match position, nothing can follow it
                    int b = elements[position];
                    if (direction.lt(afterByte, b) && direction.lt(b, nextByte))
                        nextByte = b;
                }
            }
            return nextByte;
        }

        // A byte class is active. Look for the next byte any of the active positions accepts.
        nextByte = afterByte;
        do
        {
            nextByte += direction.increase;
        }
        while (direction.inLoop(nextByte, 0, 255) && !intersects(patternMatch[nextByte], state));
        return nextByte;
    }

    @Override
    public PatternMatcher tail(int depth)
    {
        return new PatternMatcherLong(this, states[depth].clone());
    }

    @Override
    public boolean matches(ByteBuffer value)
    {
        long[] state = rootState.clone();
        long[] next = new long[numLongs];
        for (int pos = value.position(); pos < value.limit(); ++pos)
        {
            if (hasTrailingStar && (state[numLongs - 1] & matchBit) != 0)
                return true; // a trailing star matches anything that follows
            // as in descend
            long[] byteMatch = patternMatch[value.get(pos) & 0xFF];
            long carry = 0;
            long sum = 0;
            for (int i = 0; i < numLongs; ++i)
            {
                long match = state[i] & byteMatch[i];
                long matchShifted = match << 1 | carry;
                long nextIsLoop = matchShifted & isLoop[i];
                sum |= next[i] = match & isLoop[i] | matchShifted | nextIsLoop << 1;
                carry = (match | nextIsLoop) >>> 63;
            }
            if (sum == 0)
                return false;
            long[] swap = state;
            state = next;
            next = swap;
        }
        return (state[numLongs - 1] & matchBit) != 0;
    }
}
