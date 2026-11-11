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
    // Positions that are not a literal byte
    final long isWildcard;
    // Positions that can be repeated (any-string wildcards and UTF-8 continuation loops)
    final long isLoop;
    // Positions that are any-string wildcards (* in glob, % in SQL like syntax)
    final long isStar;
    // Which positions are a match for the given byte (taking wildcards into account)
    final long[] patternMatch;
    // pattern elements (for skipping to the next literal byte)
    final int[] elements;

    final long matchBit;

    long[] statesHistory;
    long currentState;
    boolean isMatch;

    TransitionsReceiver chainedReceiver;
    int curDepth;
    int trailingStarMatchDepth = Integer.MAX_VALUE;

    public static <T> Pattern64Cursor<T> create(Cursor<T> wrapped, TriePattern pattern)
    {
        int[] elements = pattern.elements;
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
        return new Pattern64Cursor<>(wrapped, elements, isStar, isLoop, isWildcard, patternMatch);
    }

    private Pattern64Cursor(Cursor<T> wrapped,
                            int[] elements,
                            long isStar,
                            long isLoop,
                            long isWildcard,
                            long[] patternMatch)
    {
        this.wrapped = wrapped;
        this.isWildcard = isWildcard;
        this.isLoop = isLoop;
        this.isStar = isStar;
        this.patternMatch = patternMatch;
        this.elements = elements;
        long matchBit = 1L << elements.length;
        if ((isStar & matchBit >>> 1) != 0)
            matchBit >>>= 1; // we can end early on trailing stars
        this.matchBit = matchBit;
        statesHistory = new long[32];
        // Entering a loop position also enters the next one, as the loop can be skipped.
        long state = 1 | (isLoop & 1) << 1;
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
        this.isLoop = source.isLoop;
        this.isWildcard = source.isWildcard;
        this.patternMatch = source.patternMatch;
        this.elements = source.elements;
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

            // We won't end up here very often (only if there's no star in the path so far). If we do, skip to the next
            // byte that the pattern can accept.
            encodedPosition = wrapped.skipTo(nextPatternPosition(encodedPosition, prev, depth));
        }
    }

    private long nextPatternPosition(long encodedPosition, long prev, int depth)
    {
        Direction direction = direction();
        int currentByte = Cursor.incomingTransition(encodedPosition);
        int nextByte = direction.select(256, -1);
        if ((prev & isWildcard) == 0)
        {
            // Only literal positions are active (usually just one): the next candidate is the closest of their bytes.
            for (long bits = prev; bits != 0; bits &= bits - 1)
            {
                int patternPos = Long.numberOfTrailingZeros(bits);
                if (patternPos >= elements.length)
                    continue; // match position, nothing can follow it
                int b = elements[patternPos];
                if (direction.lt(currentByte, b) && direction.lt(b, nextByte))
                    nextByte = b;
            }
        }
        else
        {
            // A byte class of a single-character wildcard is active (any-byte and any-string wildcards accept
            // everything, so they can't be). Look for the next byte any of the active positions accepts.
            nextByte = currentByte;
            do
            {
                nextByte += direction.increase;
            }
            while (direction.inLoop(nextByte, 0, 255) && (patternMatch[nextByte] & prev) == 0);
        }

        return Cursor.encodeWithOverflow(depth, nextByte, direction);
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
        // if we have never seen a star, and the current position is not a wildcard, use the next pattern byte
        if ((currentState & isWildcard) == 0)
            return advanceToPatternByte();
        else
            return advanceTo(wrapped.advance());
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        // if we have never seen a star, and the current position is not a wildcard, use the next pattern byte
        if ((currentState & isWildcard) == 0)
            return advanceToPatternByte();

        // Only fixed-length wildcards (any byte or character) in the state, no star yet: advance one step, so that we
        // go back to skipping by pattern byte as soon as the next pattern position is a literal.
        if ((currentState & isStar) == 0)
            return advanceTo(wrapped.advance());

        // we don't need to track anything after a trailing star, only check if we have left the branch
        if ((currentState & isStar & matchBit) != 0)
            return advanceTo(wrapped.advanceMultiple(receiver));

        // If there is a star in our state, we have to walk the full trie, so take advantage of advanceMultiple,
        // updating our state for each character.
        chainedReceiver = receiver;
        return advanceTo(wrapped.advanceMultiple(this));
    }

    private long advanceToPatternByte()
    {
        int patternPos = Long.numberOfTrailingZeros(currentState);
        // Don't descend if the pattern is fully matched, if more than one position is active (possible after a
        // single-character wildcard), or if we are on the return path (in reverse direction ordered tries present
        // prefix content after the node's children, which have already been visited).
        if (patternPos >= elements.length
            || (currentState & (currentState - 1)) != 0
            || Cursor.isOnReturnPath(wrapped.encodedPosition()))
            return advanceTo(wrapped.advance());
        Direction direction = direction();
        int nextByte = elements[patternPos];
        long skipPosition = Cursor.encode(++curDepth, nextByte, direction);
        return advanceTo(wrapped.skipTo(skipPosition));
    }

    @Override
    public long skipTo(long skipPosition)
    {
        return advanceTo(wrapped.skipTo(skipPosition));
    }

    private long setNextState(int b, long current)
    {
        // positions that accept the byte (wildcards included, all encoded in patternMatch):
        // - set the same bit if the position is a loop
        // - set the next bit
        // - if the next bit is a loop, also set the one after it (the loop can be skipped; loops are never adjacent)
        long match = current & patternMatch[b];
        return match & isLoop | (match | (match << 1 & isLoop)) << 1;
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
