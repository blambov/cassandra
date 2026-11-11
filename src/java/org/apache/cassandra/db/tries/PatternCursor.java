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

class PatternCursor<T> implements Cursor<T>
{
    // TODO: Use skipTo on source when the state doesn't contain a wildcard position
    // TODO: javadoc/comments


    final Cursor<T> wrapped;
    // The pattern elements, see TriePattern.
    final int[] elements;

    final int matchPosition;
    final boolean hasTrailingStar;

    BitSet[] statesHistory;

    int trailingStarMatchDepth = Integer.MAX_VALUE;
    boolean isMatch;

    public static <T> Cursor<T> create(Cursor<T> wrapped, TriePattern pattern)
    {
        return create(wrapped, pattern, 0);
    }

    public static <T> Cursor<T> create(Cursor<T> wrapped, TriePattern pattern, int algoChoice)
    {
        if (algoChoice == 1)
            return PatternLongCursor.create(wrapped, pattern);
        if (algoChoice != 2 && pattern.length() < 64)
            return Pattern64Cursor.create(wrapped, pattern);

        return new PatternCursor<>(wrapped, pattern.elements);
    }

    private PatternCursor(Cursor<T> wrapped, int[] elements)
    {
        this.wrapped = wrapped;
        this.elements = elements;
        hasTrailingStar = elements.length > 0 && elements[elements.length - 1] == TriePattern.ANY_STRING;
        matchPosition = hasTrailingStar ? elements.length - 1 : elements.length;
        statesHistory = new BitSet[32];
        BitSet state = new BitSet(elements.length);
        state.set(0);
        // Entering a loop position also enters the next one, as the loop can be skipped.
        if (elements.length > 0 && TriePattern.isLoop(elements[0]))
            state.set(1);
        statesHistory[0] = state;
        isMatch = state.get(matchPosition);
    }

    private PatternCursor(PatternCursor<T> source,
                          Cursor<T> wrappedTail)
    {
        this.wrapped = wrappedTail;
        this.elements = source.elements;
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
        // For each position that accepts the byte: set the same position if it is a loop, set the next position, and
        // if that is a loop, also the one after it (the loop can be skipped; loops are never adjacent).

        for (int i = current.previousSetBit(matchPosition); i >= 0; i = current.previousSetBit(i - 1))
        {
            if (i >= elements.length)
                continue; // the match position, nothing follows it
            int element = elements[i];
            if (!TriePattern.accepts(element, b))
                continue;

            next.set(i + 1);
            if (TriePattern.isLoop(element))
            {
                next.set(i);
                // A star accepts everything that any earlier position can, there's no need to process them.
                if (element == TriePattern.ANY_STRING)
                    return;
            }
            else if (i + 1 < elements.length && TriePattern.isLoop(elements[i + 1]))
                next.set(i + 2);
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
