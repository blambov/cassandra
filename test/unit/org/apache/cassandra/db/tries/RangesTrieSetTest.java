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
import java.util.NavigableMap;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.tries.TrieSetImpl.RangeState;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.tries.TrieUtil.FORWARD_COMPARATOR;
import static org.apache.cassandra.db.tries.TrieUtil.REVERSE_COMPARATOR;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.assertMapEquals;
import static org.apache.cassandra.db.tries.TrieUtil.assertTrieEquals;
import static org.junit.Assert.assertEquals;

public class RangesTrieSetTest
{
    Trie<RangeState> fullTrie(TrieSet s)
    {
        return (TrieWithImpl<RangeState>) dir -> new TrieImpl.Cursor()
        {
            private final TrieSetImpl.Cursor cursor = TrieSetImpl.impl(s).cursor(dir);

            public RangeState content()
            {
                return cursor.state();
            }

            public int depth()
            {
                return cursor.depth();
            }

            @Override
            public int incomingTransition()
            {
                return cursor.incomingTransition();
            }

            @Override
            public int advance()
            {
                return cursor.advance();
            }

            @Override
            public int skipTo(int skipDepth, int skipTransition)
            {
                return cursor.skipTo(skipDepth, skipTransition);
            }

            @Override
            public TrieSetImpl.Cursor duplicate()
            {
                throw new AssertionError();
            }
        };
    }

    String dump(TrieSet s, Direction direction)
    {
        return TrieImpl.impl(fullTrie(s)).process(new TrieDumper<>(Object::toString), direction);
    }

    void dumpToOut(TrieSet s)
    {
        System.out.println("Forward:");
        System.out.println(dump(s, Direction.FORWARD));
        System.out.println("Reverse:");
        System.out.println(dump(s, Direction.REVERSE));
    }

    void check(String... boundariesAsStrings)
    {
        ByteComparable[] boundaries = new ByteComparable[boundariesAsStrings.length];
        for (int i = 0; i < boundariesAsStrings.length; ++i)
            boundaries[i] = boundariesAsStrings[i] != null ? ByteComparable.of(boundariesAsStrings[i]) : null;
        check(boundaries);

        verifySkipTo(boundariesAsStrings, TrieSet.ranges(boundaries));
    }

    private static void verifySkipTo(String[] boundariesAsStrings, TrieSet set)
    {
        String arr = Arrays.toString(boundariesAsStrings);
        // Verify that we get the right covering state for all positions around the boundaries.
        for (int bi = 0, ei = 0; bi < boundariesAsStrings.length; bi = ei)
        {
            ++ei;
            String s = boundariesAsStrings[bi];
            if (s == null)
                continue;
            while (ei < boundariesAsStrings.length && s.equals(boundariesAsStrings[ei]))
                ++ei;
            for (int terminator : Arrays.asList(ByteSource.LT_NEXT_COMPONENT, ByteSource.TERMINATOR, ByteSource.GT_NEXT_COMPONENT))
                for (Direction direction : Direction.values())
                {
                    String term = terminator == ByteSource.LT_NEXT_COMPONENT ? "<" : terminator == ByteSource.TERMINATOR ? "=" : ">";
                    String dir = direction == Direction.FORWARD ? "FWD" : "REV";
                    String msg = term + s + " " + dir + " in " + arr + " ";
                    ByteSource b = ByteSource.withTerminator(terminator, ByteSource.of(s, VERSION));
                    TrieSetImpl.Cursor cursor = TrieSetImpl.impl(set).cursor(direction);
                    // skip to nearest position in cursor
                    int next = b.next();
                    int depth = 0;
                    while (next != ByteSource.END_OF_STREAM && cursor.skipTo(depth + 1, next) == depth + 1 && cursor.incomingTransition() == next)
                    {
                        next = b.next();
                        ++depth;
                    }
                    // Check the resulting state.
                    int effectiveIndexFwd = terminator <= ByteSource.TERMINATOR ? bi : ei;
                    int effectiveIndexRev = terminator >= ByteSource.TERMINATOR ? ei : bi;
                    boolean isExact = next == ByteSource.END_OF_STREAM;
                    RangeState state = isExact ? cursor.state() : cursor.coveringState();
                    assertEquals(msg + "covering FWD", (effectiveIndexFwd & 1) != 0 ? RangeState.END_START_PREFIX : RangeState.START_END_PREFIX, state.asCoveringState(Direction.FORWARD));
                    assertEquals(msg + "covering REV", (effectiveIndexRev & 1) != 0 ? RangeState.END_START_PREFIX : RangeState.START_END_PREFIX, state.asCoveringState(Direction.REVERSE));
                    // The above also verifies that covering states' applicableBefore and applicableAfter are the same.
                    if (isExact)
                    {
                        Assert.assertNotNull(msg + "content", state.asContent);
                        Assert.assertEquals(msg + "preceding FWD", state.asCoveringState(Direction.FORWARD).applicableBefore, state.precedingIncluded(Direction.FORWARD));
                        Assert.assertEquals(msg + "preceding REV", state.asCoveringState(Direction.REVERSE).applicableAfter, state.precedingIncluded(Direction.REVERSE));
                    }
                }
        }
    }

    void check(ByteComparable... boundaries)
    {
        TrieSet s = TrieSet.ranges(boundaries);
        dumpToOut(s);
        var expectations = getExpectations(boundaries);
        assertTrieEquals(fullTrie(s), expectations);
    }

    static class PointState
    {
        int firstIndex = Integer.MAX_VALUE;
        int lastIndex = Integer.MIN_VALUE;
        boolean exact = false;

        void addIndex(int index, boolean exact)
        {
            firstIndex = Math.min(index, firstIndex);
            lastIndex = Math.max(index, lastIndex);
            this.exact |= exact;
        }

        RangeState state()
        {
            boolean appliesBefore = (firstIndex & 1) != 0;
            boolean appliesAfter = (lastIndex & 1) == 0;
            return RangeState.values()[(appliesBefore ? 1 : 0) | (appliesAfter ? 2 : 0) | (exact ? 4 : 0)];
        }

        static PointState covered()
        {
            PointState state = new PointState();
            state.firstIndex = 1;
            state.lastIndex = 2;
            state.exact = true;
            return state;
        }
    }

    NavigableMap<ByteComparable, RangeState> getExpectations(ByteComparable... boundaries)
    {
        var expectations = new TreeMap<ByteComparable, PointState>(FORWARD_COMPARATOR);
        for (int bi = 0; bi < boundaries.length; ++bi)
        {
            ByteComparable b = boundaries[bi];
            if (b == null)
                continue;
            int len = ByteComparable.length(b, VERSION);
            for (int i = 0; i <= len; ++i)
            {
                ByteComparable v = ByteComparable.cut(b, i);
                PointState state = expectations.computeIfAbsent(v, k -> new PointState());
                state.addIndex(bi, i == len);
            }
        }
        if (expectations.isEmpty())
            expectations.put(ByteComparable.fixedLength(new byte[0]), PointState.covered());
        return expectations.entrySet()
                           .stream()
                           .collect(() -> new TreeMap(FORWARD_COMPARATOR),
                                    (m, e) -> m.put(e.getKey(), e.getValue().state()),
                                    NavigableMap::putAll);
    }

    @Test
    public void testFullInterval()
    {
        check((String) null, null);
    }

    @Test
    public void testOneNull()
    {
        check((String) null);
    }

    @Test
    public void testLeftNull()
    {
        check(null, "afg");
    }

    @Test
    public void testRightNull()
    {
        check("abc", null);
    }

    @Test
    public void testSpan()
    {
        check("abc", "afg");
    }

    @Test
    public void testPoint()
    {
        check("abc", "abc");
    }

    @Test
    public void testDual()
    {
        check("abc", "afg", "aga", "ajb");
    }

    @Test
    public void testHole()
    {
        check(null, "abc", "afg", null);
    }

    @Test
    public void testRepeatLeft()
    {
        check("abc", "abc", "abc", null);
    }

    @Test
    public void testRepeatRight()
    {
        check(null, "abc", "abc", "abc");
    }

    @Test
    public void testPointRepeat()
    {
        check("abc", "abc", "abc", "abc");
    }

    @Test
    public void testPointInSpan()
    {
        check("aa", "abc", "abc", "ad");
    }

    @Test
    public void testLong()
    {
        check("aaa", "aab", "aba", "aca", "acb", "ada", "adba", "adba", "baa", "bba", "bbb", "bbc", "bcc", "bcd");
    }
}
