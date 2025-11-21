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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiFunction;

import com.google.common.collect.Maps;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.tries.TrieUtil.FORWARD_COMPARATOR;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.directComparable;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RangesTrieSetTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    static RangeTrie<TrieSetCursor.RangeState> fullTrie(TrieSet s)
    {
        return new RangeTrie<TrieSetCursor.RangeState>()
        {
            @Override
            public RangeCursor<TrieSetCursor.RangeState> makeCursor(Direction direction)
            {
                throw new AssertionError();
            }

            // Override cursor to disable verification which does not like the content this returns.
            // The source is already verified.
            @Override
            public RangeCursor<TrieSetCursor.RangeState> cursor(Direction direction)
            {
                return new RangeCursor<>()
                {
                    private final TrieSetCursor cursor = s.cursor(direction);

                    public TrieSetCursor.RangeState content()
                    {
                        return cursor.state();
                    }

                    @Override
                    public TrieSetCursor.RangeState state()
                    {
                        return cursor.state();
                    }

                    public long encodedPosition()
                    {
                        return cursor.encodedPosition();
                    }

                    @Override
                    public long advance()
                    {
                        return cursor.advance();
                    }

                    @Override
                    public long skipTo(long encodedSkipPosition)
                    {
                        return cursor.skipTo(encodedSkipPosition);
                    }

                    @Override
                    public RangeCursor<TrieSetCursor.RangeState> tailCursor(Direction dir)
                    {
                        throw new AssertionError();
                    }

                    @Override
                    public Direction direction()
                    {
                        return direction;
                    }

                    @Override
                    public ByteComparable.Version byteComparableVersion()
                    {
                        return VERSION;
                    }
                };
            }
        };
    }

    static String dump(TrieSet s, Direction direction)
    {
        return fullTrie(s).process(direction, new TrieDumper.Plain<>(Object::toString));
    }

    static void dumpToOut(TrieSet s)
    {
        System.out.println("Forward:");
        System.out.println(dump(s, Direction.FORWARD));
        System.out.println("Reverse:");
        System.out.println(dump(s, Direction.REVERSE));
    }

    void check(String... boundariesAsStrings)
    {
        Preencoded[] boundaries = new Preencoded[boundariesAsStrings.length];
        for (int i = 0; i < boundariesAsStrings.length; ++i)
            boundaries[i] = boundariesAsStrings[i] != null ? TrieUtil.directComparable(boundariesAsStrings[i]) : null;
        check(boundaries);

        verifySkipTo(boundariesAsStrings, TrieSet.ranges(VERSION, boundaries));
        verifyTails(boundaries, TrieSet.ranges(VERSION, boundaries));
    }

    private static TrieSet tailTrie(TrieSet set, ByteComparable prefix, Direction direction)
    {
        TrieSetCursor c = set.cursor(direction);
        if (c.descendAlong(prefix.asComparableBytes(c.byteComparableVersion())))
            return dir -> c.tailCursor(dir);
        else if (c.precedingIncluded())
            return TrieSet.ranges(c.byteComparableVersion()); // full set
        else
            return null;
    }

    private static boolean startsWith(ByteComparable b, ByteComparable prefix)
    {
        ByteSource sb = b.asComparableBytes(VERSION);
        ByteSource pb = prefix.asComparableBytes(VERSION);
        int next = pb.next();
        while (next != ByteSource.END_OF_STREAM)
        {
            if (sb.next() != next)
                return false;
            next = pb.next();
        }
        return true;
    }

    private static void verifyTails(Preencoded[] boundaries, TrieSet set)
    {
        Set<Preencoded> prefixes = new TreeSet<>(FORWARD_COMPARATOR);
        for (ByteComparable b : boundaries)
        {
            if (b == null)
                continue;
            for (int i = 0; i <= ByteComparable.length(b, VERSION); ++i)
                prefixes.add(ByteComparable.cut(b, i).preencode(VERSION));
        }

        for (ByteComparable prefix : prefixes)
        {
            List<Preencoded> tails = null;
            int prefixLength = ByteComparable.length(prefix, VERSION);
            for (int i = 0; i < boundaries.length; ++i)
            {
                ByteComparable b = boundaries[i];
                if (b == null || !startsWith(b, prefix))
                    continue;
                if (tails == null)
                {
                    tails = new ArrayList<>();
                    if ((i & 1) != 0)
                        tails.add(null);
                }

                final byte[] byteComparableArray = b.asByteComparableArray(VERSION);
                if (prefixLength == byteComparableArray.length)
                    tails.add(null);
                else
                    tails.add(ByteComparable.preencoded(VERSION, Arrays.copyOfRange(byteComparableArray, prefixLength, byteComparableArray.length)));
            }

            for (Direction dir : Direction.values())
            {
                System.out.println("Tail for " + prefix.byteComparableAsString(VERSION) + " " + dir);
                TrieSet tail = tailTrie(set, prefix, dir);
                assertNotNull(tail);
                dumpToOut(tail);
                var expectations = getExpectations(tails.toArray(Preencoded[]::new));
                assertTrieEquals(expectations, tail);
            }
        }
    }

    private static void verifySkipTo(String[] boundariesAsStrings, TrieSet set)
    {
        String arr = Arrays.toString(boundariesAsStrings);
        // Verify that we get the right covering state for all positions around the boundaries.
        for (int si = 0; si < boundariesAsStrings.length; ++si)
        {
            String s = boundariesAsStrings[si];
            if (s == null)
                continue;

            int bi = 0;
            while (bi < boundariesAsStrings.length && (boundariesAsStrings[bi] == null || !boundariesAsStrings[bi].startsWith(s)))
                ++bi;

            int ei = bi;
            ++ei;
            while (ei < boundariesAsStrings.length && boundariesAsStrings[ei] != null && boundariesAsStrings[ei].startsWith(s))
                ++ei;

            for (boolean seekAfterBranch : Arrays.asList(false, true))
                for (Direction direction : Direction.values())
                {
                    String term = seekAfterBranch ? direction.select(">", "<") : "=";
                    String dir = direction == Direction.FORWARD ? "FWD" : "REV";
                    String msg = term + s + " " + dir + " in " + arr + " ";
                    ByteSource.Peekable b = directComparable(s).getPreencodedBytes();
                    TrieSetCursor cursor = set.cursor(direction);
                    // skip to nearest position in cursor
                    int next = b.next();
                    int depth = 0;
                    while (next != ByteSource.END_OF_STREAM)
                    {
                        long skipPosition = Cursor.encode(depth + 1, next, direction);

                        // Adjust to ask for post-branch position on > fwd and < rev
                        if (seekAfterBranch && b.peek() == ByteSource.END_OF_STREAM)
                            skipPosition |= Cursor.ON_RETURN_PATH_BIT;

                        if (Cursor.compare(cursor.skipTo(skipPosition), skipPosition) != 0)
                            break;
                        next = b.next();
                        ++depth;
                    }

                    boolean foundExact = next == ByteSource.END_OF_STREAM;

                    // when seeking forward !sab, we get positioned on bi (regardless if exact)
                    //           before = bi & 1, after = exact ^ before
                    // when seeking forward sab, we get positioned on :
                    //      ei - 1, if it is exact and right bound (ie ei - 1 & 1 ie ~ei & 1)
                    //           before = ei & 1 --> false, after = true
                    //      ei, otherwise, can't be exact
                    //           before = ei & 1, after = before
                    //      for both
                    //           contained if ei & 1
                    // reverse inverts treatment of indexes (exact or not)
                    // when seeking reverse !sab, we get positioned on ei - 1
                    //           contained if ei & 1
                    // when seeking reverse sab, we get positioned on :
                    //      bi, if it is exact and left bound (ie bi & 1 == 0)
                    //           contained if bi & 1 --> not contained
                    //      bi - 1, otherwise
                    //           contained if ~bi & 0

                    boolean before, after, matchesFirst;
                    int seekPos;
                    int statePos;
                    if (!seekAfterBranch)
                    {
                        seekPos = direction.select(bi, ei - 1);
                        matchesFirst = s.equals(boundariesAsStrings[seekPos]) && foundExact;
                        statePos = direction.select(bi, ei);
                        before = (statePos & 1) != 0;
                        after = matchesFirst ^ before;
                    }
                    else
                    {
                        seekPos = direction.select(ei - 1, bi);
                        matchesFirst = s.equals(boundariesAsStrings[seekPos]) && foundExact;
                        statePos = direction.select(ei, bi);
                        after = (statePos & 1) != 0;
                        before = matchesFirst ^ after;
                    }

                    // Check the resulting state.
                    TrieSetCursor.RangeState state = cursor.state();

                    System.out.format("dir %s query %s%s bi %s ei %s seekPos %s matches %s statePos %s before %s after %s state %s foundExact %s effective state %s\n",
                                      direction, s, seekAfterBranch ? "^" : "",
                                      bi, ei, seekPos, matchesFirst, statePos, before, after, state, foundExact, foundExact ? state : state.precedingState(direction));

                    if (!foundExact)
                        state = state.applicableBefore ? TrieSetCursor.RangeState.CONTAINED : TrieSetCursor.RangeState.NOT_CONTAINED;

                    assertEquals(msg + " before", before, state.applicableBefore);
                    assertEquals(msg + " after", after, state.applicableAfter);
                }
        }
    }

    void check(ByteComparable... boundaries)
    {
        TrieSet s = TrieSet.ranges(VERSION, boundaries);
        dumpToOut(s);
        var expectations = getExpectations(boundaries);
        assertTrieEquals(expectations, s);
    }

    private static void assertTrieEquals(NavigableMap<Preencoded, PointState> expectations, TrieSet s)
    {
        BaseTrie<TrieSetCursor.RangeState, ?, ?> trie = fullTrie(s);
        BiFunction<Object, TrieSetCursor.RangeState, Object> combiner =
            (x, y) -> x == null /*|| x == TrieSetCursor.RangeState.NOT_CONTAINED*/ ? y : Pair.create(x, y);
        TrieUtil.assertMapEquals(trie.entrySet(Direction.FORWARD),
                                 Maps.transformValues(expectations, PointState::forwardSide).entrySet(),
                                 FORWARD_COMPARATOR,
                                 combiner);
        TrieUtil.assertMapEquals(trie.entrySet(Direction.REVERSE),
                                 TrieUtil.reorderBy(Maps.transformValues(expectations, PointState::reverseSide),
                                                    TrieUtil.REVERSE_COMPARATOR).entrySet(),
                                 TrieUtil.REVERSE_COMPARATOR,
                                 combiner);
    }

    static class PointState
    {
        int firstIndex = Integer.MAX_VALUE;
        int lastIndex = Integer.MIN_VALUE;
        boolean firstExact = false;
        boolean lastExact = false;

        void addIndex(int index, boolean exact)
        {
            if (index < firstIndex)
            {
                firstIndex = index;
                firstExact = exact && ((index & 1) == 0);
            }
            if (index > lastIndex)
            {
                lastIndex = index;
                lastExact = exact && ((index & 1) != 0);
            }
        }

        static PointState coveringInexact(int from, int to)
        {
            PointState state = new PointState();
            state.firstIndex = from;
            state.lastIndex = to - 1;
            state.firstExact = false;
            state.lastExact = false;
            return state;
        }

        static PointState fullRange()
        {
            PointState state = new PointState();
            state.firstIndex = 1;
            state.lastIndex = 2;
            state.firstExact = false;
            state.lastExact = false;
            return state;
        }

        public static Object forwardSide(PointState pointState)
        {
            boolean applicableBefore = (pointState.firstIndex & 1) == 1;
            RangeState b1 = null;
            RangeState b2 = null;
            // choose to report b1 based on diff between first and last
            if (pointState.firstExact)
                b1 = TrieSetCursor.RangeState.fromProperties(applicableBefore, !applicableBefore);
            else if (pointState.lastIndex > pointState.firstIndex)
                b1 = TrieSetCursor.RangeState.fromProperties(applicableBefore, applicableBefore);

            if (pointState.lastExact)
            {
                boolean applicableAfter = (pointState.lastIndex & 1) == 1;
                b2 = TrieSetCursor.RangeState.fromProperties(applicableAfter, !applicableAfter);
            }

            if (b1 == null && b2 == null)
                return TrieSetCursor.RangeState.fromProperties(applicableBefore, applicableBefore);
            if (b1 != null && b2 != null)
                return Pair.create(b1, b2);
            if (b1 != null)
                return b1;
            return b2;
        }

        public static Object reverseSide(PointState pointState)
        {
            boolean applicableBefore = (pointState.lastIndex & 1) != 1;
            RangeState b1 = null;
            RangeState b2 = null;
            if (pointState.lastExact)
                b1 = TrieSetCursor.RangeState.fromProperties(applicableBefore, !applicableBefore);
            else if (pointState.lastIndex > pointState.firstIndex)
                b1 = TrieSetCursor.RangeState.fromProperties(applicableBefore, applicableBefore);
            if (pointState.firstExact)
            {
                boolean applicableAfter = (pointState.firstIndex & 1) != 1;
                b2 = TrieSetCursor.RangeState.fromProperties(applicableAfter, !applicableAfter);
            }

            if (b1 == null && b2 == null)
                return TrieSetCursor.RangeState.fromProperties(applicableBefore, applicableBefore);
            if (b1 != null && b2 != null)
                return Pair.create(b1, b2);
            if (b1 != null)
                return b1;
            return b2;
        }
    }

    static NavigableMap<Preencoded, PointState> getExpectations(ByteComparable... boundaries)
    {
        var expectations = new TreeMap<Preencoded, PointState>(FORWARD_COMPARATOR);
//        expectations.put(ByteComparable.EMPTY.preencode(VERSION), PointState.coveringInexact(0, boundaries.length));
        for (int bi = 0; bi < boundaries.length; ++bi)
        {
            ByteComparable b = boundaries[bi];
            if (b == null)
                continue;
//                b = ByteComparable.EMPTY;
            int len = ByteComparable.length(b, VERSION);
            for (int i = 0; i <= len; ++i)
            {
                Preencoded v = ByteComparable.cut(b, i).preencode(VERSION);
                PointState state = expectations.computeIfAbsent(v, k -> new PointState());
                state.addIndex(bi, i == len);
            }
        }
        if (expectations.isEmpty())
            expectations.put(ByteComparable.preencoded(VERSION, new byte[0]), PointState.fullRange());
        return expectations;
//        .entrySet()
//                           .stream()
//                           .collect(() -> new TreeMap(FORWARD_COMPARATOR),
//                                    (m, e) -> m.put(e.getKey(), e.getValue().state()),
//                                    NavigableMap::putAll);
    }

    @Test
    public void testFullInterval()
    {
        check(new String[0]);
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

    // prefixes

    @Test
    public void testPrefixLeft()
    {
        check(" a", " abc");
    }

    @Test
    public void testPrefixRight()
    {
        check(" abc", " a");
    }

    @Test
    public void testPrefixHole()
    {
        check(" a", " aaa", " acc", " a");
    }

    @Test
    public void testPrefixLeftHole()
    {
        check(" a", " aaa", " acc", " d");
    }

    @Test
    public void testPrefixRightHole()
    {
        check(" a", " daa", " dcc", " d");
    }


    // Repeats aren't valid, because they doubly list a branch

//    @Test
//    public void testRepeatLeft()
//    {
//        check("abc", "abc", "abc", null);
//    }
//
//    @Test
//    public void testRepeatRight()
//    {
//        check(null, "abc", "abc", "abc");
//    }
//
//    @Test
//    public void testPointRepeat()
//    {
//        check("abc", "abc", "abc", "abc");
//    }
//
//    @Test
//    public void testPointInSpan()
//    {
//        check("aa", "abc", "abc", "ad");
//    }

    @Test
    public void testPrefixRepeatsInSpanOdd()
    {
        check("aaa", "abc", "abe", "aff");
    }

    @Test
    public void testPrefixRepeatsInSpanEven()
    {
        check("abc", "abe", "aff");
    }

    @Test
    public void testBothEmpty()
    {
        check(ByteComparable.EMPTY, ByteComparable.EMPTY);
    }

    @Test
    public void testLeftEmpty()
    {
        check(ByteComparable.EMPTY, null);
    }

    @Test
    public void testRightEmpty()
    {
        check(null, ByteComparable.EMPTY);
    }

    @Test
    public void testLong()
    {
        check("aaa", "aab", "aba", "aca", "acb", "ada", "adba", "adba", "baa", "bba", "bbb", "bbc", "bcc", "bcd");
    }

    @Test
    public void testRangeStateFromProperties()
    {
        for (boolean applicableBefore : List.of(false, true))
            for (boolean applicableAfter : List.of(false, true))
                {
                    TrieSetCursor.RangeState state = TrieSetCursor.RangeState.fromProperties(applicableBefore, applicableAfter);
                    assertEquals(applicableBefore, state.applicableBefore);
                    assertEquals(applicableAfter, state.applicableAfter);
                }
    }
}
