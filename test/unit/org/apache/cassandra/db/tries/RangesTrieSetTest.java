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
import java.util.stream.Collectors;

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
        check(true, boundariesAsStrings);
    }

    void check(boolean endsInclusive, String... boundariesAsStrings)
    {
        Preencoded[] boundaries = new Preencoded[boundariesAsStrings.length];
        for (int i = 0; i < boundariesAsStrings.length; ++i)
            boundaries[i] = boundariesAsStrings[i] != null ? TrieUtil.directComparable(boundariesAsStrings[i]) : null;

        check(endsInclusive, boundaries);

        verifySkipTo(endsInclusive, boundariesAsStrings, TrieSet.ranges(VERSION, boundaries));
        verifyTails(endsInclusive, boundaries, TrieSet.ranges(VERSION, boundaries));

        verifyNegation(endsInclusive, boundaries, TrieSet.ranges(VERSION, boundaries));
    }

    private static void verifyNegation(boolean endsInclusive, ByteComparable[] boundaries, TrieSet set)
    {
        TrieSet negatedSet = set.negation();
        // If the first entry is not null, drop it; otherwise add a null.
        int addLeft = boundaries.length == 0 || boundaries[0] != null ? +1
                                                                      : -1;
        // If the last entry is not null, drop it; otherwise add a null. If the length is odd, don't adjust anything
        // as the length will now become even.
        int addRight = boundaries.length == 0
                       ? +1
                       : boundaries.length % 2 != 0 ? 0
                                                    : boundaries[boundaries.length - 1] != null ? +1
                                                                                                : -1;

        // Add/remove nulls on both sides of the boundaries
        ByteComparable[] negatedBoundaries = new ByteComparable[boundaries.length + addLeft + addRight];
        for (int i = Math.max(-addLeft, 0); i < boundaries.length + Math.min(addRight, 0); ++i)
            negatedBoundaries[i + addLeft] = boundaries[i];
        System.out.println("Negated boundaries: " + Arrays.stream(negatedBoundaries).map(x -> x != null ? x.byteComparableAsString(VERSION) : null).collect(Collectors.toList()));

        if (endsInclusive)
            for (int i = 1; i < negatedBoundaries.length; ++i)
                if (negatedBoundaries[i] != null && negatedBoundaries[i-1] != null &&
                    ByteComparable.compare(negatedBoundaries[i], negatedBoundaries[i - 1], VERSION) == 0)
                {
                    System.out.println("Skipping negated set check because of repetition");
                    return; // negation cannot be correctly checked when boundaries repeat in endsInclusive mode
                }

        System.out.println("Negated set");
        dumpToOut(negatedSet);
        var expectations = getExpectations(endsInclusive, negatedBoundaries);
        assertTrieEquals(expectations, negatedSet);
    }

    private static TrieSet tailTrie(TrieSet set, ByteComparable prefix, Direction direction)
    {
        TrieSetCursor c = set.cursor(direction);
        if (c.descendAlong(prefix.asComparableBytes(c.byteComparableVersion())))
            return dir -> c.tailCursor(dir);
        else if (c.precedingIncluded())
            return TrieSet.full(c.byteComparableVersion());
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

    private static void verifyTails(boolean endsInclusive, Preencoded[] boundaries, TrieSet set)
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
                var expectations = getExpectations(endsInclusive, tails.toArray(Preencoded[]::new));
                assertTrieEquals(expectations, tail);
            }
        }
    }

    private static void verifySkipTo(boolean endsInclusive, String[] boundariesAsStrings, TrieSet set)
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

    void check(boolean endsInclusive, ByteComparable... boundaries)
    {
        TrieSet s = dir -> RangesCursor.create(dir, VERSION, endsInclusive, boundaries);
        dumpToOut(s);
        var expectations = getExpectations(endsInclusive, boundaries);
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
            } else if (exact && index == firstIndex && ((index & 1) == 0))
                firstExact = true;

            if (index > lastIndex)
            {
                lastIndex = index;
                lastExact = exact && ((index & 1) != 0);
            }
            else if (exact && index == lastIndex && ((index & 1) != 0))
                lastExact = true;
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

    static NavigableMap<Preencoded, PointState> getExpectations(boolean endsInclusive, ByteComparable... boundaries)
    {
        var expectations = new TreeMap<Preencoded, PointState>(FORWARD_COMPARATOR);
        expectations.put(ByteComparable.EMPTY.preencode(VERSION), PointState.coveringInexact(0, boundaries.length));
        int l = (boundaries.length + 1) & ~1;
        for (int bi = 0; bi < l; ++bi)
        {
            ByteComparable b = bi < boundaries.length ? boundaries[bi] : null;
            if (b == null)
                b = ByteComparable.EMPTY;
            int len = ByteComparable.length(b, VERSION);
            for (int i = 0; i <= len; ++i)
            {
                Preencoded v = ByteComparable.cut(b, i).preencode(VERSION);
                PointState state = expectations.computeIfAbsent(v, k -> new PointState());
                state.addIndex(bi, i == len);
            }
        }
        return expectations;
    }

    @Test
    public void testEmptyInterval()
    {
        check(new String[0]);
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
        check("", "");
    }

    @Test
    public void testLeftEmpty()
    {
        check("", null);
    }

    @Test
    public void testRightEmpty()
    {
        check(null, "");
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
