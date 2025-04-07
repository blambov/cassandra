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
import java.util.List;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.TestRangeMarker.fromList;
import static org.apache.cassandra.db.tries.TestRangeMarker.remap;
import static org.apache.cassandra.db.tries.TestRangeMarker.toList;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.assertMapEquals;
import static org.junit.Assert.assertEquals;

public class RangeTrieIntersectionTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    static final int bitsNeeded = 4;
    int bits = bitsNeeded;

    /** Creates a {@link ByteComparable} for the provided value by splitting the integer in sequences of "bits" bits. */
    private ByteComparable of(int value)
    {
        assert value >= 0 && value <= Byte.MAX_VALUE;

        byte[] splitBytes = new byte[(bitsNeeded + bits - 1) / bits];
        int pos = 0;
        int mask = (1 << bits) - 1;
        for (int i = bitsNeeded - bits; i > 0; i -= bits)
            splitBytes[pos++] = (byte) ((value >> i) & mask);

        splitBytes[pos] = (byte) (value & mask);
        return ByteComparable.preencoded(TrieUtil.VERSION, splitBytes);
    }

    private TestRangeMarker from(int where, int value)
    {
        return change(where, -1, value, value);
    }

    private TestRangeMarker to(int where, int value)
    {
        return change(where, value, value, -1);
    }

    private TestRangeMarker point(int where, int value)
    {
        return change(where, -1, value, -1);
    }

    private TestRangeMarker change(int where, int from, int at, int to)
    {
        return new TestRangeMarker(of(where), from, at, to, true);
    }

    private TrieSet range(ByteComparable left, ByteComparable right)
    {
        return TrieSet.range(TrieUtil.VERSION, left, right);
    }

    private TrieSet ranges(ByteComparable... bounds)
    {
        return TrieSet.ranges(TrieUtil.VERSION, bounds);
    }

    @Test
    public void testSubtrie()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            RangeTrie<TestRangeMarker> trie = fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12, 12), to(10, 12)));

            System.out.println(trie.dump());
            assertEquals("No intersection", asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12, 12), to(10, 12)), toList(trie, Direction.FORWARD));

            testIntersection("all",
                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12, 12), to(10, 12)),
                             trie,
                             range(null, null));
            testIntersection("fully covered range",
                             asList(from(1, 10), to(4, 10)),
                             trie,
                             range(of(0), of(5)));
            testIntersection("fully covered range",
                             asList(from(6, 11), change(8, 11, 12, 12), to(10, 12)),
                             trie,
                             range(of(5), of(13)));
            testIntersection("matching range",
                             asList(from(1, 10), to(4, 10)),
                             trie,
                             range(of(1), of(4)));
            testIntersection("touching",
                             asList(point(4, 10), point(6, 11)),
                             trie,
                             range(of(4), of(6)));

            testIntersection("partial left",
                             asList(from(2, 10), to(4, 10)),
                             trie,
                             range(of(2), of(5)));
            testIntersection("partial left on change",
                             asList(from(8, 12), to(10, 12)),
                             trie,
                             range(of(8), of(12)));
            testIntersection("partial left with null",
                             asList(from(9, 12), to(10, 12)),
                             trie,
                             range(of(9), null));


            testIntersection("partial right",
                             asList(from(6, 11), to(7, 11)),
                             trie,
                             range(of(5), of(7)));
            testIntersection("partial right on change",
                             asList(from(6, 11), change(8, 11, 12, -1)),
                             trie,
                             range(of(5), of(8)));
            testIntersection("partial right with null",
                             asList(from(1, 10), to(2, 10)),
                             trie,
                             range(null, of(2)));

            testIntersection("inside range",
                             asList(from(2, 10), to(3, 10)),
                             trie,
                             range(of(2), of(3)));
            testIntersection("inside with change",
                             asList(from(7, 11), change(8, 11, 12, 12), to(9, 12)),
                             trie,
                             range(of(7), of(9)));

            testIntersection("point inside",
                             asList(point(7, 11)),
                             trie,
                             range(of(7), of(7)));
        }
    }

    @Test
    public void testRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            RangeTrie<TestRangeMarker> trie = fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12, 12), to(10, 12)));

            testIntersection("fully covered ranges",
                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12, 12), to(10, 12)),
                             trie,
                             ranges(of(0), of(5), of(5), of(13)));
            testIntersection("matching ranges",
                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12, 12), to(10, 12)),
                             trie,
                             ranges(of(1), of(4), of(6), of(11)));
            testIntersection("touching",
                             asList(point(1, 10), point(4, 10), point(6, 11)),
                             trie,
                             ranges(of(0), of(1), of(4), of(6), of(12), of(15)));
            testIntersection("partial left",
                             asList(from(2, 10), to(4, 10), from(9, 12), to(10, 12)),
                             trie,
                             ranges(of(2), of(5), of(9), null));

            testIntersection("partial right",
                             asList(from(1, 10), to(2, 10), from(6, 11), to(7, 11)),
                             trie,
                             ranges(null, of(2), of(5), of(7)));

            testIntersection("inside ranges",
                             asList(from(2, 10), to(3, 10), from(7, 11), change(8, 11, 12, 12), to(9, 12)),
                             trie,
                             ranges(of(2), of(3), of(7), of(9)));

            testIntersection("jumping inside",
                             asList(from(1, 10), to(2, 10), from(3, 10), to(4, 10), point(6, 11), from(7, 11), change(8, 11, 12, -1), from(9, 12), to(10, 12)),
                             trie,
                             ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10)));
        }
    }

    @Test
    public void testRangeOnSubtrie()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            RangeTrie<TestRangeMarker> trie = fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12, 12), to(10, 12), from(13, 13), to(14, 13)));

            // non-overlapping
            testIntersection("", asList(), trie, range(of(0), of(3)), range(of(4), of(7)));
            // touching
            testIntersection("", asList(point(3, 10)), trie, range(of(0), of(3)), range(of(3), of(7)));
            // overlapping 1
            testIntersection("", asList(from(2, 10), to(3, 10)), trie, range(of(0), of(3)), range(of(2), of(7)));
            // overlapping 2
            testIntersection("", asList(from(1, 10), to(3, 10)), trie, range(of(0), of(3)), range(of(1), of(7)));
            // covered
            testIntersection("", asList(from(1, 10), to(3, 10)), trie, range(of(0), of(3)), range(of(0), of(7)));
            // covered
            testIntersection("", asList(from(3, 10), to(4, 10), from(6, 11), to(7, 11)), trie, range(of(3), of(7)), range(of(0), of(7)));
            // covered 2
            testIntersection("", asList(from(1, 10), to(3, 10)), trie, range(of(1), of(3)), range(of(0), of(7)));
        }
    }

    @Test
    public void testRangesOnRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12, 12), to(10, 12), from(13, 13), to(14, 13))));
    }

    private void testIntersections(RangeTrie<TestRangeMarker> trie)
    {
        System.out.println(trie.dump());
        testIntersection("", asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12, 12), to(10, 12), from(13, 13), to(14, 13)), trie);

        TrieSet set1 = ranges(null, of(4), of(5), of(9), of(12), null);
        TrieSet set2 = ranges(of(2), of(7), of(8), of(10), of(12), of(14));
        TrieSet set3 = ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10));

        testIntersections(trie, set1, set2, set3);

        testSetAlgebraIntersection(trie);
    }

    private void testSetAlgebraIntersection(RangeTrie<TestRangeMarker> trie)
    {
        TrieSet set1 = range(null, of(3))
                              .union(range(of(2), of(4)))
                              .union(range(of(5), of(7)))
                              .union(range(of(7), of(9)))
                              .union(range(of(14), of(16)))
                              .union(range(of(12), null));
        TrieSet set2 = range(of(2), of(7))
                              .union(ranges(null, of(8), of(10), null).weakNegation())
                              .union(ranges(of(8), of(10), of(12), of(14)));
        TrieSet set3 = range(of(1), of(2))
                              .union(range(of(3), of(4)))
                              .union(range(of(5), of(6)))
                              .union(range(of(7), of(8)))
                              .union(range(of(9), of(10)));

        System.out.println("Set 0:\n" + set1.dump());
        System.out.println("Set 1:\n" + set2.dump());
        System.out.println("Set 2:\n" + set3.dump());

        testIntersections(trie, set1, set2, set3);
    }

    private void testIntersections(RangeTrie<TestRangeMarker> trie, TrieSet set1, TrieSet set2, TrieSet set3)
    {
        // set1 = ranges(-4, 5-9, 12-);
        // set2 = ranges(2-7, 8-10, 12-14);
        // set3 = ranges(1-2, 3-4, 5-6, 7-8, 9-10);
        // from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), , 12to(10, 12), from(13, 13), to(14, 13)
        testIntersection("1", asList(from(1, 10), to(4, 10),
                                     from(6, 11), change(8, 11, 12, 12), to(9, 12),
                                     from(13, 13), to(14,13)), trie, set1);

        testIntersection("2", asList(from(2, 10), to(4, 10),
                                     from(6, 11), to(7, 11),
                                     from(8, 12), to(10, 12),
                                     from(13, 13), to(14, 13)), trie, set2);

        testIntersection("3", asList(from(1, 10), to(2, 10),
                                     from(3, 10), to(4, 10),
                                     point(6, 11),
                                     from(7, 11), change(8, 11, 12, -1),
                                     from(9, 12), to(10, 12)), trie, set3);

        testIntersection("12", asList(from(2, 10), to(4, 10),
                                      from(6, 11), to(7, 11),
                                      from(8, 12), to(9, 12),
                                      from(13, 13), to(14, 13)), trie, set1, set2);

        testIntersection("13", asList(from(1, 10), to(2, 10),
                                      from(3, 10), to(4, 10),
                                      point(6, 11),
                                      from(7, 11), change(8, 11, 12, -1),
                                      point(9, 12)), trie, set1, set3);

        testIntersection("23", asList(point(2, 10),
                                      from(3, 10), to(4, 10),
                                      point(6, 11), point(7, 11), point(8, 12),
                                      from(9, 12), to(10, 12)), trie, set2, set3);

        testIntersection("123", asList(point(2, 10),
                                       from(3, 10), to(4, 10),
                                       point(6, 11), point(7, 11),
                                       point(8, 12), point(9, 12)), trie, set1, set2, set3);
    }

    public void testIntersection(String message, List<TestRangeMarker> expected, RangeTrie<TestRangeMarker> trie, TrieSet... sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            try
            {
                assertEquals(message + " forward b" + bits, expected, toList(trie, Direction.FORWARD));
                assertEquals(message + " reverse b" + bits, Lists.reverse(expected), toList(trie, Direction.REVERSE));
            }
            catch (AssertionError e)
            {
                System.out.println("\nFORWARD:\n" + trie.dump(TestRangeMarker::toStringNoPosition));
                System.out.println("\nREVERSE:\n" + trie.cursor(Direction.REVERSE).process(new TrieDumper<>(TestRangeMarker::toStringNoPosition)));
                throw e;
            }
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                TrieSet set = sets[toRemove];
                testIntersection(message + " " + toRemove, expected,
                                 trie.intersect(set),
                                 Arrays.stream(sets)
                                       .filter(x -> x != set)
                                       .toArray(TrieSet[]::new)
                );
            }
        }
    }

    @Test
    public void testRangeMethod() throws TrieSpaceExhaustedException
    {
        RangeTrie<TestRangeMarker> trie = RangeTrie.range(TrieUtil.directComparable("aa"),
                                                          TrieUtil.directComparable("bb"),
                                                          VERSION,
                                                          new TestRangeMarker(ByteComparable.EMPTY, 0, 1, 0, true));
        RangeTrie<TestRangeMarker> expected = directRangeTrie("aa", "bb");
        verifyEqualRangeTries(trie, expected);
    }

    @Test
    public void testIntersectWithCoveredBranch() throws TrieSpaceExhaustedException
    {
        TrieSet set = TrieUtil.directRanges("aaa", "aaq", "abc", "abd", "abfff", "abfff", "cde", "cde");
        RangeTrie<TestRangeMarker> trie = directRangeTrie("ab", "ab");
        RangeTrie<TestRangeMarker> expected = directRangeTrie("abc", "abd", "abfff", "abfff");
        verifyEqualRangeTries(trie.intersect(set), expected);
    }

    @Test
    public void testIntersectWithBranchCoveringSet() throws TrieSpaceExhaustedException
    {
        TrieSet set = TrieSet.singleton(VERSION, TrieUtil.directComparable("abc"));
        RangeTrie<TestRangeMarker> trie = directRangeTrie("aaa", "aba", "abcd", "abce", "abcfff", "abcfff", "bcd", "ccc");
        RangeTrie<TestRangeMarker> expected = directRangeTrie("abcd", "abce", "abcfff", "abcfff");
        verifyEqualRangeTries(trie.intersect(set), expected);
    }

    private static RangeTrie<TestRangeMarker> directRangeTrie(String... keys) throws TrieSpaceExhaustedException
    {
        InMemoryRangeTrie<TestRangeMarker> trie = InMemoryRangeTrie.shortLived(VERSION);
        boolean left = true;
        for (String s : keys)
        {
            trie.putRecursive(TrieUtil.directComparable(s),
                              new TestRangeMarker(TrieUtil.directComparable(s), left ? -1 : 1, 1, left ? 1 : -1, true),
                              (e, n) -> e != null ? e.restrict(n.leftSide >= 0, n.rightSide >= 0, n.isReportableState) : n);
            left = !left;
        }
        return trie;
    }

    private void verifyEqualRangeTries(RangeTrie<TestRangeMarker> trie, RangeTrie<TestRangeMarker> expected)
    {
        assertMapEquals(Iterables.transform(trie.entrySet(Direction.FORWARD),
                                            en -> remap(en)),
                        expected.entrySet(Direction.FORWARD),
                        TrieUtil.FORWARD_COMPARATOR);
        assertMapEquals(Iterables.transform(trie.entrySet(Direction.REVERSE),
                                            en -> remap(en)),
                        expected.entrySet(Direction.REVERSE),
                        TrieUtil.REVERSE_COMPARATOR);
        // do not use the in-memory trie extensions to dump
        assertEquals(expected.process(Direction.FORWARD, new TrieDumper<>(TestRangeMarker::toStringNoPosition)),
                     trie.dump(TestRangeMarker::toStringNoPosition));
    }
}
