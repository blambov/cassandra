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
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.TestRangeMarker.remap;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.apache.cassandra.db.tries.TrieUtil.assertMapEquals;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.makeInMemoryTrie;
import static org.apache.cassandra.db.tries.TrieUtil.toBound;
import static org.junit.Assert.assertEquals;

public class IntersectionTrieTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    private static final int COUNT = 15000;
    Random rand = new Random();
    int seed = rand.nextInt();
    final static int bitsNeeded = 4;
    int bits = bitsNeeded;


    public static final Trie.CollectionMergeResolver<Integer> RESOLVER = new Trie.CollectionMergeResolver<>()
    {
        public Integer resolve(Collection<Integer> contents)
        {
            return contents.iterator().next();
        }

        public Integer resolve(Integer b1, Integer b2)
        {
            return b1;
        }
    };

    interface RangeOp<T>
    {
        Trie<T> apply(Trie<T> t, ByteComparable left, ByteComparable right);
    }

    @Test
    public void testIntersectRangeDirect() throws Exception
    {
        testIntersectRange(COUNT, Trie::subtrie);
    }

    @Test
    public void testIntersectRangesOneDirect() throws Exception
    {
        testIntersectRange(COUNT, (t, l, r) -> t.intersect(TrieSet.ranges(VERSION, l, r)));
    }

    public void testIntersectRange(int count, RangeOp<ByteBuffer> op) throws Exception
    {
        System.out.format("intersectrange seed %d\n", ++seed);
        rand.setSeed(seed);
        ByteComparable[] src1 = generateKeys(rand, count);
        NavigableMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));

        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(src1, content1, true);

        Trie<ByteBuffer> t1 = trie1;

        checkEqualRange(content1, t1, null, null, op);
        checkEqualRange(content1, t1, TrieUtil.generateKeyBound(rand), null, op);
        checkEqualRange(content1, t1, null, TrieUtil.generateKeyBound(rand), op);

        ByteComparable l = rand.nextBoolean() ? TrieUtil.generateKeyBound(rand) : toBound(src1[rand.nextInt(src1.length)]);
        ByteComparable r = rand.nextBoolean() ? TrieUtil.generateKeyBound(rand) : toBound(src1[rand.nextInt(src1.length)]);
        int cmp = ByteComparable.compare(l, r, VERSION);
        if (cmp > 0)
        {
            ByteComparable t = l;l = r;r = t; // swap
        }

        checkEqualRange(content1, t1, l, r, op);
    }

    public void checkEqualRange(NavigableMap<ByteComparable, ByteBuffer> content1,
                                Trie<ByteBuffer> t1,
                                ByteComparable l,
                                ByteComparable r,
                                RangeOp<ByteBuffer> op) throws Exception
    {
        System.out.format("Intersection with [%s:%s]\n", asString(l), asString(r));
        NavigableMap<ByteComparable, ByteBuffer> imap = SlicedTrieTest.boundedMap(content1, l, true, r, false);

        Trie<ByteBuffer> intersection = op.apply(t1, l, r);

        assertMapEquals(intersection, imap, Direction.FORWARD);
        assertMapEquals(intersection, imap, Direction.REVERSE);
    }

    /**
     * Extract the values of the provide trie into a list.
     */
    private static <T> List<T> toList(Trie<T> trie, Direction direction)
    {
        return Iterables.toList(trie.values(direction));
    }

    private Trie<Integer> fromList(int... list) throws TrieSpaceExhaustedException
    {
        InMemoryTrie<Integer> trie = InMemoryTrie.shortLived(VERSION);
        for (int i : list)
        {
            trie.putRecursive(at(i), i, (ex, n) -> n);
        }
        return trie;
    }

    /** Creates a {@link ByteComparable} for the provided value by splitting the integer in sequences of "bits" bits. */
    private ByteComparable of(int value, int terminator)
    {
        // TODO: Also in all other tests of this type
        assert value >= 0 && value <= Byte.MAX_VALUE;

        byte[] splitBytes = new byte[(bitsNeeded + bits - 1) / bits + 1];
        int pos = 0;
        int mask = (1 << bits) - 1;
        for (int i = bitsNeeded - bits; i > 0; i -= bits)
            splitBytes[pos++] = (byte) ((value >> i) & mask);

        splitBytes[pos++] = (byte) (value & mask);
        splitBytes[pos++] = (byte) terminator;
        return ByteComparable.preencoded(VERSION, splitBytes);
    }

    private ByteComparable at(int value)
    {
        return of(value, ByteSource.TERMINATOR);
    }

    private ByteComparable before(int value)
    {
        return of(value, ByteSource.LT_NEXT_COMPONENT);
    }

    @Test
    public void testSimpleSubtrie() throws TrieSpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

            testIntersection("", asList(3, 4, 5, 6), trie,
                             TrieSet.range(VERSION, before(3), before(7)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6), trie,
                             TrieSet.range(VERSION, null, before(7)));

            testIntersection("", asList(3, 4, 5, 6, 7, 8, 9), trie,
                             TrieSet.range(VERSION, before(3), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie,
                             TrieSet.range(VERSION, null, null));

            testIntersection("", asList(), trie,
                             TrieSet.range(VERSION, before(7), before(7)));
        }
    }

    @Test
    public void testRangeOnSubtrie() throws TrieSpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            // non-overlapping
            testIntersection("", asList(), trie,
                             TrieSet.range(VERSION, before(0), before(3)),
                             TrieSet.range(VERSION, before(4), before(7)));
            // touching, i.e. still non-overlapping
            testIntersection("", asList(), trie,
                             TrieSet.range(VERSION, before(0), before(3)),
                             TrieSet.range(VERSION, before(3), before(7)));
            // overlapping 1
            testIntersection("", asList(2), trie,
                             TrieSet.range(VERSION, before(0), before(3)),
                             TrieSet.range(VERSION, before(2), before(7)));
            // overlapping 2
            testIntersection("", asList(1, 2), trie,
                             TrieSet.range(VERSION, before(0), before(3)),
                             TrieSet.range(VERSION, before(1), before(7)));
            // covered
            testIntersection("", asList(0, 1, 2), trie,
                             TrieSet.range(VERSION, before(0), before(3)),
                             TrieSet.range(VERSION, before(0), before(7)));
            // covered 2
            testIntersection("", asList(1, 2), trie,
                             TrieSet.range(VERSION, before(1), before(3)),
                             TrieSet.range(VERSION, before(0), before(7)));
        }
    }

    @Test
    public void testSimpleRanges() throws TrieSpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

            testIntersection("", asList(3, 4, 5, 6), trie,
                             TrieSet.ranges(VERSION, before(3), before(7)));

            testIntersection("", asList(3), trie,
                             TrieSet.ranges(VERSION, before(3), before(4)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6), trie,
                             TrieSet.ranges(VERSION, null, before(7)));

            testIntersection("", asList(3, 4, 5, 6, 7, 8, 9), trie,
                             TrieSet.ranges(VERSION, before(3), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie,
                             TrieSet.ranges(VERSION, null, null));

            testIntersection("", asList(3, 4, 5, 7, 8), trie,
                             TrieSet.ranges(VERSION, before(3), before(6), before(7), before(9)));

            testIntersection("", asList(3, 7, 8), trie,
                             TrieSet.ranges(VERSION, before(3), before(4), before(7), before(9)));

            testIntersection("", asList(3, 7, 8), trie,
                             TrieSet.ranges(VERSION, before(3), before(4), before(7), before(9), before(12), before(15)));

            testIntersection("", asList(3, 4, 5, 6, 7, 8), trie,
                             TrieSet.ranges(VERSION, before(3), before(9)));

            testIntersection("", asList(3), trie,
                             TrieSet.ranges(VERSION, before(3), before(4)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 7, 8), trie,
                             TrieSet.ranges(VERSION, null, before(6), before(7), before(9)));

            testIntersection("", asList(3, 4, 5, 7, 8, 9), trie,
                             TrieSet.ranges(VERSION, before(3), before(6), before(7), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 7, 8, 9), trie,
                             TrieSet.ranges(VERSION, null, before(6), before(7), null));

            testIntersection("", asList(3, 4, 5, 6, 7, 8), trie,
                             TrieSet.ranges(VERSION, before(3), before(6), before(6), before(9)));

            testIntersection("", asList(3, 4, 5, 7, 8), trie,
                             TrieSet.ranges(VERSION, before(3), before(6), before(6), before(6), before(7), before(9)));
        }
    }

    @Test
    public void testRangesOnRangesOne() throws TrieSpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

            // non-overlapping
            testIntersection("non-overlapping", asList(), trie,
                             TrieSet.ranges(VERSION, before(0), before(4)),
                             TrieSet.ranges(VERSION, before(4), before(8)));
            // touching
            testIntersection("touching", asList(3), trie,
                             TrieSet.ranges(VERSION, before(0), before(4)),
                             TrieSet.ranges(VERSION, before(3), before(8)));
            // overlapping 1
            testIntersection("overlapping A", asList(2, 3), trie,
                             TrieSet.ranges(VERSION, before(0), before(4)),
                             TrieSet.ranges(VERSION, before(2), before(8)));
            // overlapping 2
            testIntersection("overlapping B", asList(1, 2, 3), trie,
                             TrieSet.ranges(VERSION, before(0), before(4)),
                             TrieSet.ranges(VERSION, before(1), before(8)));
            // covered
            testIntersection("covered same end A", asList(0, 1, 2, 3), trie,
                             TrieSet.ranges(VERSION, before(0), before(4)),
                             TrieSet.ranges(VERSION, before(0), before(8)));
            // covered 2
            testIntersection("covered same end B", asList(4, 5, 6, 7), trie,
                             TrieSet.ranges(VERSION, before(4), before(8)),
                             TrieSet.ranges(VERSION, before(0), before(8)));
            // covered 3
            testIntersection("covered", asList(1, 2, 3), trie,
                             TrieSet.ranges(VERSION, before(1), before(4)),
                             TrieSet.ranges(VERSION, before(0), before(8)));
        }
    }

    @Test
    public void testRangesOnRanges() throws TrieSpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14));
    }

    @Test
    public void testRangesOnMerge() throws TrieSpaceExhaustedException
    {

        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(Trie.merge(ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                                                          fromList(4, 6, 7, 9, 10, 11, 12, 13)),
                                         RESOLVER));
    }

    @Test
    public void testRangesOnCollectionMerge2() throws TrieSpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            List<Trie<Integer>> inputs = ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                                                          fromList(4, 6, 7, 9, 10, 11, 12, 13));
            testIntersections(dir -> new CollectionMergeCursor.Plain<>(RESOLVER, dir, inputs, Trie::cursor));
        }
    }

    @Test
    public void testRangesOnCollectionMerge3() throws TrieSpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(Trie.merge(
                    ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                                     fromList(4, 6, 9, 10),
                                     fromList(4, 7, 11, 12, 13)),
                    RESOLVER));
    }

    @Test
    public void testRangesOnCollectionMerge10() throws TrieSpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(Trie.merge(
                    ImmutableList.of(fromList(0, 14),
                                     fromList(1, 2),
                                     fromList(2, 13),
                                     fromList(3),
                                     fromList(4, 7),
                                     fromList(5, 9, 12),
                                     fromList(6, 8),
                                     fromList(7),
                                     fromList(8),
                                     fromList(10, 11)),
                    RESOLVER));
    }

    private void testIntersections(Trie<Integer> trie)
    {
        testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14), trie);

        TrieSet set1 = TrieSet.ranges(VERSION, null, before(4), before(5), before(9), before(12), null);
        TrieSet set2 = TrieSet.ranges(VERSION, before(2), before(7), before(8), before(10), before(12), before(14));
        TrieSet set3 = TrieSet.ranges(VERSION, before(1), before(2), before(3), before(4), before(5), before(6), before(7), before(8), before(9), before(10));

        testIntersections(trie, set1, set2, set3);

        testSetAlgebraIntersection(trie);
    }

    private void testSetAlgebraIntersection(Trie<Integer> trie)
    {
        TrieSet set1 = TrieSet.range(VERSION, null, before(3))
                              .union(TrieSet.range(VERSION, before(2), before(4)))
                              .union(TrieSet.range(VERSION, before(5), before(7)))
                              .union(TrieSet.range(VERSION, before(7), before(9)))
                              .union(TrieSet.range(VERSION, before(14), before(16)))
                              .union(TrieSet.range(VERSION, before(12), null));
        TrieSet set2 = TrieSet.range(VERSION, before(2), before(7))
                              .union(TrieSet.ranges(VERSION, null, before(8), before(10), null).weakNegation())
                              .union(TrieSet.ranges(VERSION, before(8), before(10), before(12), before(14)));
        TrieSet set3 = TrieSet.range(VERSION, before(1), before(2))
                              .union(TrieSet.range(VERSION, before(3), before(4)))
                              .union(TrieSet.range(VERSION, before(5), before(6)))
                              .union(TrieSet.range(VERSION, before(7), before(8)))
                              .union(TrieSet.range(VERSION, before(9), before(10)));

        testIntersections(trie, set1, set2, set3);
    }

    private void testIntersections(Trie<Integer> trie, TrieSet set1, TrieSet set2, TrieSet set3)
    {
        testIntersection("1", asList(0, 1, 2, 3, 5, 6, 7, 8, 12, 13, 14), trie, set1);

        testIntersection("2", asList(2, 3, 4, 5, 6, 8, 9, 12, 13), trie, set2);

        testIntersection("3", asList(1, 3, 5, 7, 9), trie, set3);

        testIntersection("12", asList(2, 3, 5, 6, 8, 12, 13), trie, set1, set2);

        testIntersection("13", asList(1, 3, 5, 7), trie, set1, set3);

        testIntersection("23", asList(3, 5, 9), trie, set2, set3);

        testIntersection("123", asList(3, 5), trie, set1, set2, set3);
    }

    public void testIntersection(String message, List<Integer> expected, Trie<Integer> trie, TrieSet... sets)
    {
        testIntersectionTries(message, expected, trie, sets);
        testIntersectionSets(message + " setix", expected, trie, TrieSet.range(VERSION, null, null), sets);
    }

    public void checkEqual(String message, List<Integer> expected, Trie<Integer> trie)
    {
        assertEquals(message + " forward", expected, toList(trie, Direction.FORWARD));
        assertEquals(message + " reverse", expected.stream()
                                                   .sorted(Comparator.<Integer>naturalOrder().reversed())
                                                   .collect(Collectors.toList()),
                     toList(trie, Direction.REVERSE));
    }

    public void testIntersectionSets(String message, List<Integer> expected, Trie<Integer> trie, TrieSet intersectedSet, TrieSet[] sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            checkEqual(message + " b" + bits, expected, trie.intersect(intersectedSet));
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                TrieSet set = sets[toRemove];
                testIntersectionSets(message + " " + toRemove, expected,
                                     trie,
                                     intersectedSet.intersection(set),
                                     Arrays.stream(sets)
                                           .filter(x -> x != set)
                                           .toArray(TrieSet[]::new)
                );
            }
        }
    }

    public void testIntersectionTries(String message, List<Integer> expected, Trie<Integer> trie, TrieSet[] sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            checkEqual(message + " b" + bits, expected, trie);
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                TrieSet set = sets[toRemove];
                testIntersectionTries(message + " " + toRemove, expected,
                                      trie.intersect(set),
                                      Arrays.stream(sets)
                                                .filter(x -> x != set)
                                                .toArray(TrieSet[]::new)
                );
            }
        }
    }

    private static InMemoryTrie<Integer> duplicateTrie(Trie<Integer> trie)
    {
        try
        {
            InMemoryTrie<Integer> dupe = InMemoryTrie.shortLived(VERSION);
            dupe.apply(trie, (x, y) -> y, Predicates.alwaysFalse());
            return dupe;
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    @Test
    public void testReturnsContentOnPrefix() throws TrieSpaceExhaustedException
    {
        TrieSet set = TrieSet.singleton(VERSION, TrieUtil.directComparable("abc"));
        Trie<String> trie = TrieUtil.directTrie("a", "aa", "ab", "abc", "cd");
        Trie<String> expected = TrieUtil.directTrie("a", "ab", "abc");
        assertMapEquals(expected.entrySet(Direction.FORWARD), trie.intersect(set).entrySet(Direction.FORWARD), TrieUtil.FORWARD_COMPARATOR);
        assertMapEquals(expected.entrySet(Direction.REVERSE), trie.intersect(set).entrySet(Direction.REVERSE), TrieUtil.REVERSE_COMPARATOR);
        assertEquals(expected.process(Direction.FORWARD, new TrieDumper<>(Object::toString)), trie.intersect(set).dump());
    }

    @Test
    public void testReturnsBranchContents() throws TrieSpaceExhaustedException
    {
        TrieSet set = TrieSet.singleton(VERSION, TrieUtil.directComparable("abc"));
        Trie<String> trie = TrieUtil.directTrie("aaa", "abc", "abce", "abcfff", "bcd");
        Trie<String> expected = TrieUtil.directTrie("abc", "abce", "abcfff");
        assertMapEquals(expected.entrySet(Direction.FORWARD), trie.intersect(set).entrySet(Direction.FORWARD), TrieUtil.FORWARD_COMPARATOR);
        assertMapEquals(expected.entrySet(Direction.REVERSE), trie.intersect(set).entrySet(Direction.REVERSE), TrieUtil.REVERSE_COMPARATOR);
        assertEquals(expected.process(Direction.FORWARD, new TrieDumper<>(Object::toString)), trie.intersect(set).dump());
    }

    @Test
    public void testSkipToWhenSetIsAhead()
    {
        String[] ranges1 = {"aaa", "ddd"};
        String[] ranges2 = {"bbb", "eee"};
        String[] ixranges = {"bbb", "ddd"};
        String[] points = {"___", "aaa", "bbb", "ccc", "ddd", "eee"};
        testIntersectionSkipTo(ranges1, ranges2, ixranges, points);
    }

    @Test
    public void testSkipToWhenSetIsCovering()
    {
        String[] ranges1 = {"a", "a", "bbb", "bdd"};
        String[] ranges2 = {"aaa", "acc", "b", "b"};
        String[] ixranges = {"aaa", "acc", "bbb", "bdd"};
        String[] points = {"___", "a__", "aaa", "abb", "acc", "add", "baa", "bbb", "bcc", "bdd", "bee", "eee"};
        testIntersectionSkipTo(ranges1, ranges2, ixranges, points);
    }

    @Test
    public void testRangeUnderCoveredBranchPoint()
    {
        String[] ranges1 = {"bb", "bb"};
        String[] ranges2 = {"aa", "ab", "bbc", "bbd", "bbfff", "bbfff", "bce", "bcf", "ce", "cf"};
        String[] expected2 = {"bbc", "bbd", "bbfff", "bbfff"};
        String[] ranges3 = {"bbff", "bbff"};
        String[] expected3 = {"bbfff", "bbfff"};
        testDirectIntersections(ranges1, ranges2, expected2, ranges3, expected3);
    }

    @Test
    public void testRangeUnderCoveredRoot()
    {
        String[] ranges1 = {"", ""};
        String[] ranges2 = {"aa", "ab", "bc", "be", "bfff", "bfff", "ce", "cf"};
        String[] expected2 = ranges2;
        String[] ranges3 = {"bdff", "bdff", "cdff", "cdff"};
        String[] expected3 = {"bdff", "bdff"};
        testDirectIntersections(ranges1, ranges2, expected2, ranges3, expected3);
    }

    @Test
    public void testRangeUnderCoveredTwice()
    {
        String[] ranges1 = {"", ""};
        String[] ranges2 = {"aa", "ab", "bc", "be", "bfff", "bfff", "ce", "cf"};
        String[] expected2 = ranges2;
        String[] ranges3 = {"bcff", "bcff", "beff", "beff"};
        String[] expected3 = ranges3;
        testDirectIntersections(ranges1, ranges2, expected2, ranges3, expected3);
    }

    @Test
    public void testRangeUnderCoveredBranchRight()
    {
        String[] ranges1 = {"_", "_", "abba", "abe", "d", "d"};
        String[] ranges2 = {"aaa", "aab", "abc", "abd", "abef", "abeg", "abehhh", "abehhh", "ce", "cf"};
        String[] expected2 = {"abc", "abd", "abef", "abeg", "abehhh", "abehhh"};
        String[] ranges3 = {"abehh", "abehh"};
        String[] expected3 = {"abehhh", "abehhh"};
        testDirectIntersections(ranges1, ranges2, expected2, ranges3, expected3);
    }

    @Test
    public void testRangeUnderCoveredBranchLeft()
    {
        String[] ranges1 = {"_", "_", "abba", "abe", "d", "d"};
        String[] ranges2 = {"aaa", "aab", "abbac", "abbad", "abbafff", "abbafff", "abc", "abd", "ce", "cf"};
        String[] expected2 = {"abbac", "abbad", "abbafff", "abbafff", "abc", "abd"};
        String[] ranges3 = {"abbaff", "abbaff"};
        String[] expected3 = {"abbafff", "abbafff"};
        testDirectIntersections(ranges1, ranges2, expected2, ranges3, expected3);
    }

    @Test
    public void testLeftsAtDifferentDepth()
    {
        String[] ranges1 = {"ab", "ace"};
        String[] ranges2 = {"abba", "ae"};
        String[] expected2 = {"abba", "ace"};
        String[] ranges3 = {"abbaff", "abbaff"};
        String[] expected3 = {"abbaff", "abbaff"};
        testDirectIntersections(ranges1, ranges2, expected2, ranges3, expected3);
    }

    @Test
    public void testRightsAtDifferentDepth()
    {
        String[] ranges1 = {"ab", "affe"};
        String[] ranges2 = {"acd", "af"};
        String[] expected2 = {"acd", "affe"};
        String[] ranges3 = {"addaff", "addaff"};
        String[] expected3 = {"addaff", "addaff"};
        testDirectIntersections(ranges1, ranges2, expected2, ranges3, expected3);
    }

    private void testDirectIntersections(String[] ranges1, String[] ranges2, String[] expected2, String[] ranges3, String[] expected3)
    {
        TrieSet set1 = TrieUtil.directRanges(ranges1);
        TrieSet set2 = TrieUtil.directRanges(ranges2);
        TrieSet expected = TrieUtil.directRanges(expected2);
        verifyEqualTrieSets(set1.intersection(set2), expected);
        String[] allpoints = Arrays.asList(ranges1, ranges2, expected2, ranges3, expected3)
                                   .stream()
                                   .flatMap(Arrays::stream)
                                   .distinct()
                                   .toArray(String[]::new);
        verifyIntersectionContainsCorrectness(allpoints, set1, set2);
        // check skipTo in a covered branch
        TrieSet set3 = TrieUtil.directRanges(ranges3);
        expected = TrieUtil.directRanges(expected3);
        verifyEqualTrieSets(set1.intersection(set2).intersection(set3), expected);
        verifyEqualTrieSets(set3.intersection(set2).intersection(set1), expected);
        verifyIntersectionContainsCorrectness(allpoints, set1.intersection(set2), set3);
        verifyIntersectionContainsCorrectness(allpoints, set3.intersection(set2), set1);
    }

    private void verifyEqualTrieSets(TrieSet set, TrieSet expectedSet)
    {
        Trie<TrieSetCursor.RangeState> trie = dir -> set.cursor(dir);
        Trie<TrieSetCursor.RangeState> expected = dir -> expectedSet.cursor(dir);
        assertMapEquals(trie.entrySet(Direction.FORWARD),
                        expected.entrySet(Direction.FORWARD),
                        TrieUtil.FORWARD_COMPARATOR);
        assertMapEquals(trie.entrySet(Direction.REVERSE),
                        expected.entrySet(Direction.REVERSE),
                        TrieUtil.REVERSE_COMPARATOR);
    }

    private void testIntersectionSkipTo(String[] ranges1, String[] ranges2, String[] ixranges, String[] points)
    {
        TrieSet set1 = TrieUtil.directRanges(ranges1);
        TrieSet set2 = TrieUtil.directRanges(ranges2);
        TrieSet ix = TrieUtil.directRanges(ixranges);
        verifyEqualTrieSets(set1.intersection(set2), ix);

        verifyIntersectionContainsCorrectness(points, set1, set2);

        for (int i = 1; i < 1 << points.length; i++) // at least one set bit
        {
            String[] ranges = new String[Integer.bitCount(i) * 2];
            int p = 0;
            for (int j = 0; j < points.length; j++)
            {
                if ((i & (1 << j)) != 0)
                {
                    ranges[p++] = points[j];
                    ranges[p++] = points[j];
                }
            }
            System.out.println(Arrays.toString(ranges));
            TrieSet set3 = TrieUtil.directRanges(ranges);
            TrieSet expected = TrieUtil.directRanges(Arrays.stream(ranges).filter(x -> ix.strictlyContains(TrieUtil.directComparable(x))).toArray(String[]::new));
            verifyEqualTrieSets(set1.intersection(set2).intersection(set3), expected);
            verifyEqualTrieSets(set3.intersection(set2).intersection(set1), expected);
        }
    }

    private static void verifyIntersectionContainsCorrectness(String[] points, TrieSet set1, TrieSet set2)
    {
        TrieSet ix = set1.intersection(set2);
        for (String s : points)
        {
            ByteComparable bc = TrieUtil.directComparable(s);
            assertEquals(s, set1.strictlyContains(bc) && set2.strictlyContains(bc), ix.strictlyContains(bc));
        }
    }
}
