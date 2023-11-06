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
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.asString;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.assertMapEquals;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.generateKeys;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.makeInMemoryTrie;
import static org.junit.Assert.assertEquals;

public class SetIntersectionTrieTest
{
    private static final int COUNT = 15000;
    Random rand = new Random();
    int seed = rand.nextInt();
    final int bitsNeeded = 4;
    int bits = bitsNeeded;


    public static final Trie.CollectionMergeResolver<Integer> RESOLVER = new Trie.CollectionMergeResolver<Integer>()
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
        testIntersectRange(COUNT, Trie::subtrie, false);
    }

    @Test
    public void testIntersectRangesOneDirect() throws Exception
    {
        testIntersectRange(COUNT, (t, l, r) -> t.intersect(Trie.ranges(l, r)), false);
    }

    public void testIntersectRange(int count, RangeOp<ByteBuffer> op, boolean endInclusive) throws Exception
    {
        System.out.format("intersectrange seed %d\n", ++seed);
        rand.setSeed(seed);
        ByteComparable[] src1 = generateKeys(rand, count);
        NavigableMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, Trie.BYTE_COMPARABLE_VERSION));

        InMemoryTrie<ByteBuffer> trie1 = makeInMemoryTrie(src1, content1, true);

        Trie<ByteBuffer> t1 = trie1;

        checkEqualRange(content1, t1, null, null, op, endInclusive);
        checkEqualRange(content1, t1, InMemoryTrieTestBase.generateKey(rand), null, op, endInclusive);
        checkEqualRange(content1, t1, null, InMemoryTrieTestBase.generateKey(rand), op, endInclusive);

        ByteComparable l = rand.nextBoolean() ? InMemoryTrieTestBase.generateKey(rand) : src1[rand.nextInt(src1.length)];
        ByteComparable r = rand.nextBoolean() ? InMemoryTrieTestBase.generateKey(rand) : src1[rand.nextInt(src1.length)];
        int cmp = ByteComparable.compare(l, r, Trie.BYTE_COMPARABLE_VERSION);
        if (cmp > 0)
        {
            ByteComparable t = l;l = r;r = t; // swap
        }

        checkEqualRange(content1, t1, l, r, op, endInclusive);
    }

    public void checkEqualRange(NavigableMap<ByteComparable, ByteBuffer> content1,
                                Trie<ByteBuffer> t1,
                                ByteComparable l,
                                ByteComparable r,
                                RangeOp<ByteBuffer> op,
                                boolean endInclusive) throws Exception
    {
        System.out.format("Intersection with [%s:%s%c\n", asString(l), asString(r), endInclusive ? ']' : ')');
        NavigableMap<ByteComparable, ByteBuffer> imap = l == null
                                                        ? r == null
                                                          ? content1
                                                          : content1.headMap(r, endInclusive)
                                                        : r == null
                                                          ? content1.tailMap(l, true)
                                                          : content1.subMap(l, true, r, endInclusive);

        Trie<ByteBuffer> intersection = op.apply(t1, l, r);

        assertMapEquals(intersection.entrySet(), imap.entrySet());
    }

    /**
     * Extract the values of the provide trie into a list.
     */
    private static <T> List<T> toList(Trie<T> trie)
    {
        return Iterables.toList(trie.values());
    }

    private Trie<Integer> fromList(int... list) throws InMemoryTrie.SpaceExhaustedException
    {
        InMemoryTrie<Integer> trie = new InMemoryTrie<>(BufferType.ON_HEAP);
        for (int i : list)
        {
            trie.putRecursive(of(i), i, (ex, n) -> n);
        }
        return trie;
    }

    /** Creates a {@link ByteComparable} for the provided value by splitting the integer in sequences of "bits" bits. */
    private ByteComparable of(int value)
    {
        assert value >= 0 && value <= Byte.MAX_VALUE;

        byte[] splitBytes = new byte[(bitsNeeded + bits - 1) / bits];
        int pos = 0;
        int mask = (1 << bits) - 1;
        for (int i = bitsNeeded - bits; i > 0; i -= bits)
            splitBytes[pos++] = (byte) ((value >> i) & mask);

        splitBytes[pos++] = (byte) (value & mask);
        return ByteComparable.fixedLength(splitBytes);
    }

    @Test
    public void testSimpleSubtrie() throws InMemoryTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

            testIntersection("", asList(3, 4, 5, 6), trie, Trie.range(of(3), of(7)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6), trie, Trie.range(null, of(7)));

            testIntersection("", asList(3, 4, 5, 6, 7, 8, 9), trie, Trie.range(of(3), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie, Trie.range(null, null));
        }
    }

    @Test
    public void testRangeOnSubtrie() throws InMemoryTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            // non-overlapping
            testIntersection("", asList(), trie, Trie.range(of(0), of(3)), Trie.range(of(4), of(7)));
            // touching, i.e. still non-overlapping
            testIntersection("", asList(), trie, Trie.range(of(0), of(3)), Trie.range(of(3), of(7)));
            // overlapping 1
            testIntersection("", asList(2), trie, Trie.range(of(0), of(3)), Trie.range(of(2), of(7)));
            // overlapping 2
            testIntersection("", asList(1, 2), trie, Trie.range(of(0), of(3)), Trie.range(of(1), of(7)));
            // covered
            testIntersection("", asList(0, 1, 2), trie, Trie.range(of(0), of(3)), Trie.range(of(0), of(7)));
            // covered 2
            testIntersection("", asList(1, 2), trie, Trie.range(of(1), of(3)), Trie.range(of(0), of(7)));
        }
    }

    @Test
    public void testSimpleRanges() throws InMemoryTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

            testIntersection("", asList(3, 4, 5, 6), trie, Trie.ranges(of(3), of(7)));

            testIntersection("", asList(3), trie, Trie.ranges(of(3), of(4)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6), trie, Trie.ranges(null, of(7)));

            testIntersection("", asList(3, 4, 5, 6, 7, 8, 9), trie, Trie.ranges(of(3), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie, Trie.ranges(null, null));

            testIntersection("", asList(3, 4, 5, 7, 8), trie, Trie.ranges(of(3), of(6), of(7), of(9)));

            testIntersection("", asList(3, 7, 8), trie, Trie.ranges(of(3), of(4), of(7), of(9)));

            testIntersection("", asList(3, 7, 8), trie, Trie.ranges(of(3), of(4), of(7), of(9), of(12), of(15)));

            testIntersection("", asList(3, 4, 5, 6, 7, 8), trie, Trie.ranges(of(3), of(9)));

            testIntersection("", asList(3), trie, Trie.ranges(of(3), of(4)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 7, 8), trie, Trie.ranges(null, of(6), of(7), of(9)));

            testIntersection("", asList(3, 4, 5, 7, 8, 9), trie, Trie.ranges(of(3), of(6), of(7), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 7, 8, 9), trie, Trie.ranges(null, of(6), of(7), null));
        }
    }

    @Test
    public void testRangesOnRangesOne() throws InMemoryTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

            // non-overlapping
            testIntersection("non-overlapping", asList(), trie, Trie.ranges(of(0), of(4)), Trie.ranges(of(4), of(8)));
            // touching
            testIntersection("touching", asList(3), trie, Trie.ranges(of(0), of(4)), Trie.ranges(of(3), of(8)));
            // overlapping 1
            testIntersection("overlapping A", asList(2, 3), trie, Trie.ranges(of(0), of(4)), Trie.ranges(of(2), of(8)));
            // overlapping 2
            testIntersection("overlapping B", asList(1, 2, 3), trie, Trie.ranges(of(0), of(4)), Trie.ranges(of(1), of(8)));
            // covered
            testIntersection("covered same end A", asList(0, 1, 2, 3), trie, Trie.ranges(of(0), of(4)), Trie.ranges(of(0), of(8)));
            // covered 2
            testIntersection("covered same end B", asList(4, 5, 6, 7), trie, Trie.ranges(of(4), of(8)), Trie.ranges(of(0), of(8)));
            // covered 3
            testIntersection("covered", asList(1, 2, 3), trie, Trie.ranges(of(1), of(4)), Trie.ranges(of(0), of(8)));
        }
    }

    @Test
    public void testRangesOnRanges() throws InMemoryTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14));
    }

    @Test
    public void testRangesOnMerge() throws InMemoryTrie.SpaceExhaustedException
    {

        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(Trie.merge(ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                                                          fromList(4, 6, 7, 9, 10, 11, 12, 13)),
                                         RESOLVER));
    }

    @Test
    public void testRangesOnCollectionMerge2() throws InMemoryTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(new CollectionMergeTrie<>(
                    ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                                     fromList(4, 6, 7, 9, 10, 11, 12, 13)),
                    RESOLVER));
    }

    @Test
    public void testRangesOnCollectionMerge3() throws InMemoryTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(Trie.merge(
                    ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                                     fromList(4, 6, 9, 10),
                                     fromList(4, 7, 11, 12, 13)),
                    RESOLVER));
    }

    @Test
    public void testRangesOnCollectionMerge10() throws InMemoryTrie.SpaceExhaustedException
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

        Trie<Trie.Contained> set1 = Trie.ranges(null, of(4), of(5), of(9), of(12), null);
        Trie<Trie.Contained> set2 = Trie.ranges(of(2), of(7), of(8), of(10), of(12), of(14));
        Trie<Trie.Contained> set3 = Trie.ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10));

        testIntersection("1", asList(0, 1, 2, 3, 5, 6, 7, 8, 12, 13, 14), trie, set1);

        testIntersection("2", asList(2, 3, 4, 5, 6, 8, 9, 12, 13), trie, set2);

        testIntersection("3", asList(1, 3, 5, 7, 9), trie, set3);

        testIntersection("12", asList(2, 3, 5, 6, 8, 12, 13), trie, set1, set2);

        testIntersection("13", asList(1, 3, 5, 7), trie, set1, set3);

        testIntersection("23", asList(3, 5, 9), trie, set2, set3);

        testIntersection("123", asList(3, 5), trie, set1, set2, set3);
    }

    public void testIntersection(String message, List<Integer> expected, Trie<Integer> trie, Trie<Trie.Contained>... sets)
    {
        testIntersectionTries(message, expected, trie, sets);
        testIntersectionSets(message, expected, trie, Trie.range(null, null), sets);
    }

    public void testIntersectionSets(String message, List<Integer> expected, Trie<Integer> trie, Trie<Trie.Contained> intersectedSet, Trie<Trie.Contained>... sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            assertEquals(message + " sets b" + bits, expected, toList(trie.intersect(intersectedSet)));
            return;
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                Trie<Trie.Contained> set = sets[toRemove];
                testIntersectionSets(message + " " + toRemove, expected,
                                      trie,
                                      Trie.intersectSets(intersectedSet, set),
                                      Arrays.stream(sets)
                                            .filter(x -> x != set)
                                            .toArray(Trie[]::new)
                );
            }
        }
    }

    public void testIntersectionTries(String message, List<Integer> expected, Trie<Integer> trie, Trie<Trie.Contained>... sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            assertEquals(message + " forward b" + bits, expected, toList(trie));
            return;
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                Trie<Trie.Contained> set = sets[toRemove];
                testIntersectionTries(message + " " + toRemove, expected,
                                      trie.intersect(set),
                                      Arrays.stream(sets)
                                                .filter(x -> x != set)
                                                .toArray(Trie[]::new)
                );
            }
        }
    }
}
