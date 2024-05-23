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
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.makeInMemoryDTrie;
import static org.junit.Assert.assertEquals;

public class IntersectionTrieTest
{
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
        testIntersectRange(COUNT, (t, l, r) -> t.intersect(TrieSet.ranges(l, r)));
    }

    public void testIntersectRange(int count, RangeOp<ByteBuffer> op) throws Exception
    {
        System.out.format("intersectrange seed %d\n", ++seed);
        rand.setSeed(seed);
        ByteComparable[] src1 = generateKeys(rand, count);
        NavigableMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, TrieImpl.BYTE_COMPARABLE_VERSION));

        InMemoryDTrie<ByteBuffer> trie1 = makeInMemoryDTrie(src1, content1, true);

        Trie<ByteBuffer> t1 = trie1;

        checkEqualRange(content1, t1, null, null, op);
        checkEqualRange(content1, t1, InMemoryTrieTestBase.generateKey(rand), null, op);
        checkEqualRange(content1, t1, null, InMemoryTrieTestBase.generateKey(rand), op);

        ByteComparable l = rand.nextBoolean() ? InMemoryTrieTestBase.generateKey(rand) : src1[rand.nextInt(src1.length)];
        ByteComparable r = rand.nextBoolean() ? InMemoryTrieTestBase.generateKey(rand) : src1[rand.nextInt(src1.length)];
        int cmp = ByteComparable.compare(l, r, TrieImpl.BYTE_COMPARABLE_VERSION);
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
        NavigableMap<ByteComparable, ByteBuffer> imap = SlicedTrieTest.boundedMap(content1, l, r);

        Trie<ByteBuffer> intersection = op.apply(t1, l, r);

        assertMapEquals(intersection, imap, Direction.FORWARD);
        assertMapEquals(intersection, imap, Direction.REVERSE);
    }

    /**
     * Extract the values of the provide trie into a list.
     */
    private static <T> List<T> toList(Trie<T> trie)
    {
        return Iterables.toList(trie.values());
    }

    private Trie<Integer> fromList(int... list) throws InMemoryDTrie.SpaceExhaustedException
    {
        InMemoryDTrie<Integer> trie = new InMemoryDTrie<>(BufferType.ON_HEAP);
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
    public void testSimpleSubtrie() throws InMemoryDTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

            testIntersection("", asList(3, 4, 5, 6), trie, TrieSet.range(of(3), of(7)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6), trie, TrieSet.range(null, of(7)));

            testIntersection("", asList(3, 4, 5, 6, 7, 8, 9), trie, TrieSet.range(of(3), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie, TrieSet.range(null, null));

            testIntersection("", asList(), trie, TrieSet.range(of(7), of(7)));
        }
    }

    @Test
    public void testRangeOnSubtrie() throws InMemoryDTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            // non-overlapping
            testIntersection("", asList(), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(4), of(7)));
            // touching, i.e. still non-overlapping
            testIntersection("", asList(), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(3), of(7)));
            // overlapping 1
            testIntersection("", asList(2), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(2), of(7)));
            // overlapping 2
            testIntersection("", asList(1, 2), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(1), of(7)));
            // covered
            testIntersection("", asList(0, 1, 2), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(0), of(7)));
            // covered 2
            testIntersection("", asList(1, 2), trie, TrieSet.range(of(1), of(3)), TrieSet.range(of(0), of(7)));
        }
    }

    @Test
    public void testSimpleRanges() throws InMemoryDTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

            testIntersection("", asList(3, 4, 5, 6), trie, TrieSet.ranges(of(3), of(7)));

            testIntersection("", asList(3), trie, TrieSet.ranges(of(3), of(4)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6), trie, TrieSet.ranges(null, of(7)));

            testIntersection("", asList(3, 4, 5, 6, 7, 8, 9), trie, TrieSet.ranges(of(3), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie, TrieSet.ranges(null, null));

            testIntersection("", asList(3, 4, 5, 7, 8), trie, TrieSet.ranges(of(3), of(6), of(7), of(9)));

            testIntersection("", asList(3, 7, 8), trie, TrieSet.ranges(of(3), of(4), of(7), of(9)));

            testIntersection("", asList(3, 7, 8), trie, TrieSet.ranges(of(3), of(4), of(7), of(9), of(12), of(15)));

            testIntersection("", asList(3, 4, 5, 6, 7, 8), trie, TrieSet.ranges(of(3), of(9)));

            testIntersection("", asList(3), trie, TrieSet.ranges(of(3), of(4)));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 7, 8), trie, TrieSet.ranges(null, of(6), of(7), of(9)));

            testIntersection("", asList(3, 4, 5, 7, 8, 9), trie, TrieSet.ranges(of(3), of(6), of(7), null));

            testIntersection("", asList(0, 1, 2, 3, 4, 5, 7, 8, 9), trie, TrieSet.ranges(null, of(6), of(7), null));

            testIntersection("", asList(3, 4, 5, 6, 7, 8), trie, TrieSet.ranges(of(3), of(6), of(6), of(9)));

            testIntersection("", asList(3, 4, 5, 7, 8), trie, TrieSet.ranges(of(3), of(6), of(6), of(6), of(7), of(9)));
        }
    }

    @Test
    public void testRangesOnRangesOne() throws InMemoryDTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            Trie<Integer> trie = fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);

            // non-overlapping
            testIntersection("non-overlapping", asList(), trie, TrieSet.ranges(of(0), of(4)), TrieSet.ranges(of(4), of(8)));
            // touching
            testIntersection("touching", asList(3), trie, TrieSet.ranges(of(0), of(4)), TrieSet.ranges(of(3), of(8)));
            // overlapping 1
            testIntersection("overlapping A", asList(2, 3), trie, TrieSet.ranges(of(0), of(4)), TrieSet.ranges(of(2), of(8)));
            // overlapping 2
            testIntersection("overlapping B", asList(1, 2, 3), trie, TrieSet.ranges(of(0), of(4)), TrieSet.ranges(of(1), of(8)));
            // covered
            testIntersection("covered same end A", asList(0, 1, 2, 3), trie, TrieSet.ranges(of(0), of(4)), TrieSet.ranges(of(0), of(8)));
            // covered 2
            testIntersection("covered same end B", asList(4, 5, 6, 7), trie, TrieSet.ranges(of(4), of(8)), TrieSet.ranges(of(0), of(8)));
            // covered 3
            testIntersection("covered", asList(1, 2, 3), trie, TrieSet.ranges(of(1), of(4)), TrieSet.ranges(of(0), of(8)));
        }
    }

    @Test
    public void testRangesOnRanges() throws InMemoryDTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(fromList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14));
    }

    @Test
    public void testRangesOnMerge() throws InMemoryDTrie.SpaceExhaustedException
    {

        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(Trie.merge(ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                                                          fromList(4, 6, 7, 9, 10, 11, 12, 13)),
                                         RESOLVER));
    }

    @Test
    public void testRangesOnCollectionMerge2() throws InMemoryDTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            List<Trie<Integer>> inputs = ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                                                          fromList(4, 6, 7, 9, 10, 11, 12, 13));
            testIntersections((TrieWithImpl<Integer>) dir -> new CollectionMergeCursor.Deterministic<>(dir, RESOLVER, inputs));
        }
    }

    @Test
    public void testRangesOnCollectionMerge3() throws InMemoryDTrie.SpaceExhaustedException
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(Trie.merge(
                    ImmutableList.of(fromList(0, 1, 2, 3, 5, 8, 9, 13, 14),
                                     fromList(4, 6, 9, 10),
                                     fromList(4, 7, 11, 12, 13)),
                    RESOLVER));
    }

    @Test
    public void testRangesOnCollectionMerge10() throws InMemoryDTrie.SpaceExhaustedException
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

        TrieSet set1 = TrieSet.ranges(null, of(4), of(5), of(9), of(12), null);
        TrieSet set2 = TrieSet.ranges(of(2), of(7), of(8), of(10), of(12), of(14));
        TrieSet set3 = TrieSet.ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10));

        testIntersections(trie, set1, set2, set3);

        testSetAlgebraIntersection(trie);
    }

    private void testSetAlgebraIntersection(Trie<Integer> trie)
    {
        TrieSet set1 = TrieSet.range(null, of(3))
                              .union(TrieSet.range(of(2), of(4)))
                              .union(TrieSet.range(of(5), of(7)))
                              .union(TrieSet.range(of(7), of(9)))
                              .union(TrieSet.range(of(14), of(16)))
                              .union(TrieSet.range(of(12), null));
        TrieSet set2 = TrieSet.range(of(2), of(7))
                              .union(TrieSet.ranges(null, of(8), of(10), null).negation())
                              .union(TrieSet.ranges(of(8), of(10), of(12), of(14)));
        TrieSet set3 = TrieSet.range(of(1), of(2))
                              .union(TrieSet.range(of(3), of(4)))
                              .union(TrieSet.range(of(5), of(6)))
                              .union(TrieSet.range(of(7), of(8)))
                              .union(TrieSet.range(of(9), of(10)));

        testIntersections(trie, set1, set2, set3);

        set1 = TrieSet.range(null, of(3));
        set1 = unionByRangeIntersector(set1, TrieSet.range(of(2), of(4)));
        set1 = unionByRangeIntersector(set1, TrieSet.range(of(5), of(7)));
        set1 = unionByRangeIntersector(set1, TrieSet.range(of(7), of(9)));
        set1 = unionByRangeIntersector(set1, TrieSet.range(of(14), of(16)));
        set1 = unionByRangeIntersector(set1, TrieSet.range(of(12), null));
        set2 = TrieSet.range(of(2), of(7));
        set2 = unionByRangeIntersector(set2, TrieSet.ranges(null, of(8), of(10), null).negation());
        set2 = unionByRangeIntersector(set2, TrieSet.ranges(of(8), of(10), of(12), of(14)));;
        set3 = TrieSet.range(of(1), of(2));
        set3 = unionByRangeIntersector(set3, TrieSet.range(of(3), of(4)));
        set3 = unionByRangeIntersector(set3, TrieSet.range(of(5), of(6)));
        set3 = unionByRangeIntersector(set3, TrieSet.range(of(7), of(8)));
        set3 = unionByRangeIntersector(set3, TrieSet.range(of(9), of(10)));

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
        testIntersectionTriesByRangeApplyTo(message + " applyTo", expected, trie, sets);
        testIntersectionSets(message + " setix", expected, trie, TrieSet.range(null, null), sets);
        testIntersectionSetsByRangeIntersector(message + " rangeix", expected, trie, TrieSet.range(null, null), sets);
        testIntersectionInMemoryTrieDelete(message + " delete", expected, trie, sets);
    }

    public void testIntersectionSetsByRangeIntersector(String message, List<Integer> expected, Trie<Integer> trie, TrieSet intersectedSet, TrieSet[] sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            assertEquals(message + " sets b" + bits, expected, toList(trie.intersect(intersectedSet)));
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                TrieSet set = sets[toRemove];
                testIntersectionSetsByRangeIntersector(message + " " + toRemove, expected,
                                                       trie,
                                                       intersectByRangeIntersector(intersectedSet, set),
                                                       Arrays.stream(sets)
                                                             .filter(x -> x != set)
                                                             .toArray(TrieSet[]::new)
                );
            }
        }
    }

    private static TrieSetWithImpl intersectByRangeIntersector(TrieSet set1, TrieSet set2)
    {
        return dir -> new RangeIntersectionCursor.TrieSet(dir,
                                                          TrieSetImpl.INTERSECTION_CONTROLLER,
                                                          ((TrieSetImpl) set1).cursor(dir),
                                                          ((TrieSetImpl) set2).cursor(dir));
    }

    private static TrieSetWithImpl unionByRangeIntersector(TrieSet set1, TrieSet set2)
    {
        return dir -> new RangeIntersectionCursor.TrieSet(dir,
                                                          TrieSetImpl.UNION_CONTROLLER,
                                                          ((TrieSetImpl) set1).cursor(dir),
                                                          ((TrieSetImpl) set2).cursor(dir));
    }

    public void testIntersectionSets(String message, List<Integer> expected, Trie<Integer> trie, TrieSet intersectedSet, TrieSet[] sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            assertEquals(message + " sets b" + bits, expected, toList(trie.intersect(intersectedSet)));
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
            assertEquals(message + " forward b" + bits, expected, toList(trie));
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

    public void testIntersectionTriesByRangeApplyTo(String message, List<Integer> expected, Trie<Integer> trie, TrieSet[] sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            assertEquals(message + " forward b" + bits, expected, toList(trie));
        }
        else
        {
            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
            {
                TrieSet set = sets[toRemove];
                testIntersectionTriesByRangeApplyTo(message + " " + toRemove, expected,
                                                    applySet(set, trie),
                                                    Arrays.stream(sets)
                                                          .filter(x -> x != set)
                                                          .toArray(TrieSet[]::new)
                );
            }
        }
    }

    private static InMemoryDTrie<Integer> duplicateTrie(Trie<Integer> trie)
    {
        try
        {
            InMemoryDTrie<Integer> dupe = new InMemoryDTrie<>(BufferType.ON_HEAP);
            dupe.apply(trie, (x, y) -> y);
            return dupe;
        }
        catch (InMemoryTrie.SpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    public void testIntersectionInMemoryTrieDelete(String message, List<Integer> expected, Trie<Integer> trie, TrieSet[] sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            assertEquals(message + " forward b" + bits, expected, toList(trie));
        }
        else
        {
            try
            {
                for (int toRemove = 0; toRemove < sets.length; ++toRemove)
                {
                    TrieSet set = sets[toRemove];
                    InMemoryDTrie<Integer> ix = duplicateTrie(trie);
                    ix.deleteAllExcept(set);
                    testIntersectionInMemoryTrieDelete(message + " " + toRemove, expected,
                                                       ix,
                                                       Arrays.stream(sets)
                                                             .filter(x -> x != set)
                                                             .toArray(TrieSet[]::new)
                    );
                }
            }
            catch (InMemoryTrie.SpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    private <T> Trie<T> applySet(TrieSet set, Trie<T> trie)
    {
        RangeTrieWithImpl<TrieSetImpl.RangeState> setAsRangeTrie = new RangeTrieWithImpl<TrieSetImpl.RangeState>()
        {
            @Override
            public Cursor<TrieSetImpl.RangeState> makeCursor(Direction direction)
            {
                throw new AssertionError();
            }

            @Override
            public Cursor<TrieSetImpl.RangeState> cursor(Direction direction)
            {
                // disable debug verification (cursor is already checked by TrieSetWithImpl.cursor())
                return TrieSetImpl.impl(set).cursor(direction);
            }
        };
        return setAsRangeTrie.applyTo(trie, (range, value) -> range.matchingIncluded() ? value : null);
    }
}