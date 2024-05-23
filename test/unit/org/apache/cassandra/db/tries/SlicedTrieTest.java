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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.apache.cassandra.db.tries.TrieUtil.assertSameContent;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.makeInMemoryDTrie;
import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.TrieUtil.toBound;
import static org.junit.Assert.assertEquals;

public class SlicedTrieTest
{
    public static final ByteComparable[] BOUNDARIES = toByteComparable(new String[]{
    "test1",
    "test11",
    "test12",
    "test13",
    "test2",
    "test21",
    "te",
    "s",
    "q",
    "\000",
    "\377",
    "\377\000",
    "\000\377",
    "\000\000",
    "\000\000\000",
    "\000\000\377",
    "\377\377"
    });
    public static final ByteComparable[] KEYS = toByteComparable(new String[]{
    "test1",
    "test2",
    "test55",
    "test123",
    "test124",
    "test12",
    "test21",
    "tease",
    "sort",
    "sorting",
    "square",
    "\377\000",
    "\000\377",
    "\000\000",
    "\000\000\000",
    "\000\000\377",
    "\377\377"
    });
    public static final Comparator<ByteComparable> BYTE_COMPARABLE_COMPARATOR =
        (bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, TrieImpl.BYTE_COMPARABLE_VERSION);
    private static final int COUNT = 15000;
    Random rand = new Random();

    @Test
    public void testIntersectRangeDirect()
    {
        testIntersectRange(COUNT);
    }

    public void testIntersectRange(int count)
    {
        ByteComparable[] src1 = generateKeys(rand, count);
        NavigableMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, TrieImpl.BYTE_COMPARABLE_VERSION));

        InMemoryDTrie<ByteBuffer> trie1 = makeInMemoryDTrie(src1, content1, true);

        checkEqualRange(content1, trie1, null, false, null, false);
        checkEqualRange(content1, trie1, TrieUtil.generateKey(rand), true, null, false);
        checkEqualRange(content1, trie1, null, false, TrieUtil.generateKey(rand), true);
        for (int i = 0; i < 4; ++i)
        {
            ByteComparable l = rand.nextBoolean() ? TrieUtil.generateKey(rand) : src1[rand.nextInt(src1.length)];
            ByteComparable r = rand.nextBoolean() ? TrieUtil.generateKey(rand) : src1[rand.nextInt(src1.length)];
            int cmp = ByteComparable.compare(l, r, TrieImpl.BYTE_COMPARABLE_VERSION);
            if (cmp > 0)
            {
                ByteComparable t = l;
                l = r;
                r = t; // swap
            }

            boolean includeLeft = (i & 1) != 0 || cmp == 0;
            boolean includeRight = (i & 2) != 0 || cmp == 0;
            checkEqualRange(content1, trie1, l, includeLeft, r, includeRight);
            checkEqualRange(content1, trie1, null, includeLeft, r, includeRight);
            checkEqualRange(content1, trie1, l, includeLeft, null, includeRight);
        }
    }

    private static ByteComparable[] toByteComparable(String[] keys)
    {
        // Keys must be prefix-free
        return Arrays.stream(keys)
                     .map(x -> ByteComparable.of(x))
                     .toArray(ByteComparable[]::new);
    }

    @Test
    public void testSingletonSubtrie()
    {
        Arrays.sort(BOUNDARIES, (a, b) -> ByteComparable.compare(a, b, ByteComparable.Version.OSS50));
        for (int li = -1; li < BOUNDARIES.length; ++li)
        {
            ByteComparable l = li < 0 ? null : BOUNDARIES[li];
            for (int ri = Math.max(0, li); ri <= BOUNDARIES.length; ++ri)
            {
                ByteComparable r = ri == BOUNDARIES.length ? null : BOUNDARIES[ri];

                for (int i = li == ri ? 3 : 0; i < 4; ++i)
                {
                    boolean includeLeft = (i & 1) != 0;
                    boolean includeRight = (i & 2) != 0;

                    for (ByteComparable key : KEYS)
                    {
                        int cmp1 = l != null ? ByteComparable.compare(key, l, ByteComparable.Version.OSS50) : 1;
                        int cmp2 = r != null ? ByteComparable.compare(r, key, ByteComparable.Version.OSS50) : 1;
                        Trie<Boolean> ix = Trie.singleton(key, true).subtrie(toBound(l, !includeLeft), toBound(r, includeRight));
                        boolean expected = true;
                        if (cmp1 < 0 || cmp1 == 0 && !includeLeft)
                            expected = false;
                        if (cmp2 < 0 || cmp2 == 0 && !includeRight)
                            expected = false;
                        boolean actual = com.google.common.collect.Iterables.getFirst(ix.values(), false);
                        if (expected != actual)
                        {
                            System.err.println("Intersection");
                            System.err.println(ix.dump());
                            Assert.fail(String.format("Failed on range %s%s,%s%s key %s expected %s got %s\n",
                                                      includeLeft ? "[" : "(",
                                                      l != null ? l.byteComparableAsString(ByteComparable.Version.OSS50) : null,
                                                      r != null ? r.byteComparableAsString(ByteComparable.Version.OSS50) : null,
                                                      includeRight ? "]" : ")",
                                                      key.byteComparableAsString(ByteComparable.Version.OSS50),
                                                      expected,
                                                      actual));
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testMemtableSubtrie()
    {
        Arrays.sort(BOUNDARIES, BYTE_COMPARABLE_COMPARATOR);
        NavigableMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
        InMemoryDTrie<ByteBuffer> trie1 = makeInMemoryDTrie(KEYS, content1, true);

        for (int li = -1; li < BOUNDARIES.length; ++li)
        {
            ByteComparable l = li < 0 ? null : BOUNDARIES[li];
            for (int ri = Math.max(0, li); ri <= BOUNDARIES.length; ++ri)
            {
                ByteComparable r = ri == BOUNDARIES.length ? null : BOUNDARIES[ri];
                for (int i = 0; i < 4; ++i)
                {
                    boolean includeLeft = (i & 1) != 0;
                    boolean includeRight = (i & 2) != 0;
                    if ((!includeLeft || !includeRight) && li == ri)
                        continue;
                    checkEqualRange(content1, trie1, l, includeLeft, r, includeRight);
                }
            }
        }
    }

    @Test
    public void testMergeSubtrie()
    {
        testMergeSubtrie(2);
    }

    @Test
    public void testCollectionMergeSubtrie3()
    {
        testMergeSubtrie(3);
    }

    @Test
    public void testCollectionMergeSubtrie5()
    {
        testMergeSubtrie(5);
    }

    public void testMergeSubtrie(int mergeCount)
    {
        Arrays.sort(BOUNDARIES, BYTE_COMPARABLE_COMPARATOR);
        NavigableMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>(BYTE_COMPARABLE_COMPARATOR);
        List<Trie<ByteBuffer>> tries = new ArrayList<>();
        for (int i = 0; i < mergeCount; ++i)
        {
            tries.add(makeInMemoryDTrie(Arrays.copyOfRange(KEYS,
                                                           KEYS.length * i / mergeCount,
                                                           KEYS.length * (i + 1) / mergeCount),
                                        content1,
                                        true));
        }
        Trie<ByteBuffer> trie1 = Trie.mergeDistinct(tries);

        for (int li = -1; li < BOUNDARIES.length; ++li)
        {
            ByteComparable l = li < 0 ? null : BOUNDARIES[li];
            for (int ri = Math.max(0, li); ri <= BOUNDARIES.length; ++ri)
            {
                ByteComparable r = ri == BOUNDARIES.length ? null : BOUNDARIES[ri];
                for (int i = 0; i < 4; ++i)
                {
                    boolean includeLeft = (i & 1) != 0;
                    boolean includeRight = (i & 2) != 0;
                    if ((!includeLeft || !includeRight) && li == ri)
                        continue;
                    checkEqualRange(content1, trie1, l, includeLeft, r, includeRight);
                }
            }
        }
    }


    static <T> NavigableMap<ByteComparable, T> boundedMap(NavigableMap<ByteComparable, T> sourceMap, ByteComparable l, boolean includeLeft, ByteComparable r, boolean includeRight)
    {
        NavigableMap<ByteComparable, T> imap = l == null
                                               ? r == null
                                                 ? sourceMap
                                                 : sourceMap.headMap(r, includeRight)
                                               : r == null
                                                 ? sourceMap.tailMap(l, includeLeft)
                                                 : sourceMap.subMap(l, includeLeft, r, includeRight);
        return imap;
    }

    public void checkEqualRange(NavigableMap<ByteComparable, ByteBuffer> content1,
                                Trie<ByteBuffer> t1,
                                ByteComparable l,
                                boolean includeLeft,
                                ByteComparable r,
                                boolean includeRight)
    {
        System.out.println(String.format("Intersection with %s%s:%s%s", includeLeft ? "[" : "(", asString(l), asString(r), includeRight ? "]" : ")"));
        SortedMap<ByteComparable, ByteBuffer> imap = boundedMap(content1, l, includeLeft, r, includeRight);
        Trie<ByteBuffer> intersection = t1.subtrie(toBound(l, !includeLeft), toBound(r, includeRight));
        try
        {
            assertSameContent(intersection, imap);
        }
        catch (AssertionError e)
        {
            System.out.println("\n" + t1.dump(ByteBufferUtil::bytesToHex));

            System.out.println("\n" + intersection.dump(ByteBufferUtil::bytesToHex));
            throw e;
        }

        if (l == null || r == null)
            return;

        // Test intersecting intersection.
        intersection = t1.subtrie(toBound(l, !includeLeft), null).subtrie(null, toBound(r, includeRight));
        assertSameContent(intersection, imap);

        intersection = t1.subtrie(null, toBound(r, includeRight)).subtrie(toBound(l, !includeLeft), null);
        assertSameContent(intersection, imap);
    }

    /**
     * Extract the values of the provide trie into a list.
     */
    private static <T> List<T> toList(Trie<T> trie, Direction direction)
    {
        return Iterables.toList(trie.values(direction));
    }

    /**
     * Creates a simple trie with a root having the provided number of childs, where each child is a leaf whose content
     * is simply the value of the transition leading to it.
     *
     * In other words, {@code singleLevelIntTrie(4)} creates the following trie:
     *       Root
     * t= 0  1  2  3
     *    |  |  |  |
     *    0  1  2  3
     */
    private static Trie<Integer> singleLevelIntTrie(int childs)
    {
        return new TrieWithImpl<Integer>()
        {
            @Override
            public Cursor<Integer> makeCursor(Direction direction)
            {
                return new SingleLevelCursor(direction);
            }

            class SingleLevelCursor implements Cursor<Integer>
            {
                final Direction direction;
                int current;

                SingleLevelCursor(Direction direction)
                {
                    this.direction = direction;
                    current = direction.select(-1, childs);
                }

                @Override
                public int advance()
                {
                    current += direction.increase;
                    return depth();
                }

                @Override
                public int skipTo(int depth, int transition)
                {
                    if (depth > 1)
                        return advance();
                    if (depth < 1)
                        transition = direction.select(childs, -1);

                    if (direction.isForward())
                        current = Math.max(0, transition);
                    else
                        current = Math.min(childs - 1, transition);

                    return depth();
                }

                @Override
                public int depth()
                {
                    if (current == direction.select(-1, childs))
                        return 0;
                    if (direction.inLoop(current, 0, childs - 1))
                        return 1;
                    return -1;
                }

                @Override
                public int incomingTransition()
                {
                    return current >= childs ? -1 : current;
                }

                @Override
                public Integer content()
                {
                    return current == direction.select(-1, childs) ? -1 : current;
                }

                @Override
                public Cursor<Integer> duplicate()
                {
                    SingleLevelCursor copy = new SingleLevelCursor(direction);
                    copy.current = current;
                    return copy;
                }
            }
        };
    }

    /** Creates a single byte {@link ByteComparable} with the provide value */
    private static ByteComparable of(int value)
    {
        assert value >= 0 && value <= Byte.MAX_VALUE;
        return ByteComparable.fixedLength(new byte[]{ (byte)value });
    }

    List<Integer> maybeReversed(Direction direction, List<Integer> list)
    {
        if (direction.isForward())
            return list;
        List<Integer> reversed = new ArrayList<>(list);
        reversed.sort((x, y) -> x == -1 ? -1 : y == -1 ? 1 : Integer.compare(y, x));
        return reversed;
    }

    void assertTrieEquals(List<Integer> expected, Trie<Integer> trie)
    {
        assertEquals(expected, toList(trie, Direction.FORWARD));
        assertEquals(maybeReversed(Direction.REVERSE, expected), toList(trie, Direction.REVERSE));
    }

    @Test
    public void testSimpleIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(of(3), of(7));
        assertTrieEquals(asList(-1, 3, 4, 5, 6, 7), intersection);
    }

    @Test
    public void testSimpleLeftIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(of(3), null);
        assertTrieEquals(asList(-1, 3, 4, 5, 6, 7, 8, 9), intersection);
    }

    @Test
    public void testSimpleRightIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(null, of(7));
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7), intersection);
    }

    @Test
    public void testSimpleNoIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(null, null);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);
    }

    @Test
    public void testSimpleEmptyIntersectionLeft()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(ByteComparable.EMPTY, null);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);

        intersection = trie.subtrie(ByteComparable.EMPTY, ByteComparable.EMPTY);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);
    }

    @Test
    public void testSimpleEmptyIntersectionRight()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), trie);

        Trie<Integer> intersection = trie.subtrie(null, ByteComparable.EMPTY);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);

        intersection = trie.subtrie(ByteComparable.EMPTY, ByteComparable.EMPTY);
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), intersection);
    }

    @Test
    public void testSubtrieOnSubtrie()
    {
        Trie<Integer> trie = singleLevelIntTrie(15);

        // non-overlapping
        Trie<Integer> intersection = trie.subtrie(of(0), of(4)).subtrie(of(5), of(8));
        assertTrieEquals(asList(-1), intersection);
        // touching
        intersection = trie.subtrie(of(0), of(3)).subtrie(of(3), of(8));
        assertTrieEquals(asList(-1, 3), intersection);
        // overlapping 1
        intersection = trie.subtrie(of(0), of(4)).subtrie(of(2), of(8));
        assertTrieEquals(asList(-1, 2, 3, 4), intersection);
        // overlapping 2
        intersection = trie.subtrie(of(0), of(4)).subtrie(of(1), of(8));
        assertTrieEquals(asList(-1, 1, 2, 3, 4), intersection);
        // covered
        intersection = trie.subtrie(of(0), of(4)).subtrie(of(0), of(8));
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4), intersection);
        // covered 2
        intersection = trie.subtrie(of(4), of(8)).subtrie(of(0), of(8));
        assertTrieEquals(asList(-1, 4, 5, 6, 7, 8), intersection);
    }

    @Test
    public void testIntersectedIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(15);

        // non-overlapping
        Trie<Integer> intersection = trie.intersect(TrieSet.range(of(0), of(4))).intersect(TrieSet.range(of(5), of(8)));
        assertTrieEquals(asList(-1), intersection);
        // touching
        intersection = trie.intersect(TrieSet.range(of(0), of(3))).intersect(TrieSet.range(of(3), of(8)));
        assertTrieEquals(asList(-1, 3), intersection);
        // overlapping 1
        intersection = trie.intersect(TrieSet.range(of(0), of(4))).intersect(TrieSet.range(of(2), of(8)));
        assertTrieEquals(asList(-1, 2, 3, 4), intersection);
        // overlapping 2
        intersection = trie.intersect(TrieSet.range(of(0), of(4))).intersect(TrieSet.range(of(1), of(8)));
        assertTrieEquals(asList(-1, 1, 2, 3, 4), intersection);
        // covered
        intersection = trie.intersect(TrieSet.range(of(0), of(4))).intersect(TrieSet.range(of(0), of(8)));
        assertTrieEquals(asList(-1, 0, 1, 2, 3, 4), intersection);
        // covered 2
        intersection = trie.intersect(TrieSet.range(of(4), of(8))).intersect(TrieSet.range(of(0), of(8)));
        assertTrieEquals(asList(-1, 4, 5, 6, 7, 8), intersection);
    }
}
