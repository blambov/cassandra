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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import org.junit.Test;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.MemtableTrieTestBase.asString;
import static org.apache.cassandra.db.tries.MemtableTrieTestBase.assertMapEquals;
import static org.apache.cassandra.db.tries.MemtableTrieTestBase.assertSameContent;
import static org.apache.cassandra.db.tries.MemtableTrieTestBase.generateKeys;
import static org.apache.cassandra.db.tries.MemtableTrieTestBase.makeMemtableTrie;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class SetIntersectionTrieTest
{
    private static final int COUNT = 1000;
    Random rand = new Random();

    @Test
    public void testIntersectRangeDirect()
    {
        testIntersectRange(COUNT);
    }

    public void testIntersectRange(int count)
    {
        ByteComparable[] src1 = generateKeys(rand, count);
        NavigableMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, Trie.BYTE_COMPARABLE_VERSION));

        MemtableTrie<ByteBuffer> trie1 = makeMemtableTrie(src1, content1, true);

        checkEqualRange(content1, trie1, null, true, null, true);
        for (int loop = 0; loop < 100; ++loop)
        {
            checkEqualRange(content1, trie1, MemtableTrieTestBase.generateKey(rand), true, null, true);
            checkEqualRange(content1, trie1, null, true, MemtableTrieTestBase.generateKey(rand), true);
            for (int i = 0; i < 4; ++i)
            {
                ByteComparable l = rand.nextBoolean() ? MemtableTrieTestBase.generateKey(rand) : src1[rand.nextInt(src1.length)];
                ByteComparable r = rand.nextBoolean() ? MemtableTrieTestBase.generateKey(rand) : src1[rand.nextInt(src1.length)];
                int cmp = ByteComparable.compare(l, r, Trie.BYTE_COMPARABLE_VERSION);
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
    }

    @Test
    public void testSpecified()
    {
        testSpecifiedIntersections(new String[]{
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
                                       "square"
                                   },
                                   new String[]{
                                       "test1",
                                       "test11",
                                       "test12",
                                       "test13",
                                       "test2",
                                       "test21",
                                       "te",
                                       "s",
                                       "q",
                                       ""
                                   });
    }

    public void testSpecifiedIntersections(String[] keys, String[] boundaries)
    {
        testSpecifiedIntersections(toByteComparable(keys),
                                   toByteComparable(boundaries));
    }

    private ByteComparable[] toByteComparable(String[] keys)
    {
        return Arrays.stream(keys)
                     .map(x -> ByteComparable.fixedLength(x.getBytes(StandardCharsets.UTF_8)))
                     .toArray(ByteComparable[]::new);
    }

    public void testSpecifiedIntersections(ByteComparable[] keys, ByteComparable[] boundaries)
    {
        Comparator<ByteComparable> byteComparableComparator = (bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, Trie.BYTE_COMPARABLE_VERSION);
        Arrays.sort(boundaries, byteComparableComparator);
        NavigableMap<ByteComparable, ByteBuffer> content1 = new TreeMap<>(byteComparableComparator);
        MemtableTrie<ByteBuffer> trie1 = makeMemtableTrie(keys, content1, true);

        for (int li = -1; li < boundaries.length; ++li)
        {
            ByteComparable l = li < 0 ? null : boundaries[li];
            for (int ri = Math.max(0, li); ri <= boundaries.length; ++ri)
            {
                ByteComparable r = ri == boundaries.length ? null : boundaries[ri];
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

    public void checkEqualRange(NavigableMap<ByteComparable, ByteBuffer> content1,
                                Trie<ByteBuffer> t1,
                                ByteComparable l,
                                boolean includeLeft,
                                ByteComparable r,
                                boolean includeRight)
    {
        System.out.println(String.format("Intersection with %s%s:%s%s", includeLeft ? "[" : "(", asString(l), asString(r), includeRight ? "]" : ")"));
        SortedMap<ByteComparable, ByteBuffer> imap = l == null
                                                     ? r == null
                                                       ? content1
                                                       : content1.headMap(r, includeRight)
                                                     : r == null
                                                       ? content1.tailMap(l, includeLeft)
                                                       : content1.subMap(l, includeLeft, r, includeRight);
        Trie<ByteBuffer> intersection = t1.subtrie(l, includeLeft, r, includeRight);

        assertSameContent(intersection, imap);
    }

    /**
     * Extract the values of the provide trie into a list.
     */
    private static <T> List<T> toList(Trie<T> trie)
    {
        return Iterables.toList(trie.values());
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
        return new Trie<Integer>()
        {
            @Override
            protected Cursor<Integer> cursor()
            {
                return new SingleLevelCursor();
            }

            class SingleLevelCursor implements Cursor<Integer>
            {
                int current = -1;

                @Override
                public int advance()
                {
                    ++current;
                    return level();
                }

                @Override
                public int ascend()
                {
                    return advance();
                }

                @Override
                public int level()
                {
                    if (current == -1)
                        return 0;
                    if (current < childs)
                        return 1;
                    return -1;
                }

                @Override
                public int incomingTransition()
                {
                    return current;
                }

                @Override
                public Integer content()
                {
                    return current;
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

    @Test
    public void testSimpleIntersectionII()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), true, of(7), true);
        assertEquals(asList(3, 4, 5, 6, 7), toList(intersection));
    }

    @Test
    public void testSimpleIntersectionEI()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), false, of(7), true);
        assertEquals(asList(4, 5, 6, 7), toList(intersection));
    }

    @Test
    public void testSimpleIntersectionIE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), true, of(7), false);
        assertEquals(asList(3, 4, 5, 6), toList(intersection));
    }

    @Test
    public void testSimpleIntersectionEE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), false, of(7), false);
        assertEquals(asList(4, 5, 6), toList(intersection));
    }

    @Test
    public void testSimpleLeftIntersectionE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), false, null, true);
        assertEquals(asList(4, 5, 6, 7, 8, 9), toList(intersection));
    }

    @Test
    public void testSimpleLeftIntersectionI()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(of(3), true, null, true);
        assertEquals(asList(3, 4, 5, 6, 7, 8, 9), toList(intersection));
    }

    @Test
    public void testSimpleRightIntersectionE()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(null, true, of(7), false);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6), toList(intersection));
    }

    @Test
    public void testSimpleRightIntersectionI()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(null, true, of(7), true);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7), toList(intersection));
    }

    @Test
    public void testSimpleNoIntersection()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(null, true, null, true);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(intersection));
    }

    @Test
    public void testSimpleEmptyIntersectionLeft()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(ByteComparable.EMPTY, true, null, true);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, false, null, true);
        assertEquals(asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, true, of(5), true);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, false, of(5), true);
        assertEquals(asList(0, 1, 2, 3, 4, 5), toList(intersection));

    }

    @Test
    public void testSimpleEmptyIntersectionRight()
    {
        Trie<Integer> trie = singleLevelIntTrie(10);
        assertEquals(asList(-1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9), toList(trie));

        Trie<Integer> intersection = trie.subtrie(null, true, ByteComparable.EMPTY, true);
        assertEquals(asList(-1), toList(intersection));

        intersection = trie.subtrie(null, true, ByteComparable.EMPTY, false);
        assertEquals(asList(), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, true, ByteComparable.EMPTY, true);
        assertEquals(asList(-1), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, false, ByteComparable.EMPTY, true);
        assertEquals(asList(), toList(intersection));

        intersection = trie.subtrie(ByteComparable.EMPTY, true, ByteComparable.EMPTY, false);
        assertEquals(asList(), toList(intersection));

        // (empty, empty) is an invalid call as the "(empty" is greater than "empty)"
    }
}
