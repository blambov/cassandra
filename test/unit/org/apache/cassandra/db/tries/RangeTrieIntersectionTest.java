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
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.collect.Streams;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.DeletionMarker.fromList;
import static org.apache.cassandra.db.tries.DeletionMarker.toList;
import static org.junit.Assert.assertEquals;

public class RangeTrieIntersectionTest
{
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
        return ByteComparable.fixedLength(splitBytes);
    }

    private DeletionMarker from(int where, int value)
    {
        return new DeletionMarker(of(where), -1, value, value);
    }

    private DeletionMarker to(int where, int value)
    {
        return new DeletionMarker(of(where), value, -1, -1);
    }

    private DeletionMarker change(int where, int from, int to)
    {
        return new DeletionMarker(of(where), from, to, to);
    }

    @Test
    public void testSubtrie()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            RangeTrie<DeletionMarker> trie = fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)));

            System.out.println(trie.dump());
            assertEquals("No intersection", asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)), toList(trie));

            testIntersection("all",
                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)),
                             trie,
                             TrieSet.range(null, null));
            testIntersection("fully covered range",
                             asList(from(1, 10), to(4, 10)),
                             trie,
                             TrieSet.range(of(0), of(5)));
            testIntersection("fully covered range",
                             asList(from(6, 11), change(8, 11, 12), to(10, 12)),
                             trie,
                             TrieSet.range(of(5), of(13)));
            testIntersection("matching range",
                             asList(from(1, 10), to(4, 10)),
                             trie,
                             TrieSet.range(of(1), of(4)));
            testIntersection("touching empty",
                             asList(),
                             trie,
                             TrieSet.range(of(4), of(6)));

            testIntersection("partial left",
                             asList(from(2, 10), to(4, 10)),
                             trie,
                             TrieSet.range(of(2), of(5)));
            testIntersection("partial left on change",
                             asList(from(8, 12), to(10, 12)),
                             trie,
                             TrieSet.range(of(8), of(12)));
            testIntersection("partial left with null",
                             asList(from(9, 12), to(10, 12)),
                             trie,
                             TrieSet.range(of(9), null));


            testIntersection("partial right",
                             asList(from(6, 11), to(7, 11)),
                             trie,
                             TrieSet.range(of(5), of(7)));
            testIntersection("partial right on change",
                             asList(from(6, 11), to(8, 11)),
                             trie,
                             TrieSet.range(of(5), of(8)));
            testIntersection("partial right with null",
                             asList(from(1, 10), to(2, 10)),
                             trie,
                             TrieSet.range(null, of(2)));

            testIntersection("inside range",
                             asList(from(2, 10), to(3, 10)),
                             trie,
                             TrieSet.range(of(2), of(3)));
            testIntersection("inside with change",
                             asList(from(7, 11), change(8, 11, 12), to(9, 12)),
                             trie,
                             TrieSet.range(of(7), of(9)));

            testIntersection("empty range inside",
                             asList(),
                             trie,
                             TrieSet.range(of(7), of(7)));
        }
    }

    @Test
    public void testRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            RangeTrie<DeletionMarker> trie = fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)));

            testIntersection("fully covered ranges",
                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)),
                             trie,
                             TrieSet.ranges(of(0), of(5), of(5), of(13)));
            testIntersection("matching ranges",
                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)),
                             trie,
                             TrieSet.ranges(of(1), of(4), of(6), of(11)));
            testIntersection("touching empty",
                             asList(),
                             trie,
                             TrieSet.ranges(of(0), of(1), of(4), of(6), of(12), of(15)));
            testIntersection("partial left",
                             asList(from(2, 10), to(4, 10), from(9, 12), to(10, 12)),
                             trie,
                             TrieSet.ranges(of(2), of(5), of(9), null));

            testIntersection("partial right",
                             asList(from(1, 10), to(2, 10), from(6, 11), to(7, 11)),
                             trie,
                             TrieSet.ranges(null, of(2), of(5), of(7)));

            testIntersection("inside ranges",
                             asList(from(2, 10), to(3, 10), from(7, 11), change(8, 11, 12), to(9, 12)),
                             trie,
                             TrieSet.ranges(of(2), of(3), of(7), of(9)));

            testIntersection("jumping inside",
                             asList(from(1, 10), to(2, 10), from(3, 10), to(4, 10), from(7, 11), to(8, 11), from(9, 12), to(10, 12)),
                             trie,
                             TrieSet.ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10)));
        }
    }

    @Test
    public void testRangeOnSubtrie()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            RangeTrie<DeletionMarker> trie = fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12), from(13, 13), to(14, 13)));

            // non-overlapping
            testIntersection("", asList(), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(4), of(7)));
            // touching, i.e. still non-overlapping
            testIntersection("", asList(), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(3), of(7)));
            // overlapping 1
            testIntersection("", asList(from(2, 10), to(3, 10)), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(2), of(7)));
            // overlapping 2
            testIntersection("", asList(from(1, 10), to(3, 10)), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(1), of(7)));
            // covered
            testIntersection("", asList(from(1, 10), to(3, 10)), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(0), of(7)));
            // covered
            testIntersection("", asList(from(3, 10), to(4, 10), from(6, 11), to(7, 11)), trie, TrieSet.range(of(3), of(7)), TrieSet.range(of(0), of(7)));
            // covered 2
            testIntersection("", asList(from(1, 10), to(3, 10)), trie, TrieSet.range(of(1), of(3)), TrieSet.range(of(0), of(7)));
        }
    }

    @Test
    public void testRangesOnRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(fromList(asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12), from(13, 13), to(14, 13))));
    }

    private void testIntersections(RangeTrie<DeletionMarker> trie)
    {
        testIntersection("", asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12), from(13, 13), to(14, 13)), trie);

        TrieSet set1 = TrieSet.ranges(null, of(4), of(5), of(9), of(12), null);
        TrieSet set2 = TrieSet.ranges(of(2), of(7), of(8), of(10), of(12), of(14));
        TrieSet set3 = TrieSet.ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10));

        testIntersections(trie, set1, set2, set3);

        testSetAlgebraIntersection(trie);
    }

    private void testSetAlgebraIntersection(RangeTrie<DeletionMarker> trie)
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
    }

    private void testIntersections(RangeTrie<DeletionMarker> trie, TrieSet set1, TrieSet set2, TrieSet set3)
    {
        // set1 = TrieSet.ranges(null, of(4), of(5), of(9), of(12), null);
        // set2 = TrieSet.ranges(of(2), of(7), of(8), of(10), of(12), of(14));
        // set3 = TrieSet.ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10));
        // from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12), from(13, 13), to(14, 13)
        testIntersection("1", asList(from(1, 10), to(4, 10),
                                     from(6, 11), change(8, 11, 12), to(9, 12),
                                     from(13, 13), to(14,13)), trie, set1);

        testIntersection("2", asList(from(2, 10), to(4, 10),
                                     from(6, 11), to(7, 11),
                                     from(8, 12), to(10, 12),
                                     from(13, 13), to(14, 13)), trie, set2);

        testIntersection("3", asList(from(1, 10), to(2, 10),
                                     from(3, 10), to(4, 10),
                                     from(7, 11), to(8, 11),
                                     from(9, 12), to(10, 12)), trie, set3);

        testIntersection("12", asList(from(2, 10), to(4, 10),
                                      from(6, 11), to(7, 11),
                                      from(8, 12), to(9, 12),
                                      from(13, 13), to(14, 13)), trie, set1, set2);

        testIntersection("13", asList(from(1, 10), to(2, 10),
                                      from(3, 10), to(4, 10),
                                      from(7, 11), to(8, 11)), trie, set1, set3);

        testIntersection("23", asList(from(3, 10), to(4, 10),
                                      from(9, 12), to(10, 12)), trie, set2, set3);

        testIntersection("123", asList(from(3, 10), to(4, 10)), trie, set1, set2, set3);
    }

    public void testIntersection(String message, List<DeletionMarker> expected, RangeTrie<DeletionMarker> trie, TrieSet... sets)
    {
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            try
            {
                assertEquals(message + " forward b" + bits, expected, toList(trie));
            }
            catch (AssertionError e)
            {
                System.out.println("\n" + trie.dump());
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
}
