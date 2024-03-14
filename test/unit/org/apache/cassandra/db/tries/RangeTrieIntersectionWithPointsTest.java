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

import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.DeletionMarker.fromList;
import static org.apache.cassandra.db.tries.DeletionMarker.toList;
import static org.apache.cassandra.db.tries.DeletionMarker.verify;
import static org.junit.Assert.assertEquals;

public class RangeTrieIntersectionWithPointsTest
{
    static final int bitsNeeded = 6;
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

    private DeletionMarker point(int where, int value)
    {
        return pointInside(where, value, -1);
    }

    private DeletionMarker pointInside(int where, int value, int active)
    {
        return new DeletionMarker(of(where), active, value, active);
    }

    private ByteComparable[] array(ByteComparable... data)
    {
        return data;
    }

    @Test
    public void testSubtrie()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            List<DeletionMarker> testRanges = getTestRanges();
            RangeTrie<DeletionMarker> trie = fromList(testRanges);

            System.out.println(trie.dump());
            assertEquals("No intersection", testRanges, toList(trie));

            testIntersection("all",
                             trie,
                             testRanges,
                             array(null, null));
            testIntersection("fully covered range",
                             trie,
                             testRanges,
                             array(of(20), of(25)));
            testIntersection("fully covered range",
                             trie,
                             testRanges,
                             array(of(25), of(33)));
            testIntersection("matching range",
                             trie,
                             testRanges,
                             array(of(21), of(24)));
            testIntersection("touching empty",
                             trie,
                             testRanges,
                             array(of(24), of(26)));

            testIntersection("partial left",
                             trie,
                             testRanges,
                             array(of(22), of(25)));
            testIntersection("partial left on change",
                             trie,
                             testRanges,
                             array(of(28), of(32)));
            testIntersection("partial left with null",
                             trie,
                             testRanges,
                             array(of(29), null));


            testIntersection("partial right",
                             trie,
                             testRanges,
                             array(of(25), of(27)));
            testIntersection("partial right on change",
                             trie,
                             testRanges,
                             array(of(25), of(28)));
            testIntersection("partial right with null",
                             trie,
                             testRanges,
                             array(null, of(22)));

            testIntersection("inside range",
                             trie,
                             testRanges,
                             array(of(22), of(23)));
            testIntersection("inside with change",
                             trie,
                             testRanges,
                             array(of(27), of(29)));

//            testIntersection("empty range inside",
//                             trie,
//                             testRanges,
//                             array(of(27), of(27)));

            testIntersection("point covered",
                             trie,
                             testRanges,
                             array(of(16), of(18)));
            testIntersection("point at range start",
                             trie,
                             testRanges,
                             array(of(17), of(18)));
            testIntersection("point at range end",
                             trie,
                             testRanges,
                             array(of(16), of(17)));


            testIntersection("start point covered",
                             trie,
                             testRanges,
                             array(of(32), of(35)));
            testIntersection("start point at range start",
                             trie,
                             testRanges,
                             array(of(33), of(35)));
            testIntersection("start point at range end",
                             trie,
                             testRanges,
                             array(of(32), of(33)));


            testIntersection("end point covered",
                             trie,
                             testRanges,
                             array(of(36), of(40)));
            testIntersection("end point at range start",
                             trie,
                             testRanges,
                             array(of(38), of(40)));
            testIntersection("end point at range end",
                             trie,
                             testRanges,
                             array(of(36), of(38)));
        }
    }

    @Test
    public void testRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            List<DeletionMarker> testRanges = getTestRanges();
            RangeTrie<DeletionMarker> trie = fromList(testRanges);

            testIntersection("fully covered ranges",
                             trie,
                             testRanges,
                             array(of(20), of(25), of(25), of(33)));
            testIntersection("matching ranges",
                             trie,
                             testRanges,
                             array(of(21), of(24), of(26), of(31)));
            testIntersection("touching empty",
                             trie,
                             testRanges,
                             array(of(20), of(21), of(24), of(26), of(32), of(33), of(34), of(36)));
            testIntersection("partial left",
                             trie,
                             testRanges,
                             array(of(22), of(25), of(29), null));

            testIntersection("partial right",
                             trie,
                             testRanges,
                             array(null, of(22), of(25), of(27)));

            testIntersection("inside ranges",
                             trie,
                             testRanges,
                             array(of(22), of(23), of(27), of(29)));

            testIntersection("jumping inside",
                             trie,
                             testRanges,
                             array(of(21), of(22), of(23), of(24), of(25), of(26), of(27), of(28), of(29), of(30)));
        }
    }

    @Test
    public void testRangeOnSubtrie()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            RangeTrie<DeletionMarker> trie = fromList(getTestRanges());

            // non-overlapping
            testIntersection("", trie, getTestRanges(), array(of(20), of(23)), array(of(24), of(27)));
            // touching, i.e. still non-overlapping
            testIntersection("", trie, getTestRanges(), array(of(20), of(23)), array(of(23), of(27)));
            // overlapping 1
            testIntersection("", trie, getTestRanges(), array(of(20), of(23)), array(of(22), of(27)));
            // overlapping 2
            testIntersection("", trie, getTestRanges(), array(of(20), of(23)), array(of(21), of(27)));
            // covered
            testIntersection("", trie, getTestRanges(), array(of(20), of(23)), array(of(20), of(27)));
            // covered
            testIntersection("", trie, getTestRanges(), array(of(23), of(27)), array(of(20), of(27)));
            // covered 2
            testIntersection("", trie, getTestRanges(), array(of(21), of(23)), array(of(20), of(27)));
        }
    }

    @Test
    public void testRangesOnRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testIntersections(fromList(getTestRanges()));
    }

    private List<DeletionMarker> getTestRanges()
    {
        return asList(point(17, 20),
                      from(21, 10), pointInside(22, 21, 10), to(24, 10),
                      from(26, 11), change(28, 11, 12).withPoint(22), to(30, 12), 
                      from(33, 13).withPoint(23), to(34, 13),
                      from(36, 14), to(38, 14).withPoint(24));
    }

    private void testIntersections(RangeTrie<DeletionMarker> trie)
    {
        testIntersection("", trie, getTestRanges());

        ByteComparable[] set1 = array(null, of(24), of(25), of(29), of(32), null);
        ByteComparable[] set2 = array(of(14), of(17),
                                      of(22), of(27),
                                      of(28), of(30),
                                      of(32), of(34),
                                      of(36), of(40));
        ByteComparable[] set3 = array(of(17), of(18),
                                      of(19), of(20),
                                      of(21), of(22),
                                      of(23), of(24),
                                      of(25), of(26),
                                      of(27), of(28),
                                      of(29), of(30),
                                      of(31), of(32),
                                      of(33), of(34),
                                      of(35), of(36),
                                      of(37), of(38));

        testIntersections(trie, set1, set2, set3);
    }

    private void testIntersections(RangeTrie<DeletionMarker> trie, ByteComparable[] set1, ByteComparable[] set2, ByteComparable[] set3)
    {
        // set1 = TrieSet.ranges(null, of(24), of(25), of(29), of(32), null);
        // set2 = TrieSet.ranges(of(22), of(27), of(28), of(30), of(32), of(34));
        // set3 = TrieSet.ranges(of(21), of(22), of(23), of(24), of(25), of(26), of(27), of(28), of(29), of(30));
        // from(21, 10), to(24, 10), from(26, 11), change(28, 11, 12), to(30, 12), from(33, 13), to(34, 13)
        List<DeletionMarker> testRanges = getTestRanges();
        testIntersection("1", trie, testRanges, set1);

        testIntersection("2", trie, testRanges, set2);

        testIntersection("3", trie, testRanges, set3);

        testIntersection("12", trie, testRanges, set1, set2);

        testIntersection("13", trie, testRanges, set1, set3);

        testIntersection("23", trie, testRanges, set2, set3);

        testIntersection("123", trie, testRanges, set1, set2, set3);
    }

    public void testIntersection(String message, RangeTrie<DeletionMarker> trie, List<DeletionMarker> intersected, ByteComparable[]... sets)
    {
        System.out.println("Markers: " + intersected);
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            try
            {
                assertEquals(message + " forward b" + bits, intersected, toList(trie));
                System.out.println(message + " forward b" + bits + " matched.");
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
                ByteComparable[] ranges = sets[toRemove];
                System.out.println("Ranges:  " + toString(ranges));
                testIntersection(message + " " + toRemove,
                                 trie.intersect(TrieSet.ranges(ranges)),
                                 intersect(intersected, ranges),
                                 Arrays.stream(sets)
                                       .filter(x -> x != ranges)
                                       .toArray(ByteComparable[][]::new)
                );
            }
        }
    }

    String toString(ByteComparable[] ranges)
    {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < ranges.length; i+=2)
        {
            b.append('[');
            b.append(toString(ranges[i]));
            b.append(';');
            b.append(toString(ranges[i + 1]));
            b.append(')');
        }
        return b.toString();
    }

    private static String toString(ByteComparable ranges)
    {
        if (ranges == null)
            return "null";
        return ranges.byteComparableAsString(TrieImpl.BYTE_COMPARABLE_VERSION);
    }


    List<DeletionMarker> intersect(List<DeletionMarker> markers, ByteComparable... ranges)
    {
        int rangeIndex = 0;
        int active = -1;
        ByteComparable nextRange = ranges[0];
        if (nextRange == null)
            nextRange = ++rangeIndex < ranges.length ? ranges[rangeIndex] : null;
        List<DeletionMarker> result = new ArrayList<>();
        for (DeletionMarker marker : markers)
        {
            while (true)
            {
                int cmp;
                if (nextRange == null)
                    cmp = -1;
                else
                    cmp = ByteComparable.compare(marker.position, nextRange, TrieImpl.BYTE_COMPARABLE_VERSION);

                if (cmp < 0)
                {
                    if ((rangeIndex & 1) != 0)
                        result.add(marker);
                    break;
                }

                if (cmp == 0)
                {
                    if ((rangeIndex & 1) != 0)
                        maybeAdd(result, marker.asReportableEnd());
                    else
                        maybeAdd(result, marker.asReportableStart());
                    nextRange = ++rangeIndex < ranges.length ? ranges[rangeIndex] : null;
                    break;
                }
                else if (active >= 0) // cmp > 0, must covert active to marker
                {
                    if ((rangeIndex & 1) != 0)
                        result.add(new DeletionMarker(nextRange, active, -1, -1));
                    else
                        result.add(new DeletionMarker(nextRange, -1, active, active));
                }

                nextRange = ++rangeIndex < ranges.length ? ranges[rangeIndex] : null;
            }
            active = marker.rightSide;
        }
        assert active == -1;
        return verify(result);
    }

    static <T> void maybeAdd(List<T> list, T value)
    {
        if (value == null)
            return;
        list.add(value);
    }
}
