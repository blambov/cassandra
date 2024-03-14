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
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.DeletionMarker.fromList;
import static org.apache.cassandra.db.tries.DeletionMarker.toList;
import static org.apache.cassandra.db.tries.DeletionMarker.verify;
import static org.junit.Assert.assertEquals;

public class RangeTrieMergeTest
{
    static final int bitsNeeded = 6;
    int bits = bitsNeeded;

    /** Creates a {@link ByteComparable} for the provided value by splitting the integer in sequences of "bits" bits. */
    private ByteComparable of(int value)
    {
        assert value >= 0 && value < 1<< bitsNeeded;

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

    private List<DeletionMarker> deletedRanges(ByteComparable... dataPoints)
    {
        List<ByteComparable> data = new ArrayList<>(asList(dataPoints));
        invertDataRangeList(data);
        filterOutEmptyRepetitions(data);

        List<DeletionMarker> markers = new ArrayList<>();
        for (int i = 0; i < data.size(); ++i)
        {
            ByteComparable pos = data.get(i);
            if (pos == null)
                pos = i % 2 == 0 ? of(0) : of((1<<bitsNeeded) - 1);
            if (i % 2 == 0)
                markers.add(new DeletionMarker(pos, -1, 100, 100, true));
            else
                markers.add(new DeletionMarker(pos, 100, -1, -1, true));
        }
        return verify(markers);
    }

    private static void invertDataRangeList(List<ByteComparable> data)
    {
        // invert list
        if (data.get(0) != null)
            data.add(0, null);
        else
            data.remove(0);
        if (data.get(data.size() - 1) != null)
            data.add(null);
        else
            data.remove(data.size() - 1);
    }

    private static void filterOutEmptyRepetitions(List<ByteComparable> data)
    {
        for (int i = 0; i < data.size() - 1; ++i)
        {
            if (data.get(i) != null && data.get(i + 1) != null &&
                ByteComparable.compare(data.get(i), data.get(i + 1), TrieImpl.BYTE_COMPARABLE_VERSION) == 0)
            {
                data.remove(i + 1);
                data.remove(i);
                --i;
            }
        }
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

            testMerge("all",
                      trie,
                      testRanges,
                      deletedRanges(null, null));
            testMerge("fully covered range",
                      trie,
                      testRanges,
                      deletedRanges(of(20), of(25)));
            testMerge("fully covered range",
                      trie,
                      testRanges,
                      deletedRanges(of(25), of(33)));
            testMerge("matching range",
                      trie,
                      testRanges,
                      deletedRanges(of(21), of(24)));
            testMerge("touching empty",
                      trie,
                      testRanges,
                      deletedRanges(of(24), of(26)));

            testMerge("partial left",
                      trie,
                      testRanges,
                      deletedRanges(of(22), of(25)));
            testMerge("partial left on change",
                      trie,
                      testRanges,
                      deletedRanges(of(28), of(32)));
            testMerge("partial left with null",
                      trie,
                      testRanges,
                      deletedRanges(of(29), null));


            testMerge("partial right",
                      trie,
                      testRanges,
                      deletedRanges(of(25), of(27)));
            testMerge("partial right on change",
                      trie,
                      testRanges,
                      deletedRanges(of(25), of(28)));
            testMerge("partial right with null",
                      trie,
                      testRanges,
                      deletedRanges(null, of(22)));

            testMerge("inside range",
                      trie,
                      testRanges,
                      deletedRanges(of(22), of(23)));
            testMerge("inside with change",
                      trie,
                      testRanges,
                      deletedRanges(of(27), of(29)));

            testMerge("empty range inside",
                      trie,
                      testRanges,
                      deletedRanges(of(27), of(27)));

            testMerge("point covered",
                      trie,
                      testRanges,
                      deletedRanges(of(16), of(18)));
            testMerge("point at range start",
                      trie,
                      testRanges,
                      deletedRanges(of(17), of(18)));
            testMerge("point at range end",
                      trie,
                      testRanges,
                      deletedRanges(of(16), of(17)));


            testMerge("start point covered",
                      trie,
                      testRanges,
                      deletedRanges(of(32), of(35)));
            testMerge("start point at range start",
                      trie,
                      testRanges,
                      deletedRanges(of(33), of(35)));
            testMerge("start point at range end",
                      trie,
                      testRanges,
                      deletedRanges(of(32), of(33)));


            testMerge("end point covered",
                      trie,
                      testRanges,
                      deletedRanges(of(36), of(40)));
            testMerge("end point at range start",
                      trie,
                      testRanges,
                      deletedRanges(of(38), of(40)));
            testMerge("end point at range end",
                      trie,
                      testRanges,
                      deletedRanges(of(36), of(38)));
        }
    }

    @Test
    public void testRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            List<DeletionMarker> testRanges = getTestRanges();
            RangeTrie<DeletionMarker> trie = fromList(testRanges);

            testMerge("fully covered ranges",
                      trie,
                      testRanges,
                      deletedRanges(of(20), of(25), of(25), of(33)));
            testMerge("matching ranges",
                      trie,
                      testRanges,
                      deletedRanges(of(21), of(24), of(26), of(31)));
            testMerge("touching empty",
                      trie,
                      testRanges,
                      deletedRanges(of(20), of(21), of(24), of(26), of(32), of(33), of(34), of(36)));
            testMerge("partial left",
                      trie,
                      testRanges,
                      deletedRanges(of(22), of(25), of(29), null));

            testMerge("partial right",
                      trie,
                      testRanges,
                      deletedRanges(null, of(22), of(25), of(27)));

            testMerge("inside ranges",
                      trie,
                      testRanges,
                      deletedRanges(of(22), of(23), of(27), of(29)));

            testMerge("jumping inside",
                      trie,
                      testRanges,
                      deletedRanges(of(21), of(22), of(23), of(24), of(25), of(26), of(27), of(28), of(29), of(30)));
        }
    }

    @Test
    public void testRangeOnSubtrie()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        {
            RangeTrie<DeletionMarker> trie = fromList(getTestRanges());

            // non-overlapping
            testMerge("non-overlapping", trie, getTestRanges(), deletedRanges(of(20), of(23)), deletedRanges(of(24), of(27)));
            // touching, i.e. still non-overlapping
            testMerge("touching", trie, getTestRanges(), deletedRanges(of(20), of(23)), deletedRanges(of(23), of(27)));
            // overlapping 1
            testMerge("overlapping1", trie, getTestRanges(), deletedRanges(of(20), of(23)), deletedRanges(of(22), of(27)));
            // overlapping 2
            testMerge("overlapping2", trie, getTestRanges(), deletedRanges(of(20), of(23)), deletedRanges(of(21), of(27)));
            // covered
            testMerge("covered1", trie, getTestRanges(), deletedRanges(of(20), of(23)), deletedRanges(of(20), of(27)));
            // covered
            testMerge("covered2", trie, getTestRanges(), deletedRanges(of(23), of(27)), deletedRanges(of(20), of(27)));
            // covered 2
            testMerge("covered3", trie, getTestRanges(), deletedRanges(of(21), of(23)), deletedRanges(of(20), of(27)));
        }
    }

    @Test
    public void testRangesOnRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
            testMerges(fromList(getTestRanges()));
    }

    private List<DeletionMarker> getTestRanges()
    {
        return asList(point(17, 20),
                      from(21, 10), pointInside(22, 21, 10), to(24, 10),
                      from(26, 11), change(28, 11, 12).withPoint(22), to(30, 12), 
                      from(33, 13).withPoint(23), to(34, 13),
                      from(36, 14), to(38, 14).withPoint(24));
    }

    private void testMerges(RangeTrie<DeletionMarker> trie)
    {
        testMerge("", trie, getTestRanges());

        List<DeletionMarker> set1 = deletedRanges(null, of(24), of(25), of(29), of(32), null);
        List<DeletionMarker> set2 = deletedRanges(of(14), of(17),
                                              of(22), of(27),
                                              of(28), of(30),
                                              of(32), of(34),
                                              of(36), of(40));
        List<DeletionMarker> set3 = deletedRanges(of(17), of(18),
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

        testMerges(trie, set1, set2, set3);
    }

    private void testMerges(RangeTrie<DeletionMarker> trie, List<DeletionMarker> set1, List<DeletionMarker> set2, List<DeletionMarker> set3)
    {
        // set1 = TrieSet.ranges(null, of(24), of(25), of(29), of(32), null);
        // set2 = TrieSet.ranges(of(22), of(27), of(28), of(30), of(32), of(34));
        // set3 = TrieSet.ranges(of(21), of(22), of(23), of(24), of(25), of(26), of(27), of(28), of(29), of(30));
        // from(21, 10), to(24, 10), from(26, 11), change(28, 11, 12), to(30, 12), from(33, 13), to(34, 13)
        List<DeletionMarker> testRanges = getTestRanges();
        testMerge("1", trie, testRanges, set1);

        testMerge("2", trie, testRanges, set2);

        testMerge("3", trie, testRanges, set3);

        testMerge("12", trie, testRanges, set1, set2);

        testMerge("13", trie, testRanges, set1, set3);

        testMerge("23", trie, testRanges, set2, set3);

        testMerge("123", trie, testRanges, set1, set2, set3);
    }

    public void testMerge(String message, RangeTrie<DeletionMarker> trie, List<DeletionMarker> merged, List<DeletionMarker>... sets)
    {
        System.out.println("Markers: " + merged);
        verify(merged);
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            try
            {
                assertEquals(message + " forward b" + bits, merged, toList(trie));
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
                List<DeletionMarker> ranges = sets[toRemove];
                System.out.println("Adding:  " + ranges);
                testMerge(message + " " + toRemove,
                          trie.mergeWith(fromList(ranges), this::combineForMerge),
                          mergeLists(merged, ranges),
                          Arrays.stream(sets)
                                .filter(x -> x != ranges)
                                .toArray(List[]::new)
                );
            }
        }
    }

    int delete(int deletionTime, int data)
    {
        if (data <= deletionTime)
            return -1;
        else
            return data;
    }

    DeletionMarker delete(int deletionTime, DeletionMarker marker)
    {
        if (deletionTime < 0)
            return marker;

        int newLeft = delete(deletionTime, marker.leftSide);
        int newAt = delete(deletionTime, marker.at);
        int newRight = delete(deletionTime, marker.rightSide);
        if (newLeft < 0 && newAt < 0 && newRight < 0 || newAt == newLeft && newLeft == newRight)
            return null;
        if (newLeft == marker.leftSide && newAt == marker.at && newRight == marker.rightSide)
            return marker;
        return new DeletionMarker(marker.position, newLeft, newAt, newRight, marker.isReportableState);
    }

    DeletionMarker combine(DeletionMarker deleter, DeletionMarker marker)
    {
        int newLeft = Math.max(deleter.leftSide, marker.leftSide);
        int newAt = Math.max(deleter.at, marker.at);
        int newRight = Math.max(deleter.rightSide, marker.rightSide);
        if (newLeft < 0 && newAt < 0 && newRight < 0 || newAt == newLeft && newLeft == newRight)
            return null;
        if (newLeft == marker.leftSide && newAt == marker.at && newRight == marker.rightSide)
            return marker;
        return new DeletionMarker(marker.position, newLeft, newAt, newRight, marker.isReportableState);
    }

    DeletionMarker combineForMerge(DeletionMarker m1, boolean atC1, DeletionMarker m2, boolean atC2)
    {
        if (m1 == null)
            return m2;
        if (m2 == null)
            return m1;
        if (!atC1 && m1.isReportableState)
        {
            m1 = m1.leftSideAsActive();
            if (m1 == null)
                return m2;
        }
        if (!atC2 && m2.isReportableState)
        {
            m2 = m2.leftSideAsActive();
            if (m2 == null)
                return m1;
        }
        int newLeft = Math.max(m1.leftSide, m2.leftSide);
        int newAt = Math.max(m1.at, m2.at);
        int newRight = Math.max(m1.rightSide, m2.rightSide);
        if (newLeft < 0 && newAt < 0 && newRight < 0)
            return null;
        boolean reportable = false;
        if (atC1)
            reportable |= newLeft == m1.leftSide || newAt == m1.at || newRight == m1.rightSide;
        if (atC2)
            reportable |= newLeft == m2.leftSide || newAt == m2.at || newRight == m2.rightSide;
        reportable &= newAt != newLeft || newLeft != newRight;

        return new DeletionMarker(m2.position, newLeft, newAt, newRight, reportable);
    }


    List<DeletionMarker> mergeLists(List<DeletionMarker> left, List<DeletionMarker> right)
    {
        int active = -1;
        Iterator<DeletionMarker> rightIt = right.iterator();
        DeletionMarker nextRight = rightIt.hasNext() ? rightIt.next() : null;
        List<DeletionMarker> result = new ArrayList<>();
        for (DeletionMarker nextLeft : left)
        {
            while (true)
            {
                int cmp;
                if (nextRight == null)
                    cmp = -1;
                else
                    cmp = ByteComparable.compare(nextLeft.position, nextRight.position, TrieImpl.BYTE_COMPARABLE_VERSION);

                if (cmp < 0)
                {
                    maybeAdd(result, nextRight != null ? delete(nextRight.leftSide, nextLeft) : nextLeft);
                    break;
                }

                if (cmp == 0)
                {
                    DeletionMarker processed = combine(nextRight, nextLeft);
                    maybeAdd(result, processed);
                    nextRight = rightIt.hasNext() ? rightIt.next() : null;
                    break;
                }
                else
                {
                    // Must close active if it becomes covered, and must open active if it is no longer covered.
                    if (active >= 0)
                    {
                        DeletionMarker activeMarker = new DeletionMarker(nextRight.position, active, active, active, true);
                        nextRight = combine(activeMarker, nextRight);
                    }
                    maybeAdd(result, nextRight);
                }

                nextRight = rightIt.hasNext() ? rightIt.next() : null;
            }
            active = nextLeft.rightSide;
        }
        assert active == -1;
        while (nextRight != null)
        {
            maybeAdd(result, delete(active, nextRight));// deletion is not needed (active == -1), do just in case
            nextRight = rightIt.hasNext() ? rightIt.next() : null;
        }
        return result;
    }

    static <T> void maybeAdd(List<T> list, T value)
    {
        if (value == null)
            return;
        list.add(value);
    }
}
