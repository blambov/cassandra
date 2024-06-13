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
import java.util.stream.Collectors;

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static java.util.Arrays.asList;
import static org.apache.cassandra.db.tries.DataPoint.contentOnlyList;
import static org.apache.cassandra.db.tries.DataPoint.deletionOnlyList;
import static org.apache.cassandra.db.tries.DataPoint.fromList;
import static org.apache.cassandra.db.tries.DataPoint.toList;
import static org.apache.cassandra.db.tries.DataPoint.verify;
import static org.junit.Assert.assertEquals;

public class DeletionAwareMergeTest
{
    static final int bitsNeeded = 6;
    int bits = bitsNeeded;
    int deletionPoint = 100;

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

    private DeletionMarker deletedPoint(int where, int value)
    {
        return deletedPointInside(where, value, -1);
    }

    private DeletionMarker deletedPointInside(int where, int value, int active)
    {
        return new DeletionMarker(of(where), active, value, active);
    }

    private DataPoint livePoint(int where, int timestamp)
    {
        return new LivePoint(of(where), timestamp);
    }

    private List<DataPoint> deletedRanges(ByteComparable... dataPoints)
    {
        List<ByteComparable> data = new ArrayList<>(asList(dataPoints));
        invertDataRangeList(data);
        filterOutEmptyRepetitions(data);

        List<DataPoint> markers = new ArrayList<>();
        for (int i = 0; i < data.size(); ++i)
        {
            ByteComparable pos = data.get(i);
            if (pos == null)
                pos = i % 2 == 0 ? of(0) : of((1<<bitsNeeded) - 1);
            if (i % 2 == 0)
                markers.add(new DeletionMarker(pos, -1, deletionPoint, deletionPoint));
            else
                markers.add(new DeletionMarker(pos, deletionPoint, -1, -1));
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
        for (deletionPoint = 4; deletionPoint <= 40; deletionPoint += 9)
        {
            testMerge("no merge");

            testMerge("all",
                      deletedRanges(null, null));
            testMerge("fully covered range",
                      deletedRanges(of(20), of(25)));
            testMerge("fully covered range",
                      deletedRanges(of(25), of(33)));
            testMerge("matching range",
                      deletedRanges(of(21), of(24)));
            testMerge("touching empty",
                      deletedRanges(of(24), of(26)));

            testMerge("partial left",
                      deletedRanges(of(22), of(25)));
            testMerge("partial left on change",
                      deletedRanges(of(28), of(32)));
            testMerge("partial left with null",
                      deletedRanges(of(29), null));


            testMerge("partial right",
                      deletedRanges(of(25), of(27)));
            testMerge("partial right on change",
                      deletedRanges(of(25), of(28)));
            testMerge("partial right with null",
                      deletedRanges(null, of(22)));

            testMerge("inside range",
                      deletedRanges(of(22), of(23)));
            testMerge("inside with change",
                      deletedRanges(of(27), of(29)));

            testMerge("empty range inside",
                      deletedRanges(of(27), of(27)));

            testMerge("point covered",
                      deletedRanges(of(16), of(18)));
            testMerge("point at range start",
                      deletedRanges(of(17), of(18)));
            testMerge("point at range end",
                      deletedRanges(of(16), of(17)));


            testMerge("start point covered",
                      deletedRanges(of(32), of(35)));
            testMerge("start point at range start",
                      deletedRanges(of(33), of(35)));
            testMerge("start point at range end",
                      deletedRanges(of(32), of(33)));


            testMerge("end point covered",
                      deletedRanges(of(36), of(40)));
            testMerge("end point at range start",
                      deletedRanges(of(38), of(40)));
            testMerge("end point at range end",
                      deletedRanges(of(36), of(38)));
        }
    }

    @Test
    public void testRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        for (deletionPoint = 4; deletionPoint <= 40; deletionPoint += 9)
        {
            testMerge("fully covered ranges",
                      deletedRanges(of(20), of(25), of(25), of(33)));
            testMerge("matching ranges",
                      deletedRanges(of(21), of(24), of(26), of(31)));
            testMerge("touching empty",
                      deletedRanges(of(20), of(21), of(24), of(26), of(32), of(33), of(34), of(36)));
            testMerge("partial left",
                      deletedRanges(of(22), of(25), of(29), null));

            testMerge("partial right",
                      deletedRanges(null, of(22), of(25), of(27)));

            testMerge("inside ranges",
                      deletedRanges(of(22), of(23), of(27), of(29)));

            testMerge("jumping inside",
                      deletedRanges(of(21), of(22), of(23), of(24), of(25), of(26), of(27), of(28), of(29), of(30)));
        }
    }

    @Test
    public void testRangeOnSubtrie()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        for (deletionPoint = 4; deletionPoint <= 40; deletionPoint += 9)
        {
            // non-overlapping
            testMerge("non-overlapping", deletedRanges(of(20), of(23)), deletedRanges(of(24), of(27)));
            // touching, i.e. still non-overlapping
            testMerge("touching", deletedRanges(of(20), of(23)), deletedRanges(of(23), of(27)));
            // overlapping 1
            testMerge("overlapping1", deletedRanges(of(20), of(23)), deletedRanges(of(22), of(27)));
            // overlapping 2
            testMerge("overlapping2", deletedRanges(of(20), of(23)), deletedRanges(of(21), of(27)));
            // covered
            testMerge("covered1", deletedRanges(of(20), of(23)), deletedRanges(of(20), of(27)));
            // covered 2
            testMerge("covered2", deletedRanges(of(23), of(27)), deletedRanges(of(20), of(27)));
            // covered 3
            testMerge("covered3", deletedRanges(of(21), of(23)), deletedRanges(of(20), of(27)));
        }
    }

    @Test
    public void testRangesOnRanges()
    {
        for (bits = bitsNeeded; bits > 0; --bits)
        for (deletionPoint = 4; deletionPoint <= 40; deletionPoint += 9)
            testMerges();
    }

    private List<DataPoint> getTestRanges()
    {
        return asList(deletedPoint(17, 20),
                      livePoint(19, 30),
                      from(21, 10), deletedPointInside(22, 21, 10), livePoint(23, 31), to(24, 10),
                      from(26, 11), livePoint(27, 32), change(28, 11, 12).withPoint(22), livePoint(29, 33), to(30, 12),
                      livePoint(32, 34), from(33, 13).withPoint(23), to(34, 13),
                      from(36, 14), to(38, 14).withPoint(24), livePoint(39, 35));
    }

    private void testMerges()
    {
        testMerge("", fromList(getTestRanges()), getTestRanges());

        List<DataPoint> set1 = deletedRanges(null, of(24), of(25), of(29), of(32), null);
        List<DataPoint> set2 = deletedRanges(of(14), of(17),
                                               of(22), of(27),
                                               of(28), of(30),
                                               of(32), of(34),
                                               of(36), of(40));
        List<DataPoint> set3 = deletedRanges(of(17), of(18),
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

        testMerges(set1, set2, set3);
    }

    private void testMerges(List<DataPoint> set1, List<DataPoint> set2, List<DataPoint> set3)
    {
        // set1 = TrieSet.ranges(null, of(24), of(25), of(29), of(32), null);
        // set2 = TrieSet.ranges(of(22), of(27), of(28), of(30), of(32), of(34));
        // set3 = TrieSet.ranges(of(21), of(22), of(23), of(24), of(25), of(26), of(27), of(28), of(29), of(30));
        // from(21, 10), to(24, 10), from(26, 11), change(28, 11, 12), to(30, 12), from(33, 13), to(34, 13)
        testMerge("1", set1);

        testMerge("2", set2);

        testMerge("3", set3);

        testMerge("12", set1, set2);

        testMerge("13", set1, set3);

        testMerge("23", set2, set3);

        testMerge("123", set1, set2, set3);
    }

    @SafeVarargs
    public final void testMerge(String message, List<DataPoint>... sets)
    {
        List<DataPoint> testRanges = getTestRanges();
        testMerge(message, fromList(testRanges), testRanges, sets);
        testCollectionMerge(message + " collection", Lists.newArrayList(fromList(testRanges)), testRanges, sets);
        testMergeInMemoryTrie(message + " inmem.apply", moveDeletionBranchToRoot(fromList(testRanges)), testRanges, sets);
    }


    public void testMerge(String message, DeletionAwareTrie<LivePoint, DeletionMarker> trie, List<DataPoint> merged, List<DataPoint>... sets)
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
                List<DataPoint> ranges = sets[toRemove];
                System.out.println("Adding:  " + ranges);
                testMerge(message + " " + toRemove,
                          trie.mergeWith(fromList(ranges), LivePoint::combine, DeletionMarker::combine, DeletionMarker::delete),
                          mergeLists(merged, ranges),
                          Arrays.stream(sets)
                                .filter(x -> x != ranges)
                                .toArray(List[]::new)
                );
            }
        }
    }

    public void testCollectionMerge(String message, List<DeletionAwareTrie<LivePoint, DeletionMarker>> triesToMerge, List<DataPoint> merged, List<DataPoint>... sets)
    {
        System.out.println("Markers: " + merged);
        verify(merged);
        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
        // Checks both forward and reverse iteration direction.
        if (sets.length == 0)
        {
            DeletionAwareTrie<LivePoint, DeletionMarker> trie = DeletionAwareTrie.merge(triesToMerge,
                                                                                        LivePoint::combineCollection,
                                                                                        DeletionMarker::combineCollection,
                                                                                        DeletionMarker::delete);
            try
            {
                String msg = message + " forward b" + bits;
                assertEquals(msg + " live",
                             merged.stream().map(DataPoint::live).filter(Predicates.notNull()).collect(Collectors.toList()),
                             contentOnlyList(trie));
                assertEquals(msg + " deletions",
                             merged.stream().map(DataPoint::marker).filter(Predicates.notNull()).collect(Collectors.toList()),
                             deletionOnlyList(trie));
                assertEquals(msg, merged, toList(trie));
                System.out.println(msg + " matched.");
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
                List<DataPoint> ranges = sets[toRemove];
                System.out.println("Adding:  " + ranges);
                triesToMerge.add(fromList(ranges));
                testCollectionMerge(message + " " + toRemove,
                                    triesToMerge,
                                    mergeLists(merged, ranges),
                                    Arrays.stream(sets)
                                          .filter(x -> x != ranges)
                                          .toArray(List[]::new)
                );
                triesToMerge.remove(triesToMerge.size() - 1);
            }
        }
    }

    public void testMergeInMemoryTrie(String message, DeletionAwareTrie<LivePoint, DeletionMarker> trie, List<DataPoint> merged, List<DataPoint>... sets)
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
            try
            {
                for (int toRemove = 0; toRemove < sets.length; ++toRemove)
                {
                    List<DataPoint> ranges = sets[toRemove];
                    System.out.println("Adding:  " + ranges);
                    var dupe = duplicateTrie(trie);
                    dupe.apply(moveDeletionBranchToRoot(fromList(ranges)),
                               DeletionAwareMergeTest::combineLive,
                               DeletionAwareMergeTest::combineDeletion,
                               DeletionAwareMergeTest::deleteLive);
                    testMerge(message + " " + toRemove,
                              dupe,
                              mergeLists(merged, ranges),
                              Arrays.stream(sets)
                                    .filter(x -> x != ranges)
                                    .toArray(List[]::new)
                    );
                }
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
        }
    }

    private DeletionAwareTrie<LivePoint, DeletionMarker> moveDeletionBranchToRoot(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        // Because the in-memory trie can't resolve overlapping deletion branches, move the argument's deletion branch to the root
        RangeTrieImpl<DeletionMarker> deletions = RangeTrieImpl.impl(trie.deletionOnlyTrie());
        TrieImpl<LivePoint> lives = TrieImpl.impl(trie.contentOnlyTrie());
        return (DeletionAwareTrieWithImpl<LivePoint, DeletionMarker>) dir -> new DeletionAwareTrieImpl.Cursor<>()
        {
            TrieImpl.Cursor<LivePoint> liveCursor = lives.cursor(dir);

            @Override
            public RangeTrieImpl.Cursor<DeletionMarker> deletionBranch()
            {
                return depth() == 0 ? deletions.cursor(dir) : null;
            }

            @Override
            public DeletionAwareTrieImpl.Cursor<LivePoint, DeletionMarker> duplicate()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public LivePoint content()
            {
                return liveCursor.content();
            }

            @Override
            public int depth()
            {
                return liveCursor.depth();
            }

            @Override
            public int incomingTransition()
            {
                return liveCursor.incomingTransition();
            }

            @Override
            public int advance()
            {
                return liveCursor.advance();
            }

            @Override
            public int skipTo(int skipDepth, int skipTransition)
            {
                return liveCursor.skipTo(skipDepth, skipTransition);
            }
        };
    }

    InMemoryDeletionAwareTrie<DataPoint, LivePoint, DeletionMarker> duplicateTrie(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        try
        {
            InMemoryDeletionAwareTrie<DataPoint, LivePoint, DeletionMarker> copy = InMemoryDeletionAwareTrie.shortLived();
            copy.apply(trie, DeletionAwareMergeTest::combineLive, DeletionAwareMergeTest::combineDeletion, DeletionAwareMergeTest::deleteLive);
            return copy;
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
    }

    static LivePoint combineLive(LivePoint a, LivePoint b)
    {
        if (a == null)
            return b;
        if (b == null)
            return a;
        return LivePoint.combine(a, b);
    }

    static DeletionMarker combineDeletion(DeletionMarker a, DeletionMarker b)
    {
        if (a == null)
            return b;
        if (b == null)
            return a;
        return DeletionMarker.combine(a, b);
    }

    static LivePoint deleteLive(LivePoint live, DeletionMarker deletion)
    {
        if (deletion == null || live == null)
            return live;
        return deletion.delete(live);
    }

    DeletionMarker delete(int deletionTime, DeletionMarker marker)
    {
        if (deletionTime < 0 || marker == null)
            return marker;

        int newLeft = Math.max(deletionTime, marker.leftSide);
        int newAt = Math.max(deletionTime, marker.at);
        int newRight = Math.max(deletionTime, marker.rightSide);
        if (newLeft < 0 && newAt < 0 && newRight < 0 || newAt == newLeft && newLeft == newRight)
            return null;
        if (newLeft == marker.leftSide && newAt == marker.at && newRight == marker.rightSide)
            return marker;
        return new DeletionMarker(marker.position, newLeft, newAt, newRight);
    }

    LivePoint delete(int deletionTime, LivePoint marker)
    {
        if (deletionTime < 0 || marker == null)
            return marker;
        return marker.delete(deletionTime);
    }

    DataPoint delete(int deletionTime, DataPoint marker)
    {
        LivePoint live = delete(deletionTime, marker.live());
        DeletionMarker deletion = delete(deletionTime, marker.marker());
        return DataPoint.resolve(live, deletion);
    }

    int leftSide(DataPoint point)
    {
        if (point.marker() == null)
            return -1;
        return point.marker().leftSide;
    }

    int rightSide(DataPoint point)
    {
        if (point.marker() == null)
            return -1;
        return point.marker().rightSide;
    }

    List<DataPoint> mergeLists(List<DataPoint> left, List<DataPoint> right)
    {
        int active = -1;
        Iterator<DataPoint> rightIt = right.iterator();
        DataPoint nextRight = rightIt.hasNext() ? rightIt.next() : null;
        List<DataPoint> result = new ArrayList<>();
        for (DataPoint nextLeft : left)
        {
            while (true)
            {
                int cmp;
                if (nextRight == null)
                    cmp = -1;
                else
                    cmp = ByteComparable.compare(nextLeft.position(), nextRight.position(), TrieImpl.BYTE_COMPARABLE_VERSION);

                if (cmp < 0)
                {
                    maybeAdd(result, nextRight != null ? delete(leftSide(nextRight), nextLeft) : nextLeft);
                    break;
                }

                if (cmp == 0)
                {
                    if (nextLeft.marker() == null)
                        nextRight = delete(active, nextRight);
                    if (nextRight != null)
                        maybeAdd(result, DataPoint.combine(nextRight, nextLeft).toContent());
                    else
                        maybeAdd(result, nextLeft);

                    nextRight = rightIt.hasNext() ? rightIt.next() : null;
                    break;
                }
                else
                {
                    // Must close active if it becomes covered, and must open active if it is no longer covered.
                    maybeAdd(result, delete(active, nextRight));
                }

                nextRight = rightIt.hasNext() ? rightIt.next() : null;
            }
            if (nextLeft.marker() != null)
                active = nextLeft.marker().rightSide;
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
