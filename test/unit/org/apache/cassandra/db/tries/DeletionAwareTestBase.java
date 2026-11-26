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
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.quicktheories.core.Gen;

import static org.apache.cassandra.db.tries.DataPoint.contentOnlyList;
import static org.apache.cassandra.db.tries.DataPoint.deletionOnlyList;
import static org.apache.cassandra.db.tries.DataPoint.toList;
import static org.apache.cassandra.db.tries.DataPoint.verify;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.lists;

public class DeletionAwareTestBase
{
    /// Change to true to pring debug info
    static final boolean VERBOSE = false;

    static final int bitsNeeded = 6;

    @Parameterized.Parameters(name = "bits per transition {0}")
    public static List<Object> data()
    {
        return IntStream.rangeClosed(1, bitsNeeded)
                        .boxed()
                        .collect(Collectors.toList());
    }

    @Parameterized.Parameter(0)
    public int bits = bitsNeeded;

    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    private static String toString(ByteComparable ranges)
    {
        if (ranges == null)
            return "null";
        return ranges.byteComparableAsString(TrieUtil.VERSION);
    }

    private static DeletionMarker makeActiveMarker(int active, int rangeIndex, ByteComparable nextRange)
    {
        if (active >= 0) // cmp > 0, must covert active to marker
        {
            if ((rangeIndex & 1) != 0)
                return new DeletionMarker(nextRange, active, -1);
            else
                return new DeletionMarker(nextRange, -1, active);
        }
        return null;
    }

    protected static void assertDeletionAwareEqual(String msg, List<DataPoint> merged, DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        try
        {
            assertEquals(msg, merged, toList(trie));
            assertEquals(msg + " live",
                         merged.stream().map(DataPoint::live).filter(x -> x != null).collect(Collectors.toList()),
                         contentOnlyList(trie));
            assertEquals(msg + " deletions",
                         merged.stream().map(DataPoint::marker).filter(x -> x != null).collect(Collectors.toList()),
                         deletionOnlyList(trie));
            // DeletionMarker.equals ignores the position, so compare the printed form too, which includes the key.
            assertEquals(msg + " keys",
                         merged.stream().map(DataPoint::toString).collect(Collectors.toList()),
                         toList(trie).stream().map(DataPoint::toString).collect(Collectors.toList()));
            assertEquals(msg + " deletion keys",
                         merged.stream().map(DataPoint::marker).filter(x -> x != null).map(DeletionMarker::toString).collect(Collectors.toList()),
                         deletionOnlyList(trie).stream().map(DeletionMarker::toString).collect(Collectors.toList()));
            System.out.println(msg + " matched.");
        }
        catch (AssertionError e)
        {
            System.out.println();
            DataPoint.dumpDeletionAwareTrie(trie);
            throw e;
        }
    }

    static <T> void maybeAdd(List<T> list, T value)
    {
        if (value == null)
            return;
        list.add(value);
    }

    /// Creates a [ByteComparable] for the provided value by splitting the integer in sequences of "bits" bits.
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

    ByteComparable at(int value)
    {
        return of(value, ByteSource.TERMINATOR);
    }

    ByteComparable before(int value)
    {
        return of(value, ByteSource.LT_NEXT_COMPONENT);
    }

    ByteComparable after(int value)
    {
        return of(value, ByteSource.GT_NEXT_COMPONENT);
    }

    DeletionMarker from(int where, int value)
    {
        return new DeletionMarker(before(where), -1, value);
    }

    DeletionMarker to(int where, int value)
    {
        return new DeletionMarker(before(where), value, -1);
    }

    DeletionMarker change(int where, int from, int to)
    {
        return new DeletionMarker(before(where), from, to);
    }

    DeletionMarker[] deletedPoint(int where, int value)
    {
        return deletedPointInside(where, value, -1);
    }

    DeletionMarker[] deletedPointInside(int where, int value, int active)
    {
        return new DeletionMarker[]
               {
               new DeletionMarker(before(where), active, value),
               new DeletionMarker(after(where), value, active)
               };
    }

    DataPoint livePoint(int where, int timestamp)
    {
        return new LivePoint(at(where), timestamp);
    }

    protected ByteComparable[] array(ByteComparable... data)
    {
        return data;
    }

    protected List<DataPoint> flatten(List<Object> pointsOrArrays)
    {
        return pointsOrArrays.stream()
                             .flatMap(x -> x instanceof DataPoint ? Stream.of((DataPoint) x) : Arrays.stream((DeletionMarker[]) x))
                             .collect(Collectors.toList());
    }

    String toString(ByteComparable[] ranges)
    {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < ranges.length; i += 2)
        {
            b.append('[');
            b.append(DeletionAwareTestBase.toString(ranges[i]));
            b.append(';');
            b.append(DeletionAwareTestBase.toString(ranges[i + 1]));
            b.append(')');
        }
        return b.toString();
    }

    List<DataPoint> intersect(List<DataPoint> dataPoints, ByteComparable... ranges)
    {
        int rangeIndex = 0;
        int active = -1;
        ByteComparable nextRange = ranges[0];
        if (nextRange == null)
            nextRange = ++rangeIndex < ranges.length ? ranges[rangeIndex] : null;
        List<DataPoint> result = new ArrayList<>();
        for (DataPoint dp : dataPoints)
        {
            DeletionMarker marker = dp.marker();
            while (true)
            {
                int cmp;
                if (nextRange == null)
                    cmp = -1;
                else
                    cmp = ByteComparable.compare(dp.position(), nextRange, TrieUtil.VERSION);

                if (cmp < 0)
                {
                    if ((rangeIndex & 1) != 0)
                        DeletionAwareTestBase.maybeAdd(result, dp);
                    break;
                }

                if (cmp == 0)
                {
                    DeletionMarker adjustedMarker = marker != null ? marker : DeletionAwareTestBase.makeActiveMarker(active, rangeIndex, nextRange);

                    if ((rangeIndex & 1) == 0)
                        DeletionAwareTestBase.maybeAdd(result, dp.withMarker(startOf(adjustedMarker)));
                    else
                        DeletionAwareTestBase.maybeAdd(result, dp.withMarker(endOf(adjustedMarker)));   // live points are included at starts as well as ends

                    nextRange = ++rangeIndex < ranges.length ? ranges[rangeIndex] : null;
                    break;
                }
                else
                    DeletionAwareTestBase.maybeAdd(result, DeletionAwareTestBase.makeActiveMarker(active, rangeIndex, nextRange));

                nextRange = ++rangeIndex < ranges.length ? ranges[rangeIndex] : null;
            }
            if (marker != null)
                active = marker.rightSide;
        }
        assert active == -1;
        return result;
    }

    DeletionMarker startOf(DeletionMarker marker)
    {
        return marker != null ? marker.restrict(false, true) : null;
    }

    DeletionMarker endOf(DeletionMarker marker)
    {
        return marker != null ? marker.restrict(true, false) : null;
    }

    // Generators of random content for property-based tests.

    static final int MAX_POINTS = 20;
    static final int MAX_VALUE = 63; // Fits in 6 bits (bitsNeeded)
    static final int MAX_TIMESTAMP = 100;

    /// Generator for random live data points.
    /// Creates `LivePoint` instances with random positions and timestamps.
    Gen<LivePoint> livePointGen()
    {
        return integers().between(0, MAX_VALUE)
                        .zip(integers().between(1, MAX_TIMESTAMP),
                             (pos, ts) -> new LivePoint(at(pos), ts));
    }

    /// Generator for random deletion markers.
    /// Creates `DeletionMarker` instances representing deletion ranges.
    /// Generates pairs of markers: one to start deletion, one to end it.
    Gen<List<DataPoint>> deletionMarkerPairGen()
    {
        return integers().between(0, MAX_VALUE)
                        .zip(integers().between(0, MAX_VALUE),
                             integers().between(1, MAX_TIMESTAMP),
                             (start, end, ts) -> {
                                 int left = Math.min(start, end);
                                 int right = Math.max(start, end);
                                 if (left == right)
                                     right = Math.min(right + 1, MAX_VALUE);

                                 List<DataPoint> markers = new ArrayList<>();
                                 markers.add(new DeletionMarker(before(left), -1, ts));
                                 markers.add(new DeletionMarker(before(right), ts, -1));
                                 return markers;
                             });
    }

    /// Generator for random live point lists.
    /// Creates sorted lists mixing `LivePoint` and `DeletionMarker` instances for trie construction.
    Gen<List<DataPoint>> dataPointListGen()
    {
        // Generate a mix of live points and deletion marker pairs
        Gen<DataPoint> liveGen = livePointGen().map(lp -> (DataPoint) lp);
        Gen<List<DataPoint>> deletionGen = deletionMarkerPairGen();

        return lists().of(liveGen)
                     .ofSizeBetween(0, MAX_POINTS / 2)
                     .zip(lists().of(deletionGen)
                                .ofSizeBetween(0, MAX_POINTS / 4),
                          (livePoints, deletionPairs) -> {
                              List<DataPoint> combined = new ArrayList<>(livePoints);
                              for (List<DataPoint> pair : deletionPairs)
                                  combined.addAll(pair);
                              return combined;
                          })
                     .map(this::sortAndValidateDataPoints);
    }

    /// Generator for pairs of data point lists for merge testing.
    Gen<List<List<DataPoint>>> dataPointPairGen()
    {
        return dataPointListGen().zip(dataPointListGen(), Arrays::asList);
    }

    /// Sorts data points by position and ensures they form a valid deletion-aware sequence.
    /// Removes live points that are covered by deletion markers with higher timestamps.
    List<DataPoint> sortAndValidateDataPoints(List<DataPoint> points)
    {
        if (points.isEmpty())
            return points;

        // Sort by position
        List<DataPoint> sorted = points.stream()
                                      .sorted((a, b) -> ByteComparable.compare(a.position(), b.position(), VERSION))
                                      .collect(Collectors.toList());

        // Remove duplicates at same position, keeping the last one
        List<DataPoint> deduplicated = new ArrayList<>();
        DataPoint prev = null;
        for (DataPoint dp : sorted)
        {
            if (prev == null || ByteComparable.compare(prev.position(), dp.position(), VERSION) != 0)
                deduplicated.add(dp);
            else
                deduplicated.set(deduplicated.size() - 1, dp); // Replace with newer
            prev = dp;
        }

        // Remove live points that are covered by deletions with higher timestamps
        // Track active deletion timestamp as we iterate through sorted points
        List<DataPoint> filtered = new ArrayList<>();
        int activeDeletionTimestamp = -1;

        for (DataPoint dp : deduplicated)
        {
            if (dp instanceof DeletionMarker)
            {
                DeletionMarker marker = (DeletionMarker) dp;
                // Update active deletion timestamp based on marker semantics
                // leftSide is timestamp before position, rightSide is timestamp after position
                if (marker.leftSide == -1 && marker.rightSide >= 0)
                    activeDeletionTimestamp = marker.rightSide; // Start of deletion range
                else if (marker.leftSide >= 0 && marker.rightSide == -1)
                    activeDeletionTimestamp = -1; // End of deletion range
                else if (marker.leftSide >= 0 && marker.rightSide >= 0)
                    activeDeletionTimestamp = marker.rightSide; // Timestamp change
                filtered.add(dp);
            }
            else if (dp instanceof LivePoint)
            {
                LivePoint live = (LivePoint) dp;
                // Only include live point if not covered by active deletion with higher timestamp
                if (activeDeletionTimestamp == -1 || live.timestamp > activeDeletionTimestamp)
                    filtered.add(dp);
                // Otherwise skip this live point as it would be deleted
            }
        }

        try
        {
            return verify(filtered);
        }
        catch (AssertionError e)
        {
            // If verification fails, return empty list to avoid invalid test data
            return Collections.emptyList();
        }
    }
}
