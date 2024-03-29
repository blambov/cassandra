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

import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import com.google.common.collect.Streams;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

interface DataPoint extends DeletionAwareTrie.Deletable
{
    DeletionMarker marker();
    LivePoint live();
    ByteComparable position();

    DataPoint withMarker(DeletionMarker newMarker);
    DataPoint remap(ByteComparable newKey);

    static String toString(ByteComparable position)
    {
        if (position == null)
            return "null";
        return position.byteComparableAsString(TrieImpl.BYTE_COMPARABLE_VERSION);
    }

    static List<DataPoint> verify(List<DataPoint> dataPoints)
    {
        int active = -1;
        ByteComparable prev = null;
        for (DataPoint dp : dataPoints)
        {
            DeletionMarker marker = dp.marker();
            if (marker == null)
                continue;
            assertTrue("Order violation " + toString(prev) + " vs " + toString(marker.position),
                       prev == null || ByteComparable.compare(prev, marker.position, TrieImpl.BYTE_COMPARABLE_VERSION) < 0);
            assertEquals("Range close violation", active, marker.leftSide);
            assertTrue(marker.at != marker.leftSide || marker.at != marker.rightSide);
            prev = marker.position;
            active = marker.rightSide;
        }
        assertEquals(-1, active);
        return dataPoints;
    }

    static DataPoint resolve(LivePoint a, DeletionMarker m)
    {
        if (a == null)
            return m;
        if (m == null)
            return a;
        return new CombinedDataPoint(a, m);
    }

    static DataPoint combine(DataPoint a, DataPoint b)
    {
        LivePoint live = combine(a.live(), b.live(), LivePoint::combine);
        DeletionMarker marker = combine(a.marker(), b.marker(), DeletionMarker::combine);
        return resolve(live, marker);
    }

    static <T> T combine(T a, T b, BiFunction<T, T, T> combiner)
    {
        if (a == null)
            return b;
        if (b == null)
            return a;
        return combiner.apply(a, b);
    }

    DataPoint toContent();

    /**
     * Extract the values of the provided trie into a list.
     */
    static List<DataPoint> toList(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        return Streams.stream(trie.mergedTrie(DataPoint::resolve).entryIterator())
                      .map(en -> en.getValue().remap(en.getKey()))
                      .collect(Collectors.toList());
    }

    /**
     * Extract the values of the provided trie into a list.
     */
    static List<LivePoint> contentOnlyList(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        return Streams.stream(trie.contentOnlyTrie().entryIterator())
                      .map(en -> en.getValue().remap(en.getKey()))
                      .collect(Collectors.toList());
    }

    /**
     * Extract the values of the provided trie into a list.
     */
    static List<DeletionMarker> deletionOnlyList(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        return Streams.stream(trie.deletionOnlyTrie().entryIterator())
                      .map(en -> en.getValue().remap(en.getKey()))
                      .collect(Collectors.toList());
    }

    static DeletionAwareTrie<LivePoint, DeletionMarker> fromList(List<DataPoint> list)
    {
        InMemoryDeletionAwareTrie<DataPoint, LivePoint, DeletionMarker> trie = new InMemoryDeletionAwareTrie<>(BufferType.ON_HEAP);
        try
        {
            // If we put a deletion first, the deletion branch will start at the root which works but isn't interesting
            // enough as a test. So put the live data first.
            for (DataPoint i : list)
            {
                LivePoint live = i.live();
                if (live != null)
                    trie.putRecursive(live.position, live, (ex, n) -> n);
            }
            // If we simply put all deletions with putAlternativeRecursive, we won't get correct branches as they
            // won't always close the intervals they open. Deletions need to be put as ranges instead.
            int active = -1;
            int activeStartedAt = -1;
            for (int i = 0; i < list.size(); ++i)
            {
                DeletionMarker marker = list.get(i).marker();
                if (marker == null || marker.leftSide == marker.rightSide)
                    continue;
                assert marker.leftSide == active;
                if (active != -1)
                {
                    if (marker == null || marker.leftSide == marker.rightSide)
                        continue;

                    DeletionMarker startMarker = list.get(activeStartedAt).marker();
                    assert startMarker != null;
                    trie.putAlternativeRangeRecursive(startMarker.position, startMarker, marker.position, marker, (ex, n) -> n);
                    for (int j = activeStartedAt + 1; j < i; ++j)
                    {
                        DeletionMarker m = list.get(j).marker();
                        if (m == null)
                            continue;
                        // this will end up in the alternative branch for the range
                        trie.putAlternativeRecursive(m.position, m, (ex, n) -> n);
                    }
                }

                active = marker.rightSide;
                activeStartedAt = i;
            }
            for (DataPoint i : list)
            {
                // put single deletions separately to avoid creating invalid structure (deletion branch covering another)
                DeletionMarker marker = i.marker();
                if (marker != null && marker.leftSide == marker.rightSide)
                    trie.putAlternativeRecursive(marker.position, marker, (ex, n) -> n);
            }
        }
        catch (InMemoryTrie.SpaceExhaustedException e)
        {
            throw Throwables.propagate(e);
        }
        return trie;
    }
}
