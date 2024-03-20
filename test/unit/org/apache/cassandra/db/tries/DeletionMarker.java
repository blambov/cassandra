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

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import com.google.common.collect.Streams;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class DeletionMarker implements RangeTrie.RangeMarker<DeletionMarker>
{
    final ByteComparable position;
    final int leftSide;
    final int rightSide;

    final int at;
    final boolean isReportableState;

    DeletionMarker(ByteComparable position, int leftSide, int at, int rightSide)
    {
        this.position = position;
        this.leftSide = leftSide;
        this.rightSide = rightSide;
        this.at = at;
        this.isReportableState = at != leftSide || leftSide != rightSide;
    }

    static DeletionMarker combine(DeletionMarker m1, DeletionMarker m2)
    {
        int newLeft = Math.max(m1.leftSide, m2.leftSide);
        int newAt = Math.max(m1.at, m2.at);
        int newRight = Math.max(m1.rightSide, m2.rightSide);
        if (newLeft < 0 && newAt < 0 && newRight < 0)
            return null;
        if (newLeft == newAt && newAt == newRight && (m1.isReportableState || m2.isReportableState))
            return null; // if we are processing content, do not report ineffective markers

        return new DeletionMarker(m2.position, newLeft, newAt, newRight);
    }


    public static DeletionMarker combineCollection(Collection<DeletionMarker> deletionMarkers)
    {
        boolean isReportableState = false;
        int newLeft = -1;
        int newAt = -1;
        int newRight = -1;
        ByteComparable position = null;
        for (DeletionMarker marker : deletionMarkers)
        {
            newLeft = Math.max(newLeft, marker.leftSide);
            newAt = Math.max(newAt, marker.at);
            newRight = Math.max(newRight, marker.rightSide);
            isReportableState |= marker.isReportableState;
            position = marker.position;
        }
        if (newLeft < 0 && newAt < 0 && newRight < 0)
            return null;
        if (newLeft == newAt && newAt == newRight && isReportableState)
            return null; // if we are processing content, do not report ineffective markers

        return new DeletionMarker(position, newLeft, newAt, newRight);
    }

    DeletionMarker withPoint(int value)
    {
        return new DeletionMarker(position, leftSide, value, rightSide);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeletionMarker that = (DeletionMarker) o;
        return ByteComparable.compare(this.position, that.position, TrieImpl.BYTE_COMPARABLE_VERSION) == 0
               && leftSide == that.leftSide
               && rightSide == that.rightSide
               && at == that.at;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(position, leftSide, at, rightSide);
    }

    @Override
    public String toString()
    {
        boolean hasAt = at >= 0 && at != leftSide && at != rightSide;
        String left = leftSide != at ? "<" : "<=";
        String right = rightSide != at ? "<" : "<=";

        return (leftSide >= 0 ? leftSide + left : "") +
               '"' + toString(position) + '"' +
               (hasAt ? "=" + at : "") +
               (rightSide >= 0 ? right + rightSide : "");
    }

    @Override
    public DeletionMarker toContent()
    {
        return isReportableState ? this : null;
    }

    @Override
    public DeletionMarker leftSideAsCovering()
    {
        if (!isReportableState)
            return this;
        if (leftSide < 0)
            return null;
        return new DeletionMarker(position, leftSide, leftSide, leftSide);
    }

    @Override
    public DeletionMarker rightSideAsCovering()
    {
        if (!isReportableState)
            return this;
        if (rightSide < 0)
            return null;
        return new DeletionMarker(position, rightSide, rightSide, rightSide);
    }

    @Override
    public DeletionMarker asReportableStart()
    {
        if (rightSide < 0 && at < 0)
            return null;
        return new DeletionMarker(position, -1, at, rightSide);
    }

    @Override
    public DeletionMarker asReportableEnd()
    {
        if (leftSide < 0)
            return null;
        return new DeletionMarker(position, leftSide, -1, -1);
    }

    @Override
    public boolean lesserIncluded()
    {
        return leftSide >= 0;
    }

    static String toString(ByteComparable position)
    {
        if (position == null)
            return "null";
        return position.byteComparableAsString(TrieImpl.BYTE_COMPARABLE_VERSION);
    }

    static List<DeletionMarker> verify(List<DeletionMarker> markers)
    {
        int active = -1;
        ByteComparable prev = null;
        for (DeletionMarker marker : markers)
        {
            assertTrue("Order violation " + toString(prev) + " vs " + toString(marker.position),
                       prev == null || ByteComparable.compare(prev, marker.position, TrieImpl.BYTE_COMPARABLE_VERSION) < 0);
            assertEquals("Range close violation", active, marker.leftSide);
            assertTrue(marker.at != marker.leftSide || marker.at != marker.rightSide);
            prev = marker.position;
            active = marker.rightSide;
        }
        assertEquals(-1, active);
        return markers;
    }


    /**
     * Extract the values of the provided trie into a list.
     */
    static List<DeletionMarker> toList(RangeTrie<DeletionMarker> trie)
    {
        return Streams.stream(trie.entryIterator())
                      .map(en -> remap(en.getValue(), en.getKey()))
                      .collect(Collectors.toList());
    }

    static DeletionMarker remap(DeletionMarker dm, ByteComparable newKey)
    {
        return new DeletionMarker(newKey, dm.leftSide, dm.at, dm.rightSide);
    }

    static RangeTrie<DeletionMarker> fromList(List<DeletionMarker> list)
    {
        InMemoryRangeTrie<DeletionMarker> trie = new InMemoryRangeTrie<>(BufferType.ON_HEAP);
        for (DeletionMarker i : list)
        {
            try
            {
                trie.putRecursive(i.position, i, (ex, n) -> n);
            }
            catch (InMemoryTrie.SpaceExhaustedException e)
            {
                throw Throwables.propagate(e);
            }
        }
        return trie;
    }
}
