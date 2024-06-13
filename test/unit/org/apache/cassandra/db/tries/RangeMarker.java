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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class RangeMarker implements RangeTrie.RangeMarker<RangeMarker>
{
    final ByteComparable position;
    final int leftSide;
    final int rightSide;

    final int at;
    final boolean isReportableState;

    RangeMarker(ByteComparable position, int leftSide, int at, int rightSide)
    {
        this.position = position;
        this.leftSide = leftSide;
        this.rightSide = rightSide;
        this.at = at;
        this.isReportableState = at != leftSide || leftSide != rightSide;
    }

    static RangeMarker combine(RangeMarker m1, RangeMarker m2)
    {
        int newLeft = Math.max(m1.leftSide, m2.leftSide);
        int newAt = Math.max(m1.at, m2.at);
        int newRight = Math.max(m1.rightSide, m2.rightSide);
        if (newLeft < 0 && newAt < 0 && newRight < 0)
            return null;

        return new RangeMarker(m2.position, newLeft, newAt, newRight);
    }


    public static RangeMarker combineCollection(Collection<RangeMarker> rangeMarkers)
    {
        int newLeft = -1;
        int newAt = -1;
        int newRight = -1;
        ByteComparable position = null;
        for (RangeMarker marker : rangeMarkers)
        {
            newLeft = Math.max(newLeft, marker.leftSide);
            newAt = Math.max(newAt, marker.at);
            newRight = Math.max(newRight, marker.rightSide);
            position = marker.position;
        }
        if (newLeft < 0 && newAt < 0 && newRight < 0)
            return null;

        return new RangeMarker(position, newLeft, newAt, newRight);
    }

    RangeMarker withPoint(int value)
    {
        return new RangeMarker(position, leftSide, value, rightSide);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RangeMarker that = (RangeMarker) o;
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
    public RangeMarker toContent()
    {
        return isReportableState ? this : null;
    }

    @Override
    public RangeMarker asCoveringState(Direction direction)
    {
        if (!isReportableState)
            return this;
        int applicable = direction.select(leftSide, rightSide);
        if (applicable < 0)
            return null;
        return new RangeMarker(position, applicable, applicable, applicable);
    }

    @Override
    public RangeMarker asReportablePoint(boolean applicableBefore, boolean applicableAfter)
    {
        if ((applicableBefore || leftSide < 0) && (applicableAfter || (rightSide < 0 && at < 0)))
            return this;
        int newAt = applicableAfter ? at : -1;
        int newLeft = applicableBefore ? leftSide : -1;
        int newRight = applicableAfter ? rightSide : -1;
        if (newAt >= 0 || newLeft >= 0 || newRight >= 0)
            return new RangeMarker(position, newLeft, newAt, newRight);
        else
            return null;
    }

    @Override
    public boolean precedingIncluded(Direction direction)
    {
        return direction.select(leftSide, rightSide) >= 0;
    }

    static String toString(ByteComparable position)
    {
        if (position == null)
            return "null";
        return position.byteComparableAsString(TrieImpl.BYTE_COMPARABLE_VERSION);
    }

    static List<RangeMarker> verify(List<RangeMarker> markers)
    {
        int active = -1;
        ByteComparable prev = null;
        for (RangeMarker marker : markers)
        {
            assertTrue("Order violation " + toString(prev) + " vs " + toString(marker.position),
                       prev == null || ByteComparable.compare(prev, marker.position, TrieImpl.BYTE_COMPARABLE_VERSION) < 0);
            assertEquals("Range close violation", active, marker.leftSide);
            assertTrue(marker.at != marker.leftSide || marker.at != marker.rightSide);
            prev = marker.position;
            active = marker.rightSide;
        }
        assertEquals("Unclosed range", -1, active);
        return markers;
    }


    /**
     * Extract the values of the provided trie into a list.
     */
    static List<RangeMarker> toList(RangeTrie<RangeMarker> trie, Direction direction)
    {
        return Streams.stream(trie.entryIterator(direction))
                      .map(en -> remap(en.getValue(), en.getKey()))
                      .collect(Collectors.toList());
    }

    static RangeMarker remap(RangeMarker dm, ByteComparable newKey)
    {
        return new RangeMarker(newKey, dm.leftSide, dm.at, dm.rightSide);
    }

    static InMemoryRangeTrie<RangeMarker> fromList(List<RangeMarker> list)
    {
        InMemoryRangeTrie<RangeMarker> trie = new InMemoryRangeTrie<>(BufferType.ON_HEAP);
        for (RangeMarker i : list)
        {
            try
            {
                trie.putRecursive(i.position, i, (ex, n) -> n);
            }
            catch (TrieSpaceExhaustedException e)
            {
                throw Throwables.propagate(e);
            }
        }
        return trie;
    }

    @Override
    public boolean agreesWith(RangeMarker other)
    {
        if (other == null)
            return false;
        return other.leftSide == leftSide && other.at == at && other.rightSide == rightSide;
    }
}
