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
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class TestRangeMarker implements RangeMarker<TestRangeMarker>
{
    final ByteComparable position;
    final int leftSide;
    final int rightSide;

    final int at;
    final boolean isReportableState;

    TestRangeMarker(ByteComparable position, int leftSide, int at, int rightSide, boolean isReportableState)
    {
        this.position = position;
        this.leftSide = leftSide;
        this.rightSide = rightSide;
        this.at = at;
        this.isReportableState = isReportableState;
    }

    static TestRangeMarker combine(TestRangeMarker m1, TestRangeMarker m2)
    {
        int newLeft = Math.max(m1.leftSide, m2.leftSide);
        int newAt = Math.max(m1.at, m2.at);
        int newRight = Math.max(m1.rightSide, m2.rightSide);
        if (newLeft < 0 && newAt < 0 && newRight < 0)
            return null;

        return new TestRangeMarker(m2.position, newLeft, newAt, newRight,
                                   (m1.isReportableState || m2.isReportableState) && (newLeft != newRight || newLeft != newAt));
    }


    public static TestRangeMarker combineCollection(Collection<TestRangeMarker> rangeMarkers)
    {
        int newLeft = -1;
        int newAt = -1;
        int newRight = -1;
        boolean isReportableState = false;
        ByteComparable position = null;
        for (TestRangeMarker marker : rangeMarkers)
        {
            newLeft = Math.max(newLeft, marker.leftSide);
            newAt = Math.max(newAt, marker.at);
            newRight = Math.max(newRight, marker.rightSide);
            position = marker.position;
            isReportableState |= marker.isReportableState;
        }
        if (newLeft < 0 && newAt < 0 && newRight < 0)
            return null;
        isReportableState &= newLeft != newRight || newLeft != newAt;

        return new TestRangeMarker(position, newLeft, newAt, newRight, isReportableState);
    }

    TestRangeMarker withPoint(int value)
    {
        return new TestRangeMarker(position, leftSide, value, rightSide, isReportableState);
    }

//    @Override
//    public boolean equals(Object o)
//    {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//        TestRangeMarker that = (TestRangeMarker) o;
//        return ByteComparable.compare(this.position, that.position, TrieUtil.VERSION) == 0
//               && leftSide == that.leftSide
//               && rightSide == that.rightSide
//               && at == that.at;
//    }

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
               (rightSide >= 0 ? right + rightSide : "") +
               (isReportableState ? "" : " not reportable");
    }

    public String toStringNoPosition()
    {
        boolean hasAt = at >= 0 && at != leftSide && at != rightSide;
        String left = leftSide != at ? "<" : "<=";
        String right = rightSide != at ? "<" : "<=";

        return (leftSide >= 0 ? leftSide + left : "") +
               'X' +
               (hasAt ? "=" + at : "") +
               (rightSide >= 0 ? right + rightSide : "") +
               (isReportableState ? "" : " not reportable");
    }

    @Override
    public TestRangeMarker toContent()
    {
        return isReportableState ? this : null;
    }

    @Override
    public TestRangeMarker precedingState(Direction direction)
    {
        if (leftSide == rightSide && leftSide == at && !isReportableState)
            return this;
        int applicable = direction.select(leftSide, rightSide);
        if (applicable < 0)
            return null;
        return new TestRangeMarker(position, applicable, applicable, applicable, false);
    }

    @Override
    public TestRangeMarker branchState()
    {
        if (leftSide == rightSide && leftSide == at && !isReportableState)
            return this;
        if (!isReportableState || at < 0)
            return null;
        return new TestRangeMarker(position, at, at, at, false);
    }

    @Override
    public TestRangeMarker restrict(boolean applicableBefore, boolean applicableAfter, boolean convertCoveringToReported)
    {
        if ((applicableBefore || leftSide < 0) && (applicableAfter || (rightSide < 0 && at < 0)) && (!convertCoveringToReported || isReportableState))
            return this;
        int newAt = isReportableState || convertCoveringToReported ? at : -1;
        int newLeft = applicableBefore ? leftSide : -1;
        int newRight = applicableAfter ? rightSide : -1;
        if (newAt >= 0 || newLeft >= 0 || newRight >= 0)
            return new TestRangeMarker(position, newLeft, newAt, newRight, isReportableState || convertCoveringToReported);
        else
            return null;
    }

    static String toString(ByteComparable position)
    {
        if (position == null)
            return "null";
        return position.byteComparableAsString(TrieUtil.VERSION);
    }

    static List<TestRangeMarker> verify(List<TestRangeMarker> markers)
    {
        int active = -1;
        ByteComparable prev = null;
        for (TestRangeMarker marker : markers)
        {
            assertTrue("Order violation " + toString(prev) + " vs " + toString(marker.position),
                       prev == null || ByteComparable.compare(prev, marker.position, TrieUtil.VERSION) < 0);
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
    static List<TestRangeMarker> toList(RangeTrie<TestRangeMarker> trie, Direction direction)
    {
        return Streams.stream(trie.entryIterator(direction))
                      .map(en -> remap(en.getValue(), en.getKey()))
                      .collect(Collectors.toList());
    }

    static TestRangeMarker remap(TestRangeMarker dm, ByteComparable newKey)
    {
        return new TestRangeMarker(newKey, dm.leftSide, dm.at, dm.rightSide, dm.isReportableState);
    }

    static Map.Entry<ByteComparable, TestRangeMarker> remap(Map.Entry<ByteComparable, TestRangeMarker> entry)
    {
        return Maps.immutableEntry(entry.getKey(), remap(entry.getValue(), entry.getKey()));
    }

    static InMemoryRangeTrie<TestRangeMarker> fromList(List<TestRangeMarker> list)
    {
        InMemoryRangeTrie<TestRangeMarker> trie = InMemoryRangeTrie.shortLived(TrieUtil.VERSION);
        for (TestRangeMarker i : list)
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
    public boolean equals(Object other)
    {
        if (other == null)
            return false;
        TestRangeMarker otherMarker = (TestRangeMarker) other;
        return otherMarker.leftSide == leftSide && otherMarker.at == at && otherMarker.rightSide == rightSide;
    }
}
