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

package org.apache.cassandra.repair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Comparables;
import org.apache.cassandra.utils.OptimizedSplits;

/// Proof of concept: choose the segments to split an incremental repair of a token range into, so that each segment
/// has at most a given amount of unrepaired data and repairing the segments separately needs as little anticompaction
/// as possible.
///
/// Incremental repair marks the unrepaired sstables that a repaired range fully contains as pending, but has to
/// anticompact, i.e. rewrite, the ones that cross the boundaries of the range. Repairing a range in segments thus also
/// rewrites every sstable that crosses a boundary between segments. This chooses the boundaries with [OptimizedSplits],
/// using the unrepaired sstables as items, their size spread uniformly over their token range as weight, and their
/// size as the cost of splitting them. Sstables that cross the boundaries of the range are anticompacted anyway, so
/// splitting them again costs nothing.
///
/// Limitations of this proof of concept:
/// - It only knows the sstables of the local node, which other replicas' sstables may not match.
/// - An sstable that crosses several boundaries is only charged once, although each repair session after the first
///   rewrites what is left of it.
/// - It assumes that the data of each sstable is spread uniformly over its token range.
public class RepairSegmentSelector
{
    /// The segments chosen for a range.
    public static class Segments
    {
        /// The segments in token order. Together they cover the whole range.
        public final List<Range<Token>> ranges;
        /// The size of the unrepaired data in each segment, in bytes.
        public final List<Double> sizes;
        /// The total size of the sstables that need to be anticompacted when the segments are repaired, i.e. the ones
        /// that cross the boundaries of the range or between segments, in bytes.
        public final double anticompactionSize;

        Segments(List<Range<Token>> ranges, List<Double> sizes, double anticompactionSize)
        {
            this.ranges = ranges;
            this.sizes = sizes;
            this.anticompactionSize = anticompactionSize;
        }
    }

    /// Choose the segments of the given range for the unrepaired sstables of the given tables.
    public static Segments select(Collection<ColumnFamilyStore> tables, Range<Token> range, double maxSegmentSize)
    {
        List<SSTableReader> sstables = new ArrayList<>();
        for (ColumnFamilyStore table : tables)
            sstables.addAll(table.getLiveSSTables());
        return select(sstables, range, maxSegmentSize);
    }

    /// Choose the segments of the given range for the given sstables, of which only the unrepaired ones matter. Each
    /// segment will have at most `maxSegmentSize` bytes of their data, unless the token space can't be split finely
    /// enough.
    public static Segments select(Collection<SSTableReader> sstables, Range<Token> range, double maxSegmentSize)
    {
        if (range.isTrulyWrapAround())
        {
            // Choose the segments of the two parts separately.
            List<Range<Token>> ranges = new ArrayList<>();
            List<Double> sizes = new ArrayList<>();
            double anticompactionSize = 0;
            for (Range<Token> part : range.unwrap())
            {
                Segments segments = select(sstables, part, maxSegmentSize);
                ranges.addAll(segments.ranges);
                sizes.addAll(segments.sizes);
                anticompactionSize += segments.anticompactionSize;
            }
            return new Segments(ranges, sizes, anticompactionSize);
        }

        SSTableSpace space = new SSTableSpace(range);
        List<SSTableReader> unrepaired = new ArrayList<>();
        double crossingSize = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.isRepaired() || sstable.isPendingRepair() || space.startOf(sstable).compareTo(space.endOf(sstable)) >= 0)
                continue;
            unrepaired.add(sstable);
            if (!isContained(sstable, range))
                crossingSize += sstable.onDiskLength();
        }

        OptimizedSplits.Split<Token> solution = OptimizedSplits.optimizedSplits(unrepaired,
                                                                                space,
                                                                                sstable -> isContained(sstable, range) ? sstable.onDiskLength() : 0,
                                                                                maxSegmentSize);
        if (solution == null)
            return new Segments(List.of(range), List.of(0.0), 0);

        List<Range<Token>> ranges = new ArrayList<>();
        List<Double> sizes = new ArrayList<>();
        for (OptimizedSplits.Split<Token> group = solution; group != null; group = group.previous)
        {
            Token start = group.previous != null ? group.previous.pointAfter : range.left;
            Token end = group.pointAfter != null ? group.pointAfter : range.right;
            ranges.add(new Range<>(start, end));
            sizes.add(group.weight);
        }
        Collections.reverse(ranges);
        Collections.reverse(sizes);
        return new Segments(ranges, sizes, crossingSize + solution.accumulatedSplitCost);
    }

    /// The total size of the unrepaired sstables that repairing the given segments separately needs to anticompact,
    /// i.e. the ones that intersect the segments without being contained in one of them.
    public static double anticompactionSize(Collection<SSTableReader> sstables, List<Range<Token>> segments)
    {
        double size = 0;
        for (SSTableReader sstable : sstables)
        {
            if (sstable.isRepaired() || sstable.isPendingRepair() || !sstable.intersects(segments))
                continue;
            boolean contained = false;
            for (Range<Token> segment : segments)
                contained |= isContained(sstable, segment);
            if (!contained)
                size += sstable.onDiskLength();
        }
        return size;
    }

    private static boolean isContained(SSTableReader sstable, Range<Token> range)
    {
        return range.contains(sstable.getFirst().getToken()) && range.contains(sstable.getLast().getToken());
    }

    /// The sstables as spans between the points where a segment can end. A segment `(left, right]` contains the tokens
    /// after `left` up to and including `right`, so ending one at a token splits the sstables that have data at that
    /// token and after it. An sstable thus spans from the token before its first one to its last one, limited to the
    /// range.
    private static class SSTableSpace implements OptimizedSplits.ItemSpace<SSTableReader, Token>
    {
        final Token left;
        final Token right;

        SSTableSpace(Range<Token> range)
        {
            this.left = range.left;
            // A range that ends with the minimum token extends to the end of the ring.
            this.right = range.right.isMinimum() ? range.right.getPartitioner().getMaximumToken() : range.right;
        }

        @Override
        public Token startOf(SSTableReader sstable)
        {
            return Comparables.max(sstable.getFirst().getToken().prevValidToken(), left);
        }

        @Override
        public Token endOf(SSTableReader sstable)
        {
            return Comparables.min(sstable.getLast().getToken(), right);
        }

        @Override
        public double weight(SSTableReader sstable, Token sliceStart, Token sliceEnd)
        {
            Token start = sstable.getFirst().getToken().prevValidToken();
            Token end = sstable.getLast().getToken();
            return sstable.onDiskLength() * sliceStart.size(sliceEnd) / start.size(end);
        }

        @Override
        @Nullable
        public Token split(Token start, Token end, double ratio)
        {
            Token token = start.getPartitioner().split(start, end, ratio);
            return token.compareTo(start) > 0 && token.compareTo(end) < 0 ? token : null;
        }
    }
}
