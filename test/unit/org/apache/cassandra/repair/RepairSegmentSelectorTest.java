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
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.MockSchema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RepairSegmentSelectorTest
{
    static final double EPSILON = 1e-6;

    static int generation = 0;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        MockSchema.cleanup();
    }

    @Test
    public void testAvoidsSplittingBigSSTables()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        List<SSTableReader> sstables = List.of(sstable(cfs, 1, 600, 1200),
                                               sstable(cfs, 601, 1000, 800),
                                               sstable(cfs, 1, 1000, 100),
                                               sstable(cfs, 250, 750, 50));
        Range<Token> range = range(0, 1000);

        // The 2150 bytes need two segments. Splitting at 600, between the two big sstables, only anticompacts the two
        // small ones that span the whole range and its middle.
        RepairSegmentSelector.Segments segments = verify(sstables, range, 1300);
        assertEquals(List.of(range(0, 600), range(600, 1000)), segments.ranges);
        assertEquals(150, segments.anticompactionSize, EPSILON);
        // Splitting in the middle of the token range would also anticompact the biggest sstable.
        assertEquals(1350, RepairSegmentSelector.anticompactionSize(sstables, List.of(range(0, 500), range(500, 1000))), EPSILON);
    }

    @Test
    public void testSplitsSSTablesThatCrossTheRangeForFree()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        // The first sstable crosses the start of the range, so it is anticompacted anyway. Its 2000 bytes inside the
        // range are too many for one segment, and splitting it again costs nothing.
        List<SSTableReader> sstables = List.of(sstable(cfs, 1, 600, 2400),
                                               sstable(cfs, 601, 1000, 600));
        RepairSegmentSelector.Segments segments = verify(sstables, range(100, 1000), 1500);
        assertEquals(List.of(range(100, 475), range(475, 1000)), segments.ranges);
        assertEquals(2400, segments.anticompactionSize, EPSILON);
    }

    @Test
    public void testRangeToTheEndOfTheRing()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        long end = Long.MAX_VALUE;
        List<SSTableReader> sstables = List.of(sstable(cfs, end - 1000, end - 500, 1000),
                                               sstable(cfs, end - 499, end, 1000),
                                               sstable(cfs, end - 1000, end, 100));
        Range<Token> range = new Range<>(token(end - 2000), new Murmur3Partitioner.LongToken(Long.MIN_VALUE));
        RepairSegmentSelector.Segments segments = verify(sstables, range, 1100);
        assertEquals(List.of(new Range<>(token(end - 2000), token(end - 500)),
                             new Range<>(token(end - 500), range.right)),
                     segments.ranges);
        assertEquals(100, segments.anticompactionSize, EPSILON);
    }

    @Test
    public void testRandomLayouts()
    {
        long seed = System.nanoTime();
        Random rand = new Random(seed);
        ColumnFamilyStore cfs = MockSchema.newCFS();
        try
        {
            for (int i = 0; i < 100; ++i)
            {
                List<SSTableReader> sstables = new ArrayList<>();
                int count = 1 + rand.nextInt(40);
                for (int j = 0; j < count; ++j)
                {
                    long first = 1 + rand.nextInt(100000);
                    long last = first + rand.nextInt(rand.nextBoolean() ? 5000 : 100000);
                    sstables.add(sstable(cfs, first, last, 1 + rand.nextInt(100000)));
                }
                Range<Token> range = range(rand.nextInt(50000), 100000 + rand.nextInt(100000));
                double total = sstables.stream().mapToDouble(SSTableReader::onDiskLength).sum();
                double limit = total / (1 + rand.nextInt(20));
                RepairSegmentSelector.Segments segments = verify(sstables, range, limit);

                // The fewest segments of equal token width that are within the limit are never better.
                for (int n = segments.ranges.size(); n <= 4 * segments.ranges.size(); ++n)
                {
                    List<Range<Token>> uniform = uniformSegments(range, n);
                    if (uniform.stream().allMatch(segment -> sizeIn(sstables, segment) <= limit))
                    {
                        assertTrue(segments.anticompactionSize <= RepairSegmentSelector.anticompactionSize(sstables, uniform) + EPSILON);
                        break;
                    }
                }
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError("Failed with seed " + seed, t);
        }
    }


    @Test
    public void scratchDemo() throws Exception
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        long r = 1L << 40;
        Range<Token> range = range(0, r);

        // UCS-like: four flushes covering everything, and levels of 8, 32 and 128 aligned shards of 128 MB each
        List<SSTableReader> ucs = new ArrayList<>();
        for (int i = 0; i < 4; ++i)
            ucs.add(sstable(cfs, 1, r, 64));
        for (int shards : new int[]{ 8, 32, 128 })
            for (int j = 0; j < shards; ++j)
                ucs.add(sstable(cfs, r / shards * j + 1, r / shards * (j + 1), 128));
        demo("UCS-like", ucs, range, new double[]{ 1024, 2048, 4096, 8192 });

        // LCS-like: four 32 MB flushes covering everything, and levels of 10, 100 and 1000 sstables of 160 MB each,
        // with unrelated boundaries
        Random rand = new Random(1);
        List<SSTableReader> lcs = new ArrayList<>();
        for (int i = 0; i < 4; ++i)
            lcs.add(sstable(cfs, 1, r, 32));
        for (int count : new int[]{ 10, 100, 1000 })
        {
            long[] cuts = new long[count + 1];
            cuts[count] = r;
            for (int j = 1; j < count; ++j)
                cuts[j] = 1 + (long) (rand.nextDouble() * (r - 2));
            java.util.Arrays.sort(cuts);
            for (int j = 0; j < count; ++j)
                lcs.add(sstable(cfs, cuts[j] + 1, cuts[j + 1], 160));
        }
        demo("LCS-like", lcs, range, new double[]{ 1024, 4096, 16384, 65536 });
    }

    static void demo(String name, List<SSTableReader> sstables, Range<Token> range, double[] limits) throws Exception
    {
        double total = sizeIn(sstables, range);
        for (double limit : limits)
        {
            for (int warmup = 0; warmup < 5; ++warmup)
                RepairSegmentSelector.select(sstables, range, limit);
            long start = System.nanoTime();
            RepairSegmentSelector.Segments segments = RepairSegmentSelector.select(sstables, range, limit);
            double ms = (System.nanoTime() - start) / 1e6;
            double max = segments.sizes.stream().mapToDouble(x -> x).max().getAsDouble();
            List<Range<Token>> uniform = uniformSegments(range, segments.ranges.size());
            double uniformMax = uniform.stream().mapToDouble(segment -> sizeIn(sstables, segment)).max().getAsDouble();
            int feasible = segments.ranges.size();
            while (uniformSegments(range, feasible).stream().anyMatch(segment -> sizeIn(sstables, segment) > limit))
                ++feasible;
            String line = String.format("DEMO %-8s %5d sstables %7.0f MB limit %6.0f MB | optimized %4d segments max %6.0f MB anticompaction %7.0f MB in %6.2f ms" +
                                        " | uniform %4d segments max %6.0f MB anticompaction %7.0f MB | uniform within limit %4d segments anticompaction %7.0f MB",
                                        name, sstables.size(), total, limit, segments.ranges.size(), max, segments.anticompactionSize, ms,
                                        uniform.size(), uniformMax, RepairSegmentSelector.anticompactionSize(sstables, uniform),
                                        feasible, RepairSegmentSelector.anticompactionSize(sstables, uniformSegments(range, feasible)));
            java.nio.file.Files.writeString(java.nio.file.Path.of("/tmp/claude-1000/-home-blambov-git-cassandra5/6462ccbb-f40e-4926-aac8-78efd1db82c0/scratchpad/demo.txt"),
                                            line + "\n", java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND);
        }
    }

    /// Choose the segments, and check that they cover the range in order, are within the limit, and that their sizes and
    /// anticompaction size match what an independent count gives.
    static RepairSegmentSelector.Segments verify(List<SSTableReader> sstables, Range<Token> range, double limit)
    {
        RepairSegmentSelector.Segments segments = RepairSegmentSelector.select(sstables, range, limit);
        assertEquals(segments.ranges.size(), segments.sizes.size());
        assertEquals(range.left, segments.ranges.get(0).left);
        assertEquals(range.right, segments.ranges.get(segments.ranges.size() - 1).right);
        double total = 0;
        for (int i = 0; i < segments.ranges.size(); ++i)
        {
            Range<Token> segment = segments.ranges.get(i);
            if (i > 0)
                assertEquals(segments.ranges.get(i - 1).right, segment.left);
            // A segment can only be over the limit if it is a single token, which can't be split.
            assertTrue("Segment " + segment + " with size " + segments.sizes.get(i) + " over the limit " + limit,
                       segments.sizes.get(i) <= limit + EPSILON || width(segment) == 1);
            total += segments.sizes.get(i);
        }
        assertEquals(sizeIn(sstables, range), total, 1e-6 * Math.max(1, total));
        assertEquals(RepairSegmentSelector.anticompactionSize(sstables, segments.ranges), segments.anticompactionSize, EPSILON);
        return segments;
    }

    /// The size of the data of the given sstables in the range, assuming it is spread uniformly over their tokens.
    static double sizeIn(List<SSTableReader> sstables, Range<Token> range)
    {
        long left = range.left.getLongValue();
        long right = range.right.isMinimum() ? Long.MAX_VALUE : range.right.getLongValue();
        double size = 0;
        for (SSTableReader sstable : sstables)
        {
            // An sstable covers the tokens from its first to its last, i.e. (first - 1, last].
            long start = sstable.getFirst().getToken().getLongValue() - 1;
            long end = sstable.getLast().getToken().getLongValue();
            long overlap = Math.min(end, right) - Math.max(start, left);
            if (overlap > 0)
                size += sstable.onDiskLength() * (double) overlap / (end - start);
        }
        return size;
    }

    static long width(Range<Token> range)
    {
        long right = range.right.isMinimum() ? Long.MAX_VALUE : range.right.getLongValue();
        return right - range.left.getLongValue();
    }

    static List<Range<Token>> uniformSegments(Range<Token> range, int count)
    {
        List<Range<Token>> segments = new ArrayList<>();
        Token start = range.left;
        for (int i = 1; i <= count; ++i)
        {
            Token end = i == count ? range.right : range.left.getPartitioner().split(range.left, range.right, (double) i / count);
            segments.add(new Range<>(start, end));
            start = end;
        }
        return segments;
    }

    static SSTableReader sstable(ColumnFamilyStore cfs, long first, long last, int size)
    {
        return MockSchema.sstable(++generation, size, true, first, last, cfs);
    }

    static Token token(long value)
    {
        return new Murmur3Partitioner.LongToken(value);
    }

    static Range<Token> range(long left, long right)
    {
        return new Range<>(token(left), token(right));
    }
}
