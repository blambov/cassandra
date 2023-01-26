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

package org.apache.cassandra.db.compaction;

import java.util.List;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;

public class ShardManagerNoDisks implements ShardManager
{
    final SortedLocalRanges localRanges;

    /**
     * Staring positions for the local token ranges, in covered token range. The last number defines the total token
     * share owned by the node.
     */
    final double[] localRangePositions;

    public ShardManagerNoDisks(SortedLocalRanges localRanges)
    {
        this.localRanges = localRanges;
        double position = 0;
        final List<Splitter.WeightedRange> ranges = localRanges.getRanges();
        localRangePositions = new double[ranges.size()];
        for (int i = 0; i < localRangePositions.length; ++i)
        {
            Range<Token> range = ranges.get(i).range();
            double span = range.left.size(range.right);
            position += span;
            localRangePositions[i] = position;
        }
    }

    @Override
    public double rangeSpanned(Range<Token> tableRange)
    {
        assert !tableRange.isTrulyWrapAround();
        return rangeSizeNonWrapping(tableRange);
    }

    double rangeSizeNonWrapping(Range<Token> tableRange)
    {
        double size = 0;
        for (Splitter.WeightedRange range : localRanges.getRanges())
        {
            Range<Token> ix = range.range().intersectionNonWrapping(tableRange);
            if (ix == null)
                continue;
            size += ix.left.size(ix.right);
        }
        return size;
    }

    @Override
    public double localSpaceCoverage()
    {
        return localRangePositions[localRangePositions.length - 1];
    }

    @Override
    public ShardIterator boundaries(int count)
    {
        return new BoundaryIterator(count);
    }

    public class BoundaryIterator implements ShardIterator
    {
        private final double rangeStep;
        private final int count;
        private int nextShardIndex;
        private int currentRange;
        private Token currentStart;
        private Token currentEnd;   // null for the last shard

        public BoundaryIterator(int count)
        {
            this.count = count;
            rangeStep = localSpaceCoverage() / count;
            currentStart = localRanges.getRanges().get(0).left();
            currentRange = 0;
            nextShardIndex = 1;
            if (nextShardIndex == count)
                currentEnd = null;
            else
                currentEnd = getEndToken(rangeStep * nextShardIndex);
        }

        private Token getEndToken(double toPos)
        {
            double left = currentRange > 0 ? localRangePositions[currentRange - 1] : 0;
            double right = localRangePositions[currentRange];
            while (toPos > right)
            {
                left = right;
                right = localRangePositions[++currentRange];
            }

            final Range<Token> range = localRanges.getRanges().get(currentRange).range();
            return currentStart.getPartitioner().split(range.left, range.right, (toPos - left) / (right - left));
        }

        @Override
        public Token shardStart()
        {
            return currentStart;
        }

        @Override
        public Token shardEnd()
        {
            return currentEnd;
        }

        @Override
        public Range<Token> shardSpan()
        {
            return new Range<>(currentStart, currentEnd != null ? currentEnd : currentStart.minValue());
        }

        @Override
        public double shardSpanSize()
        {
            return rangeStep;
        }

        @Override
        public boolean advanceTo(Token nextToken)
        {
            if (currentEnd == null || nextToken.compareTo(currentEnd) < 0)
                return false;
            do
            {
                currentStart = currentEnd;
                if (++nextShardIndex == count)
                    currentEnd = null;
                else
                    currentEnd = getEndToken(rangeStep * nextShardIndex);
            }
            while (!(currentEnd == null || nextToken.compareTo(currentEnd) < 0));
            return true;
        }

        @Override
        public int count()
        {
            return count;
        }

        @Override
        public double fractionInShard(Range<Token> targetSpan)
        {
            Range<Token> shardSpan = shardSpan();
            Range<Token> covered = targetSpan.intersectionNonWrapping(shardSpan);
            if (covered == null)
                return 0;
            if (covered == targetSpan)
                return 1;
            double inShardSize = covered == shardSpan ? shardSpanSize() : ShardManagerNoDisks.this.rangeSpanned(covered);
            double totalSize = ShardManagerNoDisks.this.rangeSpanned(targetSpan);
            return inShardSize / totalSize;
        }

        @Override
        public double rangeSpanned(PartitionPosition first, PartitionPosition last)
        {
            return ShardManagerNoDisks.this.rangeSpanned(first, last);
        }

        @Override
        public int shardIndex()
        {
            return nextShardIndex - 1;
        }
    }
}
