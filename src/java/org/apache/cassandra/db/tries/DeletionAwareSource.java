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

import java.util.function.BiFunction;

import javax.annotation.Nullable;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// A cursor applying deletions to a deletion-aware cursor, where the deletions can be dynamically added.
/// Based on [RangeApplyCursor] and used by [MergeCursor.DeletionAware] to process each source with the deletions of the
/// other. The cursor will present the content of the data trie modified by any applicable/covering range of the
/// deletion trie, and will leave the deletion branches unmodied (allowing the merger to process them).
class DeletionAwareSource<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>> implements DeletionAwareCursor<T, D>
{
    final BiFunction<D, T, T> resolver;
    final Direction direction;
    final DeletionAwareCursor<T, D> data;
    @Nullable RangeCursor<D> deletions;

    boolean atRange;

    DeletionAwareSource(BiFunction<D, T, T> resolver, DeletionAwareCursor<T, D> data)
    {
        this.direction = data.direction();
        this.resolver = resolver;
        this.deletions = null;
        this.data = data;
        assert data.depth() == 0;
        atRange = false;
    }

    DeletionAwareSource(BiFunction<D, T, T> resolver, DeletionAwareCursor<T, D> data, RangeCursor<D> deletions)
    {
        this.direction = data.direction();
        this.resolver = resolver;
        this.deletions = deletions;
        this.data = data;
        assert data.depth() == 0;
        assert deletions == null || deletions.depth() == 0;
        atRange = deletions != null;
    }

    @Override
    public int depth()
    {
        return data.depth();
    }

    @Override
    public int incomingTransition()
    {
        return data.incomingTransition();
    }

    @Override
    public Direction direction()
    {
        return direction;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        assert deletions == null || deletions.byteComparableVersion() == data.byteComparableVersion() :
        "Merging cursors with different byteComparableVersions: " +
        deletions.byteComparableVersion() + " vs " + data.byteComparableVersion();
        return data.byteComparableVersion();
    }

    @Override
    public int advance()
    {
        if (deletions == null)
            return data.advance();

        return maybeSkipRange(atRange ? deletions.advance() : deletions.depth(), data.advance());
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        if (deletions == null)
            return data.skipTo(skipDepth, skipTransition);

        int rangeDepth = deletions.depth();
        int dataDepth = data.depth();
        assert skipDepth <= dataDepth + 1;

        int newDataDepth = data.skipTo(skipDepth, skipTransition);

        // Tricky point: if data and range are at the same depth but different transition and data descends,
        // range should not.
        if (!atRange && rangeDepth == dataDepth && newDataDepth == dataDepth + 1)
            return setAtRangeAndReturnDepth(false, newDataDepth);
        else // otherwise skip range to the new data position if needed
            return maybeSkipRange(rangeDepth, newDataDepth);
    }

    @Override
    public int advanceMultiple(TransitionsReceiver receiver)
    {
        if (deletions == null)
            return data.advanceMultiple(receiver);

        // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
        if (atRange)
            return maybeSkipRange(deletions.advance(), data.advance());
        else // atData only
            return maybeSkipRange(deletions.depth(), data.advanceMultiple(receiver));
    }

    int maybeSkipRange(int rangeDepth, int dataDepth)
    {
        if (rangeDepth < 0)
        {
            deletions = null;
            return setAtRangeAndReturnDepth(false, dataDepth);
        }

        // If data position is at or before the range position, we are good.
        if (rangeDepth < dataDepth)
            return setAtRangeAndReturnDepth(false, dataDepth);

        int dataTrans = data.incomingTransition();
        if (rangeDepth == dataDepth)
        {
            int rangeTrans = deletions.incomingTransition();
            if (direction.le(dataTrans, rangeTrans))
                return setAtRangeAndReturnDepth(dataTrans == rangeTrans, dataDepth);
        }

        // Range cursor is before data cursor. Skip it ahead so that we are positioned on data.
        rangeDepth = deletions.skipTo(dataDepth, dataTrans);
        return setAtRangeAndReturnDepth(rangeDepth == dataDepth && deletions.incomingTransition() == dataTrans,
                                        dataDepth);
    }

    private int setAtRangeAndReturnDepth(boolean atRange, int depth)
    {
        this.atRange = atRange;
        return depth;
    }

    @Override
    public T content()
    {
        T content = data.content();
        if (content == null)
            return null;
        if (deletions == null)
            return content;

        D applicableRange = atRange ? deletions.content() : null;
        if (applicableRange == null)
        {
            applicableRange = deletions.precedingState();
            if (applicableRange == null)
                return content;
        }

        return resolver.apply(applicableRange, content);
    }

    @Override
    public DeletionAwareCursor<T, D> tailCursor(Direction direction)
    {
        if (atRange)
            return new DeletionAwareSource<>(resolver, data.tailCursor(direction), deletions.tailCursor(direction));
        else
            return data.tailCursor(direction);
    }

    @Override
    public RangeCursor<D> deletionBranch()
    {
        // Return unchanged, to be handled by MergeCursor.
        return data.deletionBranch();
    }

    public void addDeletions(RangeCursor<D> deletions)
    {
        assert this.deletions == null;
        this.deletions = deletions;
    }

    public boolean hasDeletions()
    {
        return deletions != null;
    }
}
