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
/// other. The cursor will present the content of the live trie modified by any applicable/covering range of the
/// deletion trie, and will leave the deletion branches unmodied (allowing the merger to process them).
class DeletionAwareMergeSource<T, D extends RangeState<D>, E extends RangeState<E>> implements DeletionAwareCursor<T, D>
{
    final BiFunction<E, T, T> resolver;
    final DeletionAwareCursor<T, D> live;
    @Nullable RangeCursor<E> deletions;
    /// The position of the live cursor at which the deletions are attached, and the depth correction for it. The
    /// deletions' positions are converted with [DepthAdjustedCursor#toAdjustedDepth(long, long, long)].
    long deletionsRootPosition;
    long deletionsDepthCorrection;

    boolean atDeletions;

    DeletionAwareMergeSource(BiFunction<E, T, T> resolver, DeletionAwareCursor<T, D> live)
    {
        this(resolver, live, null);
    }

    DeletionAwareMergeSource(BiFunction<E, T, T> resolver, DeletionAwareCursor<T, D> live, RangeCursor<E> deletions)
    {
        this.resolver = resolver;
        this.deletions = deletions;
        this.live = live;
        this.deletionsRootPosition = live.encodedPosition();
        this.deletionsDepthCorrection = 0;
        live.assertFresh();
        if (deletions != null)
            deletions.assertFresh();
        atDeletions = deletions != null;
    }

    @Override
    public void close()
    {
        try
        {
            live.close();
        }
        finally
        {
            if (deletions != null)
                deletions.close();
        }
    }

    @Override
    public long encodedPosition()
    {
        return live.encodedPosition();
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        assert deletions == null || deletions.byteComparableVersion() == live.byteComparableVersion() :
        "Merging cursors with different byteComparableVersions: " +
        deletions.byteComparableVersion() + " vs " + live.byteComparableVersion();
        return live.byteComparableVersion();
    }

    @Override
    public long advance()
    {
        long newLivePosition = live.advance();

        if (deletions == null)
            return newLivePosition;
        else if (atDeletions) // if both cursors were at the same position, always advance the deletions' cursor to catch up.
            return skipDeletionsToLivePosition(newLivePosition);
        else // otherwise skip deletions to the new live position only if it advances past the deletions' current position.
            return maybeSkipDeletions(newLivePosition);
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        long newLivePosition = live.skipTo(encodedSkipPosition);

        if (deletions == null)
            return newLivePosition;
        else if (atDeletions) // if both cursors were at the same position, always advance the deletions' cursor to catch up.
            return skipDeletionsToLivePosition(newLivePosition);
        else // otherwise skip deletions to the new live position only if it advances past the deletions' current position.
            return maybeSkipDeletions(newLivePosition);
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        if (deletions == null)
            return live.advanceMultiple(receiver);

        // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
        if (atDeletions)
            return skipDeletionsToLivePosition(live.advance());
        else // at live only
            return maybeSkipDeletions(live.advanceMultiple(receiver));
    }

    long maybeSkipDeletions(long livePosition)
    {
        long deletionsPosition = DepthAdjustedCursor.toAdjustedDepth(deletions.encodedPosition(), deletionsRootPosition, deletionsDepthCorrection);
        
        // If the live position is at or before the deletions position, we are good.
        long cmp = Cursor.compare(livePosition, deletionsPosition);
        if (cmp <= 0)
            return setAtDeletionsAndReturnPosition(cmp == 0, livePosition);

        // Deletions cursor is before live cursor.
        return skipDeletionsToLivePosition(livePosition);
    }

    private long skipDeletionsToLivePosition(long livePosition)
    {
        // Skip deletions cursor to the live position; if that is beyond the branch's root, no need to skip, just leave it.
        long deletionsSkipPosition = DepthAdjustedCursor.fromAdjustedDepth(livePosition, deletionsRootPosition, deletionsDepthCorrection);
        long deletionsPositionUncorrected = !Cursor.isExhausted(deletionsSkipPosition)
                                            ? deletions.skipTo(deletionsSkipPosition)
                                            : Cursor.exhaustedPosition(deletionsSkipPosition);
        if (Cursor.isExhausted(deletionsPositionUncorrected))
            return leaveDeletionsBranch(livePosition);
        else
            return setAtDeletionsAndReturnPosition(Cursor.compare(deletionsPositionUncorrected, deletionsSkipPosition) == 0,
                                                   livePosition);
    }

    private long leaveDeletionsBranch(long livePosition)
    {
        // The deletions cursor is only attached for the span of a branch; release it as soon as we leave that branch
        // rather than waiting for [#close], as a file-backed one holds a buffer.
        deletions.close();
        deletions = null;
        return setAtDeletionsAndReturnPosition(false, livePosition);
    }

    private long setAtDeletionsAndReturnPosition(boolean atDeletions, long position)
    {
        this.atDeletions = atDeletions;
        return position;
    }

    @Override
    public T content()
    {
        T content = live.content();
        if (content == null)
            return null;
        if (deletions == null)
            return content;

        E applicableDeletions = atDeletions ? deletions.content() : null;
        if (applicableDeletions == null)
        {
            applicableDeletions = deletions.precedingState();
            if (applicableDeletions == null)
                return content;
        }

        return resolver.apply(applicableDeletions, content);
    }

    @Override
    public DeletionAwareMergeSource<T, D, E> tailCursor(Direction direction)
    {
        if (atDeletions)
            return new DeletionAwareMergeSource<>(resolver, live.tailCursor(direction), deletions.tailCursor(direction));
        else if (deletions != null)
            return new DeletionAwareMergeSource<>(resolver, live.tailCursor(direction), deletions.precedingStateCursor(direction));
        else
            return new DeletionAwareMergeSource<>(resolver, live.tailCursor(direction));
    }

    @Override
    public RangeCursor<D> deletionBranchCursor(Direction direction)
    {
        // Return unchanged, to be handled by MergeCursor.
        return live.deletionBranchCursor(direction);
    }

    public void addDeletions(RangeCursor<E> deletions)
    {
        assert this.deletions == null;
        deletions.assertFresh();
        this.deletions = deletions;
        this.deletionsRootPosition = live.encodedPosition();
        this.deletionsDepthCorrection = Cursor.depthCorrectionValue(deletionsRootPosition);
        this.atDeletions = true;
    }

    public boolean hasDeletions()
    {
        return deletions != null;
    }

    public static <T, D extends RangeState<D>, E extends RangeState<E>>
    DeletionAwareMergeSource<T, D, E> empty(Direction direction, ByteComparable.Version version)
    {
        return new DeletionAwareMergeSource<T, D, E>(null, new DeletionAwareCursor.Empty<>(direction, version));
    }
}