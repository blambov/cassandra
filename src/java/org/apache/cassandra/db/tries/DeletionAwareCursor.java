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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// Deletion-aware trie, containing both live data and deletions.
/// To be able to query live data and deletions separately, we split deletions into separate branches of the trie,
/// given by the [#deletionBranchCursor] method. Deletion branches are range tries, i.e. they support deletions of
/// individual as well as ranges of keys.
///
/// Deletion-aware tries must satisfy the following requirements:
/// - No deletion branch can be covered by another deletion branch, i.e. whenever the deletion branch is non-null at a
///   given node, it must be null for all descendants of this node.
/// - Deletion branches must be well-formed, i.e.:
///   - they cannot start with an active deletion (i.e. open start and open end ranges are not permitted),
///   - every deletion opened by an entry must be closed by the next one,
///   - the latter includes point deletions, which, if interrupting a range, must close and reopen it,
///   - the deletion branch cursor cannot extend past the node of its introduction (i.e. it can never advance to depth <=
///     its initial depth),
///   - precedingState must be properly reported on the deletion branch.
/// - There cannot be entries in the trie that are deleted by the same trie (the condition above means this is not
///   possible for deletions).
public interface DeletionAwareCursor<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>> extends Cursor<T>
{
    /// Returns the deletion branch rooted at this position, if any.
    ///
    /// The returned deletion branch cannot extend beyond the current branch, but in order to make merges efficient, it
    /// must report a state (i.e. depth and incoming transition) congruent with the source cursor's.
    ///
    /// When this method returns a non-null deletion branch, the source cursor is not allowed to return another deletion
    /// branch in the covered branch. In other words, for any given path in the trie there must be at most one node
    /// where [#deletionBranchCursor] is non-null.
    RangeCursor<D> deletionBranchCursor(Direction direction);

    @Override
    DeletionAwareCursor<T, D> tailCursor(Direction direction);

    class LiveAndDeletionsMergeCursor<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>, Z>
    extends FlexibleMergeCursor.WithMappedContent<T, D, DeletionAwareCursor<T, D>, RangeCursor<D>, Z>
    {
        LiveAndDeletionsMergeCursor(BiFunction<T, D, Z> resolver, DeletionAwareCursor<T, D> c1)
        {
            super(resolver, c1);
            postAdvance(c1.depth());
        }

        LiveAndDeletionsMergeCursor(BiFunction<T, D, Z> resolver, DeletionAwareCursor<T, D> c1, RangeCursor<D> c2, int c2depthCorrection)
        {
            super(resolver, c1, c2, c2depthCorrection);
            postAdvance(c1.depth());
        }

        @Override
        int postAdvance(int depth)
        {
            if (state == State.C1_ONLY)
            {
                RangeCursor<D> deletionsBranch = c1.deletionBranchCursor(direction);
                if (deletionsBranch != null)
                    addCursor(deletionsBranch, c1.depth());
            }
            return depth;
        }

        @Override
        public LiveAndDeletionsMergeCursor<T, D, Z> tailCursor(Direction direction)
        {
            switch (state)
            {
                case C1_ONLY:
                    return new LiveAndDeletionsMergeCursor<>(resolver, c1.tailCursor(direction));
                case AT_C2:
                    return new LiveAndDeletionsMergeCursor<>(resolver, new DeletionAwareCursor.Empty<>(direction, byteComparableVersion()), c2.tailCursor(direction), 0);
                case AT_C1:
                    return new LiveAndDeletionsMergeCursor<>(resolver, c1.tailCursor(direction), c2.precedingStateCursor(direction), 0);
                case AT_BOTH:
                    return new LiveAndDeletionsMergeCursor<>(resolver, c1.tailCursor(direction), c2.tailCursor(direction), 0);
                default:
                    throw new AssertionError();
            }
        }
    }

    class DeletionsTrieCursor<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends FlexibleMergeCursor<DeletionAwareCursor<T, D>, RangeCursor<D>, D> implements RangeCursor<D>
    {
        DeletionsTrieCursor(DeletionAwareCursor<T, D> c1)
        {
            super(c1);
            postAdvance(c1.depth());
        }

        @Override
        public D state()
        {
            return c2 != null ? c2.state() : null;
        }

        @Override
        public D precedingState()
        {
            return c2 != null ? c2.precedingState() : null;
        }

        @Override
        public D content()
        {
            return c2 != null ? c2.content() : null;
        }

        @Override
        int postAdvance(int depth)
        {
            switch (state)
            {
                case AT_C2:
                    // already in deletion branch
                    break;
                case C1_ONLY:
                    RangeCursor<D> deletionsBranch = c1.deletionBranchCursor(direction);
                    if (deletionsBranch != null)
                    {
                        final int c1depth = c1.depth();
                        addCursor(deletionsBranch, c1depth);
                        // deletion branches cannot be nested; skip past the current position in the main trie as we
                        // don't need to further track it inside this branch
                        c1.skipTo(c1depth, c1.incomingTransition() + direction.increase);
                        state = State.AT_C2;
                    }
                    break;
                default:
                    throw new AssertionError("Deletion branch extends above its introduction");
            }
            return depth;
        }

        @Override
        public RangeCursor<D> tailCursor(Direction direction)
        {
            switch (state)
            {
                case AT_C2:
                    return c2.tailCursor(direction);
                case C1_ONLY:
                    return new DeletionsTrieCursor<>(c1.tailCursor(direction));
                default:
                    throw new AssertionError("Deletion branch extends above its introduction");
            }
        }
    }

    static class Empty<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends Cursor.Empty<T> implements DeletionAwareCursor<T, D>
    {
        public Empty(Direction direction, ByteComparable.Version byteComparableVersion)
        {
            super(direction, byteComparableVersion);
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            return null;
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            return new DeletionAwareCursor.Empty<>(direction, byteComparableVersion());
        }
    }
}
