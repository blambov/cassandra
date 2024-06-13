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

/**
 * Deletion-aware trie, containing both live data and deletions.
 * To be able to query live data and deletions separately, we split deletions into separate branches of the trie,
 * given by the {@link Cursor#deletionBranch} method. Deletion branches are range tries, i.e. they support
 * deletions of individual as well as ranges of keys.
 * <p>
 * Deletion-aware tries must satisfy the following requirements:
 * - No deletion branch can be covered by another deletion branch, i.e. whenever the deletion branch is non-null at a
 *   given node, it must be null for all descendants of this node.
 * - Deletion branches must be well-formed, i.e.:
 *   - they cannot start with an active deletion (i.e. open start and open end ranges are not permitted),
 *   - every deletion opened by an entry must be closed by the next one,
 *   - the latter includes point deletions, which, if interrupting a range, must close and reopen it,
 *   - the deletion branch cursor cannot extend past the node of its introduction (i.e. it can never advance to depth <=
 *     its initial depth),
 *   - coveringState must be properly reported on the deletion branch.
 * - There cannot be entries in the trie that are deleted by the same trie (the condition above means this is not
 *   possible for with deletions).
 */
public interface DeletionAwareTrieImpl<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>> extends CursorWalkable<DeletionAwareTrieImpl.Cursor<T, D>>
{
    interface Cursor<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>> extends TrieImpl.Cursor<T>
    {
        /**
         * Deletion branch rooted at this position.
         */
        RangeTrieImpl.Cursor<D> deletionBranch();

        @Override
        Cursor<T, D> duplicate();
    }

    default <R> R process(TrieImpl.Walker<T, R> walker, Direction direction)
    {
        return TrieImpl.process(walker, cursor(direction));
    }

    @SuppressWarnings("unchecked")
    static <T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    DeletionAwareTrieImpl<T, D> impl(DeletionAwareTrie<T, D> trie)
    {
        return (DeletionAwareTrieImpl<T, D>) trie;
    }

    class LiveAndDeletionsMergeCursor<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>, Z>
    extends FlexibleMergeCursor.WithMappedContent<T, D, DeletionAwareTrieImpl.Cursor<T, D>, RangeTrieImpl.Cursor<D>, Z>
    {
        LiveAndDeletionsMergeCursor(Direction direction, BiFunction<T, D, Z> resolver, DeletionAwareTrieImpl.Cursor<T, D> c1)
        {
            super(direction, resolver, c1, null);
            maybeAddDeletionsBranch(c1.depth());
        }

        public LiveAndDeletionsMergeCursor(LiveAndDeletionsMergeCursor<T, D, Z> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public int advance()
        {
            return maybeAddDeletionsBranch(super.advance());
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return maybeAddDeletionsBranch(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            return maybeAddDeletionsBranch(super.advanceMultiple(receiver));
        }

        int maybeAddDeletionsBranch(int depth)
        {
            if (state == State.C1_ONLY)
            {
                RangeTrieImpl.Cursor<D> deletionsBranch = c1.deletionBranch();
                if (deletionsBranch != null)
                    addCursor(deletionsBranch);
            }
            return depth;
        }

        @Override
        public LiveAndDeletionsMergeCursor<T, D, Z> duplicate()
        {
            return new LiveAndDeletionsMergeCursor<>(this);
        }
    }

    class DeletionsTrieCursor<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends FlexibleMergeCursor<Cursor<T, D>, RangeTrieImpl.Cursor<D>> implements RangeTrieImpl.Cursor<D>
    {
        DeletionsTrieCursor(Direction direction, DeletionAwareTrieImpl.Cursor<T, D> c1)
        {
            super(direction, c1, null);
            maybeAddDeletionsBranch(c1.depth());
        }

        public DeletionsTrieCursor(DeletionsTrieCursor<T, D> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public D coveringState()
        {
            return c2 != null ? c2.coveringState() : null;
        }

        @Override
        public D content()
        {
            return c2 != null ? c2.content() : null;
        }

        @Override
        public int advance()
        {
            return maybeAddDeletionsBranch(super.advance());
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return maybeAddDeletionsBranch(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            return maybeAddDeletionsBranch(super.advanceMultiple(receiver));
        }

        int maybeAddDeletionsBranch(int depth)
        {
            if (state == State.C1_ONLY)
            {
                RangeTrieImpl.Cursor<D> deletionsBranch = c1.deletionBranch();
                if (deletionsBranch != null)
                {
                    addCursor(deletionsBranch);
                    c1.skipTo(c1.depth(), c1.incomingTransition() + direction.increase); // skip past the deletion branch
                    state = State.AT_C2;
                }
            }
            return depth;
        }

        @Override
        public DeletionsTrieCursor<T, D> duplicate()
        {
            return new DeletionsTrieCursor<>(this);
        }
    }

    static class EmptyCursor<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends TrieImpl.EmptyCursor<T> implements Cursor<T, D>
    {
        @Override
        public RangeTrieImpl.Cursor<D> deletionBranch()
        {
            return null;
        }

        @Override
        public Cursor<T, D> duplicate()
        {
            return depth == 0 ? new EmptyCursor<>() : this;
        }
    }

    @SuppressWarnings("rawtypes")
    static DeletionAwareTrieWithImpl EMPTY = dir -> new EmptyCursor();
}
