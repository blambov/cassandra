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

/// A merged view of two trie cursors.
///
/// This is accomplished by walking the two cursors in parallel; the merged cursor takes the position and features of the
/// smaller and advances with it; when the two cursors are equal, both are advanced.
///
/// Crucial for the efficiency of this is the fact that when they are advanced like this, we can compare cursors'
/// positions by their `depth` descending and then `incomingTransition` ascending.
/// See [Trie.md](./Trie.md) for further details.
abstract class MergeCursor<T, C extends Cursor<T>> implements Cursor<T>
{
    final Direction direction;
    final Trie.MergeResolver<T> resolver;

    final C c1;
    final C c2;

    boolean atC1;
    boolean atC2;

    MergeCursor(Trie.MergeResolver<T> resolver, C c1, C c2)
    {
        this.direction = c1.direction();
        this.resolver = resolver;
        this.c1 = c1;
        this.c2 = c2;
        assert c1.depth() == c2.depth();
        assert c1.incomingTransition() == c2.incomingTransition();
        atC1 = atC2 = true;
    }

    @Override
    public int advance()
    {
        return checkOrder(atC1 ? c1.advance() : c1.depth(),
                          atC2 ? c2.advance() : c2.depth());
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        int c1depth = c1.depth();
        int c2depth = c2.depth();
        assert skipDepth <= c1depth + 1 || skipDepth <= c2depth + 1;
        if (atC1 || skipDepth < c1depth || skipDepth == c1depth && direction.gt(skipTransition, c1.incomingTransition()))
            c1depth = c1.skipTo(skipDepth, skipTransition);
        if (atC2 || skipDepth < c2depth || skipDepth == c2depth && direction.gt(skipTransition, c2.incomingTransition()))
            c2depth = c2.skipTo(skipDepth, skipTransition);

        return checkOrder(c1depth, c2depth);
    }

    @Override
    public int advanceMultiple(TransitionsReceiver receiver)
    {
        // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
        if (atC1 && atC2)
            return checkOrder(c1.advance(), c2.advance());

        // If we are in a branch that's only covered by one of the sources, we can use its advanceMultiple as it is
        // only different from advance if it takes multiple steps down, which does not change the order of the
        // cursors.
        // Since it might ascend, we still have to check the order after the call.
        if (atC1)
            return checkOrder(c1.advanceMultiple(receiver), c2.depth());
        else // atC2
            return checkOrder(c1.depth(), c2.advanceMultiple(receiver));
    }

    int checkOrder(int c1depth, int c2depth)
    {
        if (c1depth > c2depth)
        {
            atC1 = true;
            atC2 = false;
            return c1depth;
        }
        if (c1depth < c2depth)
        {
            atC1 = false;
            atC2 = true;
            return c2depth;
        }
        // c1depth == c2depth
        int c1trans = c1.incomingTransition();
        int c2trans = c2.incomingTransition();
        atC1 = direction.le(c1trans, c2trans);
        atC2 = direction.le(c2trans, c1trans);
        assert atC1 | atC2;
        return c1depth;
    }

    @Override
    public int depth()
    {
        return atC1 ? c1.depth() : c2.depth();
    }

    @Override
    public int incomingTransition()
    {
        return atC1 ? c1.incomingTransition() : c2.incomingTransition();
    }

    @Override
    public Direction direction()
    {
        return direction;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        assert c1.byteComparableVersion() == c2.byteComparableVersion() :
            "Merging cursors with different byteComparableVersions: " +
            c1.byteComparableVersion() + " vs " + c2.byteComparableVersion();
        return c1.byteComparableVersion();
    }

    /// Merge implementation for [Trie]
    static class Plain<T> extends MergeCursor<T, Cursor<T>>
    {
        Plain(Trie.MergeResolver<T> resolver, Cursor<T> c1, Cursor<T> c2)
        {
            super(resolver, c1, c2);
        }

        @Override
        public T content()
        {
            T mc = atC2 ? c2.content() : null;
            T nc = atC1 ? c1.content() : null;
            if (mc == null)
                return nc;
            else if (nc == null)
                return mc;
            else
                return resolver.resolve(nc, mc);
        }

        @Override
        public Cursor<T> tailCursor(Direction direction)
        {
            if (atC1 && atC2)
                return new Plain<>(resolver, c1.tailCursor(direction), c2.tailCursor(direction));
            else if (atC1)
                return c1.tailCursor(direction);
            else if (atC2)
                return c2.tailCursor(direction);
            else
                throw new AssertionError();
        }
    }

    /// Merge implementation for [RangeTrie]
    static class Range<S extends RangeState<S>> extends MergeCursor<S, RangeCursor<S>> implements RangeCursor<S>
    {
        private S state;
        boolean stateCollected;

        Range(Trie.MergeResolver<S> resolver, RangeCursor<S> c1, RangeCursor<S> c2)
        {
            super(resolver, c1, c2);
        }

        @Override
        public S state()
        {
            if (!stateCollected)
            {
                S state1 = atC1 ? c1.state() : c1.precedingState();
                S state2 = atC2 ? c2.state() : c2.precedingState();
                if (state1 == null)
                    return state2;
                if (state2 == null)
                    return state1;
                state = resolver.resolve(state1, state2);
                stateCollected = true;
            }
            return state;
        }

        @Override
        public int advance()
        {
            stateCollected = false;
            return super.advance();
        }

        @Override
        public int skipTo(int depth, int incomingTransition)
        {
            stateCollected = false;
            return super.skipTo(depth, incomingTransition);
        }

        @Override
        public int advanceMultiple(Cursor.TransitionsReceiver receiver)
        {
            stateCollected = false;
            return super.advanceMultiple(receiver);
        }

        @Override
        public RangeCursor<S> tailCursor(Direction direction)
        {
            if (atC1 && atC2)
                return new Range<>(resolver, c1.tailCursor(direction), c2.tailCursor(direction));
            else if (atC1)
                return new Range<>(resolver, c1.tailCursor(direction), c2.precedingStateCursor(direction));
            else if (atC2)
                return new Range<>(resolver, c1.precedingStateCursor(direction), c2.tailCursor(direction));
            else
                throw new AssertionError();
        }
    }

    static class DeletionAware<T, D extends RangeState<D>>
    extends MergeCursor<T, DeletionAwareMergeSource<T, D>> implements DeletionAwareCursor<T, D>
    {
        final Trie.MergeResolver<D> deletionResolver;
        int deletionBranchDepth = -1;

        DeletionAware(Trie.MergeResolver<T> mergeResolver,
                      Trie.MergeResolver<D> deletionResolver,
                      BiFunction<D, T, T> deleter,
                      DeletionAwareCursor<T, D> c1,
                      DeletionAwareCursor<T, D> c2)
        {
            this(mergeResolver,
                 deletionResolver,
                 new DeletionAwareMergeSource<>(deleter, c1),
                 new DeletionAwareMergeSource<>(deleter, c2));
            // We will add deletion sources to the above as we find them.
            maybeAddDeletionsBranch(this.c1.depth());
        }

        DeletionAware(Trie.MergeResolver<T> mergeResolver,
                      Trie.MergeResolver<D> deletionResolver,
                      DeletionAwareMergeSource<T, D> c1,
                      DeletionAwareMergeSource<T, D> c2)
        {
            super(mergeResolver, c1, c2);
            // We will add deletion sources to the above as we find them.
            this.deletionResolver = deletionResolver;
        }

        @Override
        public T content()
        {
            T mc = atC2 ? c2.content() : null;
            T nc = atC1 ? c1.content() : null;
            if (mc == null)
                return nc;
            else if (nc == null)
                return mc;
            else
                return resolver.resolve(nc, mc);
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
            if (depth <= deletionBranchDepth)   // ascending above common deletions root
            {
                deletionBranchDepth = -1;
                assert !c1.hasDeletions();
                assert !c2.hasDeletions();
            }

            if (atC1 && atC2)
            {
                maybeAddDeletionsBranch(c1, c2);
                maybeAddDeletionsBranch(c2, c1);
            }   // otherwise even if there is deletion, the other cursor is ahead of it and can't be affected
            return depth;
        }

        void maybeAddDeletionsBranch(DeletionAwareMergeSource<T, D> c1,
                                     DeletionAwareMergeSource<T, D> c2)
        {
            if (c1.hasDeletions())
                return;

            RangeCursor<D> deletionsBranch = c2.deletionBranchCursor(direction);
            if (deletionsBranch != null)
                c1.addDeletions(deletionsBranch);  // apply all c2 deletions to c1
        }

        @Override
        public RangeCursor<D> deletionBranchCursor(Direction direction)
        {
            int depth = depth();
            if (deletionBranchDepth != -1 && depth > deletionBranchDepth)
                return null;    // already covered by a deletion branch, if there is any here it will be reflected in that

            if (!atC1)
                return c2.deletionBranchCursor(direction); // if c1 is ahead, it can't affect this deletion branch
            if (!atC2)
                return c1.deletionBranchCursor(direction);

            // We are positioned at a common branch. If one has a deletion branch, we must combine it with the
            // deletion-tree branch of the other to make sure that we merge any higher-depth deletion branch with it.
            RangeCursor<D> b1 = c1.deletionBranchCursor(direction);
            RangeCursor<D> b2 = c2.deletionBranchCursor(direction);
            if (b1 == null && b2 == null)
                return null;

            deletionBranchDepth = depth;
            if (b1 == null)
                b1 = new DeletionAwareCursor.DeletionsTrieCursor(c1.data.tailCursor(direction));
            if (b2 == null)
                b2 = new DeletionAwareCursor.DeletionsTrieCursor(c2.data.tailCursor(direction));

            return new Range<>(deletionResolver, b1, b2);
        }

        @Override
        public DeletionAwareCursor<T, D> tailCursor(Direction direction)
        {
            if (atC1 && atC2)
                return new DeletionAware<>(resolver,
                                           deletionResolver,
                                           c1.tailCursor(direction),
                                           c2.tailCursor(direction));
            else if (atC1)
                return c1.tailCursor(direction);
            else if (atC2)
                return c2.tailCursor(direction);
            else
                throw new AssertionError();
        }
    }
}
