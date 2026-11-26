/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import java.util.function.BiFunction;

/// A merge of two basic deletion-aware cursors.
///
/// The merge of full deletion-aware cursors ([MergeCursor.DeletionAware]) has to combine its sources in four ways:
/// merge their live branches, merge their deletion branches, and apply the deletion branch of each to the live branch
/// of the other. Basic cursors present deletions together with the live content, which lets this merge do all of it
/// in one walk of each source, tracking the deletion of each source that covers the current position.
///
/// A deletion branch root is designated where either source has one, unless the position is already under one.
class BasicDeletionAwareMergeCursor<T, D extends RangeState<D>>
extends BasicMergeCursor<BasicDeletionAwareCursor<T, D>, BasicDeletionAwareCursor<T, D>, T>
implements BasicDeletionAwareCursor<T, D>
{
    final Trie.MergeResolver<T> liveResolver;
    final Trie.MergeResolver<D> deletionResolver;
    final BiFunction<D, T, T> deleter;

    /// The deletion boundary of each source at the current position, null if it has none or is not at the position.
    D deletion1;
    D deletion2;
    /// The deletion of each source that covers the current position if the source has no boundary there.
    D covering1;
    D covering2;
    /// The depth of the deletion branch root that the current position is under, -1 if none.
    int deletionBranchDepth = -1;

    BasicDeletionAwareMergeCursor(Trie.MergeResolver<T> liveResolver,
                                  Trie.MergeResolver<D> deletionResolver,
                                  BiFunction<D, T, T> deleter,
                                  BasicDeletionAwareCursor<T, D> c1,
                                  BasicDeletionAwareCursor<T, D> c2)
    {
        super(c1, c2);
        this.liveResolver = liveResolver;
        this.deletionResolver = deletionResolver;
        this.deleter = deleter;
        checkOrder(c1.encodedPosition(), c2.encodedPosition());
    }

    @Override
    public long advance()
    {
        leavePosition();
        return super.advance();
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        leavePosition();
        return super.advanceMultiple(receiver);
    }

    /// The boundaries at the position we are leaving determine the deletions that cover what follows.
    private void leavePosition()
    {
        Direction direction = Cursor.direction(currentPosition);
        if (deletion1 != null)
            covering1 = deletion1.succedingState(direction);
        if (deletion2 != null)
            covering2 = deletion2.succedingState(direction);
    }

    @Override
    long checkOrder(long c1pos, long c2pos)
    {
        long position = super.checkOrder(c1pos, c2pos);
        deletion1 = atC1 ? BasicDeletionAwareCursor.deletion(c1, c1pos) : null;
        deletion2 = atC2 ? BasicDeletionAwareCursor.deletion(c2, c2pos) : null;

        // Deletion branch roots cannot be nested. Keep the first on each path.
        int depth = Cursor.depth(position);
        if (deletionBranchDepth >= 0 && !(depth > deletionBranchDepth || depth == deletionBranchDepth && Cursor.isOnReturnPath(position)))
            deletionBranchDepth = -1;
        if (BasicDeletionAwareCursor.isDeletionBranchRoot(position))
        {
            if (deletionBranchDepth < 0)
                deletionBranchDepth = depth;
            else
                position &= ~Cursor.MAY_HAVE_DELETION_BRANCH_BIT;
        }
        return currentPosition = position;
    }

    @Override
    public T content()
    {
        T content1 = atC1 ? applyDeletion(deletion2, covering2, BasicCursor.content(c1, c1.encodedPosition())) : null;
        T content2 = atC2 ? applyDeletion(deletion1, covering1, BasicCursor.content(c2, c2.encodedPosition())) : null;
        if (content1 == null)
            return content2;
        else if (content2 == null)
            return content1;
        else
            return liveResolver.resolve(content1, content2);
    }

    /// Apply the deletion of the other source to the content of one.
    private T applyDeletion(D deletion, D covering, T content)
    {
        if (content == null)
            return null;
        if (deletion == null)
            deletion = covering;
        return deletion != null ? deleter.apply(deletion, content) : content;
    }

    @Override
    public D deletion()
    {
        // There is no boundary here unless one of the sources has one. The other may still cover the position.
        if (deletion1 == null && deletion2 == null)
            return null;
        D state1 = deletion1 != null ? deletion1 : covering1;
        D state2 = deletion2 != null ? deletion2 : covering2;
        if (state1 == null)
            return state2;
        if (state2 == null)
            return state1;
        D resolved = deletionResolver.resolve(state1, state2);
        return resolved != null && resolved.isBoundary() ? resolved : null;
    }
}
