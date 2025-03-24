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
abstract class MergeCursor<T, T1, C1 extends Cursor<T1>, T2, C2 extends Cursor<T2>> implements Cursor<T>
{
    final Direction direction;
    final C1 c1;
    final C2 c2;

    boolean atC1;
    boolean atC2;

    MergeCursor(C1 c1, C2 c2)
    {
        this.direction = c1.direction();
        this.c1 = c1;
        this.c2 = c2;
        assert c1.depth() == 0;
        assert c2.depth() == 0;
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

    static abstract class WithContent<T, C extends Cursor<T>> extends MergeCursor<T, T, C, T, C>
    {
        final Trie.MergeResolver<T> resolver;

        WithContent(Trie.MergeResolver<T> resolver, C c1, C c2)
        {
            super(c1, c2);
            this.resolver = resolver;
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
    }


    /// Merge implementation for [Trie]
    static class Plain<T> extends WithContent<T, Cursor<T>>
    {
        Plain(Trie.MergeResolver<T> resolver, Cursor<T> c1, Cursor<T> c2)
        {
            super(resolver, c1, c2);
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
    static class Range<M extends RangeMarker<M>> extends WithContent<M, RangeCursor<M>> implements RangeCursor<M>
    {
        private M coveringState;
        boolean coveringStateSet;

        Range(Trie.MergeResolver<M> resolver, RangeCursor<M> c1, RangeCursor<M> c2)
        {
            super(resolver, c1, c2);
        }

        @Override
        public M coveringState()
        {
            if (!coveringStateSet)
            {
                M state1 = c1.coveringState();
                M state2 = c2.coveringState();
                if (state1 == null)
                    return state2;
                if (state2 == null)
                    return state1;
                coveringState = resolver.resolve(state1, state2);
                coveringStateSet = true;
            }
            return coveringState;
        }

        @Override
        public int advance()
        {
            coveringStateSet = false;
            return super.advance();
        }

        @Override
        public int skipTo(int depth, int incomingTransition)
        {
            coveringStateSet = false;
            return super.skipTo(depth, incomingTransition);
        }

        @Override
        public int advanceMultiple(Cursor.TransitionsReceiver receiver)
        {
            coveringStateSet = false;
            return super.advanceMultiple(receiver);
        }

        @Override
        public M content()
        {
            M content1 = atC1 ? c1.content() : null;
            M content2 = atC2 ? c2.content() : null;
            if (content1 == null && content2 == null)
                return null;
            if (content1 != null && content2 != null)
                return toContent(resolver.resolve(content1, content2));

            // Exactly one is non-null; must apply the state of the other
            if (content1 == null)
            {
                content1 = c1.coveringState();
                if (content1 == null)
                    return content2;
            } else // content2 == null
            {
                content2 = c2.coveringState();
                if (content2 == null)
                    return content1;
            }

            return toContent(resolver.resolve(content1, content2));
        }

        @Override
        public RangeCursor<M> tailCursor(Direction direction)
        {
            if (atC1 && atC2)
                return new Range<>(resolver, c1.tailCursor(direction), c2.tailCursor(direction));
            else if (atC1)
                return new Range<>(resolver, c1.tailCursor(direction), c2.coveringStateCursor(direction));
            else if (atC2)
                return new Range<>(resolver, c1.coveringStateCursor(direction), c2.tailCursor(direction));
            else
                throw new AssertionError();
        }

        private M toContent(M content)
        {
            return content != null ? content.toContent() : null;
        }
    }

    static class RangeOnTrie<M extends RangeMarker<M>, T> extends MergeCursor<T, M, RangeCursor<M>, T, Cursor<T>>
    {
        final BiFunction<M, T, T> resolver;

        RangeOnTrie(BiFunction<M, T, T> resolver, RangeCursor<M> c1, Cursor<T> c2)
        {
            super(c1, c2);
            this.resolver = resolver;
        }

        @Override
        public int advance()
        {
            return maybeSkipC1(super.advance());
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return maybeSkipC1(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public int advanceMultiple(Cursor.TransitionsReceiver receiver)
        {
            return maybeSkipC1(super.advanceMultiple(receiver));
        }

        int maybeSkipC1(int depth)
        {
            if (atC2)
                return depth;
            assert atC1;
            final int c2depth = c2.depth();
            return checkOrder(c1.skipTo(c2depth, c2.incomingTransition()), c2depth);
        }
        // TODO: This can be simplified a lot (atC2 is always true)

        @Override
        public T content()
        {
            if (!atC2)
                return null;
            T content = c2.content();
            if (content == null)
                return null;

            M applicableRange = atC1 ? c1.content() : null;
            if (applicableRange == null)
            {
                applicableRange = c1.coveringState();
                if (applicableRange == null)
                    return content;
            }

            return resolver.apply(applicableRange, content);
        }

        @Override
        public Cursor<T> tailCursor(Direction direction)
        {
            assert atC2;
            if (atC1)
                return new RangeOnTrie(resolver, c1.tailCursor(direction), c2.tailCursor(direction));
            else
                return c2.tailCursor(direction);
        }
    }
}
