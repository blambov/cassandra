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

/**
 * Intersection implementation which works with {@code Trie<Contained>} as the intersecting set.
 * <p>
 * Implemented as a parallel walk over the two tries, skipping over the parts which are not in the set, and skipping
 * advancing in the set while we know branches are fully contained within the set (i.e. when its {@code content()}
 * returns FULLY). To do the latter we always examing the set's content, and on receiving FULLY we store the set's
 * current depth as the lowest depth we need to see on a source advance to be outside of the covered branch. This
 * allows us to implement {@code advanceMultiple}, and to have a very low overhead on all advances inside the covered
 * branch.
 * <p>
 * As it is expected that the structure of the set is going to be simpler, unless we know that we are inside a fully
 * covered branch we advance the set first, and skip in the source to the advanced position.
 */
public class IntersectionTrie<T> extends Trie<T>
{
    final Trie<T> trie;
    final Trie<Contained> set;

    public IntersectionTrie(Trie<T> trie, Trie<Contained> set)
    {
        this.trie = trie;
        this.set = set;
    }

    @Override
    protected Cursor<T> cursor()
    {
        return new IntersectionCursor<>(trie.cursor(), set.cursor());
    }

    private static class IntersectionCursor<T> implements Cursor<T>
    {
        final Cursor<T> source;
        final Cursor<Contained> set;
        int setQueryDepth;
        Contained contained;

        public IntersectionCursor(Cursor<T> source, Cursor<Contained> set)
        {
            this(source, set, 0);
        }

        IntersectionCursor(Cursor<T> source, Cursor<Contained> set, int depth)
        {
            this.source = source;
            this.set = set;
            intersectionFound(depth);
        }

        @Override
        public int depth()
        {
            return source.depth();
        }

        @Override
        public int incomingTransition()
        {
            return source.incomingTransition();
        }

        @Override
        public T content()
        {
            return contained != null
                   ? source.content()
                   : null;
        }

        @Override
        public int advance()
        {
            return contained == Contained.FULLY
                   ? advanceSourceFirst(source.advance())
                   : advanceSetFirst(set.advance());
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return contained == Contained.FULLY
                   ? advanceSourceFirst(source.skipTo(skipDepth, skipTransition))
                   : advanceSetFirst(set.skipTo(skipDepth, skipTransition));
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            return contained == Contained.FULLY
                   ? advanceSourceFirst(source.advanceMultiple(receiver))
                   : advanceSetFirst(set.advance());
        }

        @Override
        public Cursor<T> alternateBranch()
        {
            Cursor<T> alternate = source.alternateBranch();
            if (alternate == null)
                return null;
            return makeCursor(alternate, set.duplicate(), depth());
        }

        @Override
        public Cursor<T> duplicate()
        {
            return makeCursor(source.duplicate(), set.duplicate(), depth());
        }

        Cursor<T> makeCursor(Cursor<T> source, Cursor<Contained> set, int depth)
        {
            return new IntersectionCursor<>(source, set, depth);
        }

        private int advanceSourceFirst(int sourceDepth)
        {
            if (sourceDepth > setQueryDepth)
                return sourceDepth; // still inside a fully-covered branch, no need to advance in set

            int sourceTransition = source.incomingTransition();
            int setDepth, setTransition;
            while (true)
            {
                setDepth = set.skipTo(sourceDepth, sourceTransition);
                setTransition = set.incomingTransition();
                if (setDepth == sourceDepth && (sourceDepth == -1 || setTransition == sourceTransition))
                    break;
                sourceDepth = source.skipTo(setDepth, setTransition);
                sourceTransition = source.incomingTransition();
                if (setDepth == sourceDepth && (sourceDepth == -1 || setTransition == sourceTransition))
                    break;
            }
            return intersectionFound(setDepth);
        }

        private int advanceSetFirst(int setDepth)
        {
            int setTransition = set.incomingTransition();
            int sourceDepth, sourceTransition;
            while (true)
            {
                sourceDepth = source.skipTo(setDepth, setTransition);
                sourceTransition = source.incomingTransition();
                if (setDepth == sourceDepth && (sourceDepth == -1 || setTransition == sourceTransition))
                    break;
                setDepth = set.skipTo(sourceDepth, sourceTransition);
                setTransition = set.incomingTransition();
                if (setDepth == sourceDepth && (sourceDepth == -1 || setTransition == sourceTransition))
                    break;
            }
            return intersectionFound(setDepth);
        }

        private int intersectionFound(int depth)
        {
            contained = set.content();
            if (contained == Contained.FULLY)
                setQueryDepth = depth;

            return depth;
        }
    }

    static class SetIntersectionTrie extends IntersectionTrie<Contained>
    {
        public SetIntersectionTrie(Trie<Contained> trie, Trie<Contained> set)
        {
            super(trie, set);
        }

        @Override
        protected Cursor<Contained> cursor()
        {
            return new SetIntersectionCursor(trie.cursor(), set.cursor());
        }
    }

    private static class SetIntersectionCursor extends IntersectionCursor<Contained>
    {
        public SetIntersectionCursor(Cursor<Contained> source, Cursor<Contained> set)
        {
            this(source, set, 0);
        }

        public SetIntersectionCursor(Cursor<Contained> source, Cursor<Contained> set, int depth)
        {
            super(source, set, depth);
        }

        @Override
        public Contained content()
        {
            final Contained sourceContent = source.content();
            if (sourceContent == null)
                return null;
            if (contained == Contained.FULLY)
                return sourceContent;
            else if (contained == Contained.PARTIALLY)
                return Contained.PARTIALLY;
            else
                return null;
        }

        @Override
        Cursor<Contained> makeCursor(Cursor<Contained> source, Cursor<Contained> set, int depth)
        {
            return new SetIntersectionCursor(source, set, depth);
        }
    }
}
