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
 * Intersection of a trie with a set, given as a {@code Trie<RegionEnd>}, where the presence
 * of content (regardless true or false) is interpreted to mean presence to the set.
 * <p>
 * The latter is the simplest possible definition of a set, which also allows direct application
 * of merges and intersections to sets. Its main disadvantage is that it does not define
 * "fully in-set" branches and thus walks the set while walking the source even for fully
 * covered branches thus adding to iteration time.
 * <p>
 * TODO: Consider alternatives of the set definition that can provide region end's
 * depth+transition so that e.g. advanceMultiple can be fully delegated to the source.
 */
public class IntersectionTrie<T> extends Trie<T>
{
    interface RegionEnd
    {
        int endDepth(); // cannot be deeper than iterated depth
        int endTransition();
    }

    final Trie<T> trie;
    final Trie<? extends RegionEnd> set;

    public IntersectionTrie(Trie<T> trie, Trie<? extends RegionEnd> set)
    {
        this.trie = trie;
        this.set = set;
    }

    @Override
    protected Cursor<T> cursor()
    {
        return new IntersectionCursor(trie.cursor(), set.cursor());
    }

    private static class IntersectionCursor<T> implements Cursor<T>
    {
        final Cursor<T> source;
        final Cursor<? extends RegionEnd> set;

//        private static final int UNAVAILABLE = Integer.MAX_VALUE;
        boolean inSet;
        int endDepth;
        int endTransition;

        public IntersectionCursor(Cursor<T> source, Cursor<? extends RegionEnd> set)
        {
            this.source = source;
            this.set = set;
            updateState(0, -1);
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
            return inSet ? source.content() : null;
        }

        void updateState(int depth, int transition)
        {
            RegionEnd end = set.content();
            if (end != null)
            {
                inSet = true;
                endDepth = end.endDepth();
                endTransition = end.endTransition();
            }
            else
            {
                inSet = false;
                endDepth = Integer.MAX_VALUE;
//                endTransition = transition;
            }
        }

        @Override
        public int advance()
        {
            return advanceToIntersection(source.advance());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            int depth = source.depth();
            if (depth > endDepth || depth == endDepth && source.incomingTransition() < endTransition)
                return advanceToIntersection(source.advanceMultiple(receiver));
            else
                return advanceToIntersection(source.advance());
        }

        private int advanceToIntersection(int depth)
        {
            int transition = source.incomingTransition();
            if (depth > endDepth || depth == endDepth && transition < endTransition)
                return depth;   // still inside same covered region

            int setDepth = set.skipTo(depth, transition);
            int setTransition = set.incomingTransition();
            while (setDepth != depth || (depth != -1 && setTransition != transition))
            {
                depth = source.skipTo(setDepth, setTransition);
                transition = source.incomingTransition();
                if (!(setDepth != depth || (depth != -1 && setTransition != transition)))
                    break;

                setDepth = set.skipTo(depth, transition);
                setTransition = set.incomingTransition();
            }
            if (depth > 0)
                updateState(depth, transition);
            return depth;
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return advanceToIntersection(source.skipTo(skipDepth, skipTransition));
        }
    }
}
