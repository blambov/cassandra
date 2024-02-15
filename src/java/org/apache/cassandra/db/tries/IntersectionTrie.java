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

    static class IntersectionCursor<T> implements Cursor<T>
    {
        final Cursor<T> source;
        final Cursor<Contained> set;
        boolean setAhead;
        boolean reportContent;

        public IntersectionCursor(Cursor<T> source, Cursor<Contained> set)
        {
            this.source = source;
            this.set = set;
            onMatchingPosition(depth());
        }

        public IntersectionCursor(Cursor<T> source, Cursor<Contained> set, boolean setAhead, boolean reportContent)
        {
            this.source = source;
            this.set = set;
            this.setAhead = setAhead;
            this.reportContent = reportContent;
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
            return reportContent
                   ? source.content()
                   : null;
        }

        @Override
        public int advance()
        {
            if (setAhead)
                return advanceInCoveredBranch(set.depth(), source.advance());

            int setDepth = set.advance();
            if (inSetCoveredArea())
                return advanceInCoveredBranch(setDepth, source.advance());
            else
                return advanceWithSetAhead(setDepth, set.incomingTransition());
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            if (setAhead)
                return advanceInCoveredBranch(set.depth(), source.skipTo(skipDepth, skipTransition));

            int setDepth = set.skipTo(skipDepth, skipTransition);
            if (inSetCoveredArea())
                return advanceInCoveredBranch(setDepth, source.skipTo(skipDepth, skipTransition));
            else
                return advanceWithSetAhead(setDepth, set.incomingTransition());
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            if (setAhead)
                return advanceInCoveredBranch(set.depth(), source.advanceMultiple(receiver));

            int setDepth = set.advance();
            if (inSetCoveredArea())
                return advanceInCoveredBranch(setDepth, source.advance());
            else
                return advanceWithSetAhead(setDepth, set.incomingTransition());
        }

        private int advanceInCoveredBranch(int setDepth, int sourceDepth)
        {
            if (sourceDepth < 0)
                return sourceDepth; // exhausted
            if (sourceDepth > setDepth)
                return markSetAheadAndCovered(sourceDepth);
            int setTransition = set.incomingTransition();
            int sourceTransition = source.incomingTransition();
            if (sourceDepth == setDepth)
            {
                if (sourceTransition < setTransition)
                    return markSetAheadAndCovered(sourceDepth);
                if (sourceTransition == setTransition)
                    return onMatchingPosition(setDepth);
            }

            return advanceWithSourceAhead(sourceDepth, sourceTransition);
        }

        private int advanceWithSetAhead(int setDepth, int setTransition)
        {
            int sourceDepth = source.skipTo(setDepth, setTransition);
            if (sourceDepth < 0)
                return sourceDepth; // exhausted
            int sourceTransition = source.incomingTransition();

            if (sourceDepth == setDepth && sourceTransition == setTransition)
                return onMatchingPosition(setDepth);
            else
                return advanceWithSourceAhead(sourceDepth, sourceTransition);
        }

        private int advanceWithSourceAhead(int sourceDepth, int sourceTransition)
        {
            int setDepth;
            int setTransition;
            while (true)
            {
                // source is now ahead of the set
                setDepth = set.skipTo(sourceDepth, sourceTransition);
                setTransition = set.incomingTransition();
//                if (setDepth < 0)
//                    return source.skipTo(-1, 0);    // exhausted
                if (setDepth == sourceDepth && setTransition == sourceTransition)
                    return onMatchingPosition(setDepth);

                // At this point set is ahead. Check content to see if we are in a covered branch.
                if (inSetCoveredArea())
                    return markSetAheadAndCovered(sourceDepth);

                // Otherwise we need to skip the source as well and repeat the process
                sourceDepth = source.skipTo(setDepth, setTransition);
                sourceTransition = source.incomingTransition();
                if (sourceDepth < 0)
                    return sourceDepth;
                if (sourceDepth == setDepth && sourceTransition == setTransition)
                    return onMatchingPosition(setDepth);
            }
        }

        private boolean inSetCoveredArea()
        {
            Contained contained = set.content();
            return (contained == Contained.INSIDE_PREFIX || contained == Contained.END);
        }

        private int markSetAheadAndCovered(int depth)
        {
            setAhead = true;
            reportContent = true;
            return depth;
        }

        @Override
        public Cursor<T> alternateBranch()
        {
            Cursor<T> alternate = source.alternateBranch();
            if (alternate == null)
                return null;
            return makeCursor(alternate, set.duplicate(), setAhead, reportContent);
        }

        @Override
        public Cursor<T> duplicate()
        {
            return makeCursor(source.duplicate(), set.duplicate(), setAhead, reportContent);
        }

        Cursor<T> makeCursor(Cursor<T> source, Cursor<Contained> set, boolean setAhead, boolean reportContent)
        {
            return new IntersectionCursor<>(source, set, setAhead, reportContent);
        }

        private int onMatchingPosition(int depth)
        {
            Contained contained = set.content();
            reportContent = contained == Contained.START || contained == Contained.INSIDE_PREFIX;
            setAhead = false;
            return depth;
        }
    }

//    static class SetIntersectionTrie extends IntersectionTrie<Contained>
//    {
//        public SetIntersectionTrie(Trie<Contained> trie, Trie<Contained> set)
//        {
//            super(trie, set);
//        }
//
//        @Override
//        protected Cursor<Contained> cursor()
//        {
//            return new SetIntersectionCursor(trie.cursor(), set.cursor());
//        }
//    }
//
//    private static class SetIntersectionCursor extends IntersectionCursor<Contained>
//    {
//        public SetIntersectionCursor(Cursor<Contained> source, Cursor<Contained> set)
//        {
//            super(source, set);
//        }
//
//        @Override
//        public Contained content()
//        {
//            final Contained sourceContent = source.content();
//            if (sourceContent == null)
//                return null;
//            if (contained == Contained.FULLY)
//                return sourceContent;
//            else if (contained == Contained.PARTIALLY)
//                return Contained.PARTIALLY;
//            else
//                return null;
//        }
//
//        @Override
//        Cursor<Contained> makeCursor(Cursor<Contained> source, Cursor<Contained> set)
//        {
//            return new SetIntersectionCursor(source, set);
//        }
//    }
}
