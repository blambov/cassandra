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
 * The intersection of a trie with the given set.
 */
class SetIntersectionTrie<T> extends Trie<T>
{
    private final Trie<T> trie;
    private final TrieSet intersectingSet;

    SetIntersectionTrie(Trie<T> trie, TrieSet intersectingSet)
    {
        this.trie = trie;
        this.intersectingSet = intersectingSet;
    }

    protected Cursor<T> cursor()
    {
        return new IntersectionCursor(trie.cursor(), intersectingSet.cursor());
    }

    private static class IntersectionCursor<T> implements Cursor<T>
    {
        private final Cursor<T> tCursor;
        private final Cursor<TrieSet.InSet> sCursor;

        public IntersectionCursor(Cursor<T> tCursor,
                                  Cursor<TrieSet.InSet> sCursor)
        {
            this.tCursor = tCursor;
            this.sCursor = sCursor;
        }

        @Override
        public int advance()
        {
            int tLevel = tCursor.advance();
            if (sCursor.content() == TrieSet.InSet.BRANCH)
            {
                if (tLevel > sCursor.level())
                    return tLevel;
                // otherwise we have left the intersection's covered branch
            }
            int sLevel = sCursor.advance();

            return advanceToIntersection(tLevel, sLevel);
        }

        @Override
        public int advanceMultiple(TransitionsReceiver transitionsReceiver)
        {
            int tLevel;
            if (sCursor.content() == TrieSet.InSet.BRANCH)
            {
                tLevel = tCursor.advanceMultiple(transitionsReceiver);
                if (tLevel > sCursor.level())
                    return tLevel;
                // otherwise we have left the intersection's covered branch
            }
            else
                tLevel = tCursor.advance();

            int sLevel = sCursor.advance();
            return advanceToIntersection(tLevel, sLevel);
        }

        @Override
        public int ascend() // this is not tested ATM
        {
            int tLevel = tCursor.ascend();
            if (sCursor.content() == TrieSet.InSet.BRANCH)
            {
                if (tLevel > sCursor.level())
                    return tLevel;
                // otherwise we have left the intersection's covered branch
            }
            int sLevel = sCursor.ascend();

            return advanceToIntersection(tLevel, sLevel);
        }

        private int advanceToIntersection(int tLevel, int sLevel)
        {
            while (sLevel != -1 && tLevel != -1)
            {
                if (sLevel == tLevel)
                {
                    int tIncoming = tCursor.incomingTransition();
                    int sIncoming = sCursor.incomingTransition();
                    if (sIncoming == tIncoming)
                        return tLevel;  // got entry
                    else if (sIncoming < tIncoming)
                        sLevel = sCursor.ascend();
                    else // sIncoming > tIncoming
                        tLevel = tCursor.ascend();
                }
                else if (sLevel < tLevel)
                {
                    if (sCursor.content() == TrieSet.InSet.BRANCH)
                        return tLevel;
                    tLevel = tCursor.ascend();
                }
                else // (sLevel > tLevel)
                    sLevel = sCursor.ascend();
            }
            return -1;
        }

        // TODO: implement advanceMultiple

        public int level()
        {
            return tCursor.level();
        }

        public int incomingTransition()
        {
            return tCursor.incomingTransition();
        }

        public T content()
        {
            return sCursor.content() != null
                   ? tCursor.content()
                   : null;
        }
    }
}
