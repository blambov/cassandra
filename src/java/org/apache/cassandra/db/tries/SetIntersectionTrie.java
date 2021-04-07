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

    @Override
    public <L> Node<T, L> root()
    {
        Node<TrieSet.InSet, Void> sRoot = intersectingSet.root();
        if (sRoot == null)
            return null;

        Node<T, L> tRoot = trie.root();
        if (sRoot.content() == TrieSet.InSet.BRANCH)
            return tRoot;
        if (tRoot == null)
            return null;

        return new IntersectionNode<>(tRoot, sRoot);
    }

    static class IntersectionNode<T, L> extends Node<T, L>
    {
        final Node<T, L> tNode;
        final Node<TrieSet.InSet, Void> sNode;

        public IntersectionNode(Node<T, L> tNode, Node<TrieSet.InSet, Void> sNode)
        {
            super(tNode.parentLink);
            this.tNode = tNode;
            this.sNode = sNode;
        }

        public Remaining startIteration()
        {
            Remaining sHas = sNode.startIteration();
            if (sHas == null)
                return null;

            return advanceToIntersection(tNode.startIteration());
        }

        public Remaining advanceIteration()
        {
            Remaining sHas = sNode.advanceIteration();
            if (sHas == null)
                return null;
            return advanceToIntersection(tNode.advanceIteration());
        }

        public Remaining advanceToIntersection(Remaining tHas)
        {
            Remaining sHas;
            if (tHas == null)
                return null;
            int sByte = sNode.currentTransition;
            int tByte = tNode.currentTransition;

            while (tByte != sByte)
            {
                if (tByte < sByte)
                {
                    tHas = tNode.advanceIteration();
                    if (tHas == null)
                        return null;
                    tByte = tNode.currentTransition;
                }
                else // (tByte > sByte)
                {
                    sHas = sNode.advanceIteration();
                    if (sHas == null)
                        return null;
                    sByte = sNode.currentTransition;
                }
            }
            currentTransition = sByte;
            return tHas;    // ONE or MULTIPLE
        }

        public Node<T, L> getCurrentChild(L parent)
        {
            Node<TrieSet.InSet, Void> receivedSetNode = sNode.getCurrentChild(null);

            if (receivedSetNode == null)
                return null;    // branch is completely outside the set

            Node<T, L> nn = tNode.getCurrentChild(parent);

            if (nn == null)
                return null;

            if (receivedSetNode.content() == TrieSet.InSet.BRANCH)
                return nn;     // Branch is fully covered, we no longer need to augment nodes there.

            return new IntersectionNode<>(nn, receivedSetNode);
        }

        public T content()
        {
            if (sNode.content().pointIncluded())
                return tNode.content();
            return null;
        }
    }

    protected Cursor<T> cursor()
    {
        return new IntersectionCursor(trie.cursor(), intersectingSet.cursor());
    }

    private class IntersectionCursor implements Cursor<T>
    {
        private final Cursor<T> tCursor;
        private final Cursor<TrieSet.InSet> sCursor;
        int branchLevel = Integer.MAX_VALUE;

        public IntersectionCursor(Cursor<T> tCursor,
                                  Cursor<TrieSet.InSet> sCursor)
        {
            this.tCursor = tCursor;
            this.sCursor = sCursor;
        }

        public int advance()
        {
            int tLevel = tCursor.advance();
            if (sCursor.content().branchCovered())
            {
                if (tLevel > sCursor.level())
                    return tLevel;
                // otherwise we have left the intersection's covered branch
            }
            int sLevel = sCursor.advance();

            return advanceToIntersection(tLevel, sLevel);
        }

        public int ascend() // this is not tested ATM
        {
            int tLevel = tCursor.ascend();
            if (sCursor.content().branchCovered())
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
                        sLevel = sCursor.advance();
                    else // sIncoming > tIncoming
                        tLevel = tCursor.advance();
                }
                else if (sLevel < tLevel)
                {
                    if (sCursor.content().branchCovered())
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
            return sCursor.content().pointIncluded()
                   ? tCursor.content()
                   : null;
        }
    }
}
