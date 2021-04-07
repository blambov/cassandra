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

import java.util.Arrays;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;


/**
 * TrieSet representing the range between two keys.
 *
 * The keys must be correctly ordered, including with respect to the `includeLeft` and `includeRight` constraints.
 * (i.e. RangeTrieSet(x, false, x, false) is an invalid call but RangeTrieSet(x, true, x, false) is inefficient
 * but fine for an empty set).
 */
public class RangeTrieSet extends TrieSet
{
    /** Left-side boundary. The characters of this are requested as we descend along the left-side boundary. */
    private final ByteComparable left;

    /** Right-side boundary. The characters of this are requested as we descend along the right-side boundary. */
    private final ByteComparable right;

    private final boolean includeLeft;
    private final boolean includeRight;

    RangeTrieSet(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        this.left = left;
        this.includeLeft = includeLeft;
        this.right = right;
        this.includeRight = includeRight;
    }

    protected Cursor<InSet> cursor()
    {
        return new RangeCursor();
    }

    private class RangeCursor implements Cursor<InSet>
    {
        private int[] backlog;
        int backlogPos;
        private ByteSource remainingLeftLimit;
        private ByteSource remainingRightLimit;
        boolean atLeftLimit;
        boolean atRightLimit;
        boolean rightLimitDone;
        int leftLimitNext;
        int rightLimitNext;
        int transitionAtRightLevel;
        private int incomingTransition;
        private int level;
        InSet inSet;


        private RangeCursor()
        {
            backlog = new int[32];
            backlogPos = 0;
            level = 0;
            atLeftLimit = left != null;
            if (atLeftLimit)
            {
                remainingLeftLimit = left.asComparableBytes(BYTE_COMPARABLE_VERSION);
                leftLimitNext = remainingLeftLimit.next();
                inSet = InSet.PREFIX;
                if (leftLimitNext == ByteSource.END_OF_STREAM)
                {
                    atLeftLimit = false;
                    if (includeLeft)
                        inSet = InSet.CONTAINED;
                    transitionAtRightLevel = -1;
                }
            }
            else
            {
                inSet = InSet.CONTAINED;
                transitionAtRightLevel = -1;
            }

            atRightLimit = right != null;
            if (atRightLimit)
            {
                remainingRightLimit = right.asComparableBytes(BYTE_COMPARABLE_VERSION);
                rightLimitNext = remainingRightLimit.next();
                if (rightLimitNext == ByteSource.END_OF_STREAM)
                {
                    rightLimitDone = true;
                    assert !atLeftLimit;
                    atRightLimit = false;
                    if (!includeRight)
                    {
                        level = -1;
                        inSet = InSet.PREFIX;
                        return;
                    }
                }
                else
                    rightLimitDone = false;
            }
            else
            {
                // else we exhaust the backlog at level -1 and terminate before any continueAlongRight is called
                rightLimitNext = 255;
                rightLimitDone = true;
            }

            incomingTransition = -1;
            if (!atLeftLimit && !atRightLimit && rightLimitNext >= 0 && inSet == InSet.CONTAINED)
                inSet = InSet.BRANCH;
        }


        public int advance()
        {
            if (atLeftLimit)
            {
                if (atRightLimit)
                    return descendAlongBoth();
                else
                    return descendAlongLeft();
            }

            if (processBacklog())
                return level;

            return continueAlongRight();
        }

        private int descendAlongBoth()
        {
            assert rightLimitNext >= leftLimitNext;
            int next = leftLimitNext;
            leftLimitNext = remainingLeftLimit.next();
            if (rightLimitNext == next)
                rightLimitNext = remainingRightLimit.next();
            else
            {
                transitionAtRightLevel = next + 1;
                atRightLimit = false;
            }

            incomingTransition = next;
            if (leftLimitNext != ByteSource.END_OF_STREAM)
            {
                inSet = InSet.PREFIX;
            }
            else
            {
                atLeftLimit = false;
                if (rightLimitNext == ByteSource.END_OF_STREAM)
                {
                    if (includeLeft && includeRight)
                        inSet = InSet.CONTAINED;
                    else
                        return -1;
                }
                inSet = includeLeft ? InSet.BRANCH : InSet.PREFIX;//: InSet.BRANCH_EXCLUDING;
            }
            return ++level;
        }

        private int descendAlongLeft()
        {
            int next = leftLimitNext;
            leftLimitNext = remainingLeftLimit.next();
            addBacklog(next + 1);

            incomingTransition = next;
            if (leftLimitNext != ByteSource.END_OF_STREAM)
            {
                inSet = InSet.PREFIX;
            }
            else
            {
                atLeftLimit = false;
                inSet = includeLeft ? InSet.BRANCH : InSet.PREFIX;//: InSet.BRANCH_EXCLUDING;
            }
            return ++level;
        }

        private boolean processBacklog()
        {
            while (backlogPos > 0)
            {
                incomingTransition = backlog[backlogPos - 1]++;
                if (incomingTransition < 256)
                {
                    inSet = InSet.BRANCH;
                    return true;
                }
                --backlogPos;
                --level;
            }
            return false;
        }

        private int continueAlongRight()
        {
            if (transitionAtRightLevel < 0)
            {
                transitionAtRightLevel = 0;
                ++level;
            }
            incomingTransition = transitionAtRightLevel++;

            if (incomingTransition < rightLimitNext)
            {
                inSet = InSet.BRANCH;
                return level;
            }
            else // (incomingTransition == rightLimitNext)
            {
                if (rightLimitDone)
                    return -1;

                rightLimitNext = remainingRightLimit.next();
                if (rightLimitNext == ByteSource.END_OF_STREAM)
                {
                    rightLimitDone = true;
                    if (!includeRight)
                        return -1;
                }
                transitionAtRightLevel = -1;
                inSet = InSet.CONTAINED;
                return level;
            }
        }

        void addBacklog(int transition)
        {
            if (backlogPos == backlog.length)
                backlog = Arrays.copyOf(backlog, backlogPos * 2);
            backlog[backlogPos++] = transition;
        }

        public int ascend()
        {
            atLeftLimit = false;
            if (processBacklog())
                return level;
            if (transitionAtRightLevel < 0)
                return -1;
            return continueAlongRight();
        }

        public int level()
        {
            return level;
        }

        public int incomingTransition()
        {
            return incomingTransition;
        }

        public InSet content()
        {
            return inSet;
        }
    }


    // TODO: Change to start/stop sets when nodes are taken out of the picture

    public <L> Node<InSet, L> root()
    {
        return makeNode(left == null ? null : left.asComparableBytes(Trie.BYTE_COMPARABLE_VERSION),
                        left != null,
                        right == null ? null : right.asComparableBytes(Trie.BYTE_COMPARABLE_VERSION),
                        right != null,
                        null);
    }

    private <L> Node<InSet, L> makeNode(ByteSource lLimit, boolean atLLimit, ByteSource rLimit, boolean atRLimit, L parentLink)
    {
        // We only have a constraint on the branch if we are at one or both boundaries.
        // If the node falls completely between them, the whole branch (at any depth) is in the set.
        if (!atLLimit && !atRLimit)
            return parentLink == null ? (Node<InSet, L>) FULL : new FullNode<>(parentLink);

        return new RangeNode<>(lLimit, atLLimit, rLimit, atRLimit, parentLink);
    }

    class FullNode<L> extends NoChildrenNode<InSet, L>
    {
        FullNode(L parent)
        {
            super(parent);
        }

        public InSet content()
        {
            return InSet.BRANCH;
        }
    }

    class RangeNode<L> extends Node<InSet, L>
    {
        /** Byte at the left boundary, inclusive. */
        final int llimit;
        final ByteSource remainingLLimit;
        /** Byte at the right boundary, inclusive. */
        final int rlimit;
        final ByteSource remainingRLimit;
        /** Whether or not we are descending along the left boundary. */
        final boolean atLLimit;
        /** Whether or not we are descending along the right boundary. */
        final boolean atRLimit;

        /** Whether the current path is in the covered set. */
        final InSet inSet;

        RangeNode(ByteSource remainingLLimit, boolean atLLimit, ByteSource remainingRLimit, boolean atRLimit, L parentLink)
        {
            super(parentLink);
            int llimit = 0;
            boolean inSet = true;
            if (atLLimit)
            {
                llimit = remainingLLimit.next();
                if (llimit == ByteSource.END_OF_STREAM)
                {
                    atLLimit = false;
                    llimit = 0;
                    inSet &= includeLeft; // The current path matches left boundary
                }
                else
                    inSet = false;  // The current path is a prefix of the left boundary, ie. smaller.
            }
            int rlimit = 255;
            if (atRLimit)
            {
                rlimit = remainingRLimit.next();
                if (rlimit == ByteSource.END_OF_STREAM)
                {
                    atRLimit = false;
                    rlimit = -1;    // no op, added for clarity. Node should have no children.
                    inSet &= includeRight; // The current path matches right boundary
                }
            }
            assert llimit <= rlimit || rlimit == -1 : "Bound " + left + " not <= " + right + " in range " + llimit + " vs " + rlimit;

            this.llimit = llimit;
            this.remainingLLimit = remainingLLimit;
            this.rlimit = rlimit;
            this.remainingRLimit = remainingRLimit;
            this.atLLimit = atLLimit;
            this.atRLimit = atRLimit;
            this.inSet = inSet ? InSet.CONTAINED : InSet.PREFIX;
        }

        public Node<InSet, L> getCurrentChild(L parentLink)
        {
            return makeNode(remainingLLimit, atLLimit && (currentTransition == llimit),
                            remainingRLimit, atRLimit && (currentTransition == rlimit),
                            parentLink);
        }

        public Remaining startIteration()
        {
            currentTransition = llimit;
            return currentTransition <= rlimit ? Remaining.MULTIPLE : null;
        }

        public Remaining advanceIteration()
        {
            return ++currentTransition <= rlimit ? Remaining.MULTIPLE : null;
        }

        public InSet content()
        {
            return inSet;
        }
    }

}
