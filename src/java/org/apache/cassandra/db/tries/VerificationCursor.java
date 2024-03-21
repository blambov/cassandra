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
import java.util.Objects;

import com.google.common.base.Preconditions;

import org.agrona.DirectBuffer;

public class VerificationCursor
{
    /**
     * Verifies:
     * - advance does advance, depth <= prevDepth + 1 and transition is higher than previous at the same depth
     *   (this requires path tracking)
     * - skipTo is not called with earlier or equal position (including lower levels)
     * - maybeSkipTo is not called with earlier position that can't be identified with depth/incomingTransition only
     *   (i.e. seeks to lower depth with an incoming transition that lower than the previous at that depth)
     * - exhausted state is depth = -1, incomingTransition = 0 (or maybe -1? decide!)
     * - start state is depth = 0, incomingTransition = -1 (maybe change to 0?)
     */
    class Walkable<C extends CursorWalkable.Cursor> implements CursorWalkable.Cursor, CursorWalkable.TransitionsReceiver
    {
        final C source;
        final int minDepth;
        int returnedDepth;
        int returnedTransition;
        byte[] path;

        CursorWalkable.TransitionsReceiver chainedReceiver = null;
        boolean advanceMultipleCalledReceiver;

        Walkable(C cursor, int minDepth, int expectedDepth, int expectedTransition)
        {
            this.source = cursor;
            this.minDepth = minDepth;
            this.returnedDepth = expectedDepth;
            this.returnedTransition = expectedTransition;
            this.path = new byte[16];
            Preconditions.checkState(source.depth() == expectedDepth && source.incomingTransition() == expectedTransition,
                                     "Invalid initial depth %d with incoming transition %d (must be %d, %d)",
                                     source.depth(), source.incomingTransition(),
                                     expectedDepth, expectedTransition);
        }


        Walkable(Walkable<C> copyFrom)
        {
            this.source = copyFrom.source;
            this.minDepth = copyFrom.minDepth;
            this.returnedDepth = copyFrom.returnedDepth;
            this.returnedTransition = copyFrom.incomingTransition();
            this.path = Arrays.copyOf(copyFrom.path, copyFrom.path.length);
        }

        @Override
        public int depth()
        {
            Preconditions.checkState(returnedDepth == source.depth(),
                                     "Depth changed without advance: %d -> %d", returnedDepth, source.depth());
            return returnedDepth;
        }

        @Override
        public int incomingTransition()
        {
            Preconditions.checkState(returnedTransition == source.incomingTransition(),
                                     "Transition changed without advance: %d -> %d", returnedTransition, source.incomingTransition());
            return source.incomingTransition();
        }

        @Override
        public int advance()
        {
            return verify(source.advance());
        }

        @Override
        public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
        {
            advanceMultipleCalledReceiver = false;
            chainedReceiver = receiver;
            int depth = source.advanceMultiple(this);
            chainedReceiver = null;
            Preconditions.checkState(!advanceMultipleCalledReceiver || depth == returnedDepth + 1,
                                     "advanceMultiple returned depth %d did not match depth %d after added characters",
                                     depth, returnedDepth + 1);
            return verify(depth);
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            verifySkipRequest(skipDepth, skipTransition);
            return verify(source.skipTo(skipDepth, skipTransition));
        }

        @Override
        public int maybeSkipTo(int skipDepth, int skipTransition)
        {
            if (skipDepth < returnedDepth)
                verifySkipRequest(skipDepth, skipTransition);
            return verify(source.maybeSkipTo(skipDepth, skipTransition));
        }

        private void verifySkipRequest(int skipDepth, int skipTransition)
        {
            Preconditions.checkState(skipDepth <= returnedDepth + 1,
                                     "Skip descends more than one level: %d -> %d",
                                     returnedDepth,
                                     skipDepth);
            if (skipDepth <= returnedDepth)
                Preconditions.checkState(path[skipDepth - minDepth] < skipTransition,
                                         "Skip goes backwards to %d at depth %d where it already visited %d",
                                         skipTransition, skipDepth, path[skipDepth - minDepth]);

        }

        private int verify(int depth)
        {
            Preconditions.checkState(depth <= returnedDepth + 1,
                                     "Cursor advanced more than one level: %d -> %d",
                                     returnedDepth,
                                     depth);
            Preconditions.checkState(depth < 0 || depth >= minDepth,
                                     "Cursor ascended to depth %d beyond its minimum depth %d",
                                     depth, minDepth);
            final int transition = source.incomingTransition();
            if (depth < 0)
            {
                Preconditions.checkState(depth == -1 && transition == 0,
                                         "Cursor exhausted state should be -1, 0 but was %d, %d",
                                         depth, transition);
            }
            else if (depth <= returnedDepth)
            {
                Preconditions.checkState(path[depth - minDepth] < transition,
                                         "Cursor went backwards to %d at depth %d where it already visited %d",
                                         transition, depth, path[depth - minDepth]);
            }
            returnedDepth = depth;
            returnedTransition = transition;
            addByte(returnedTransition, depth - 1);
            return depth;
        }

        @Override
        public Walkable<C> duplicate()
        {
            return new Walkable<>(this);
        }

        @Override
        public void addPathByte(int nextByte)
        {
            addByte(nextByte, returnedDepth++);
            returnedTransition = nextByte;
            if (chainedReceiver != null)
                chainedReceiver.addPathByte(nextByte);
        }

        private void addByte(int nextByte, int depth)
        {
            int index = depth - minDepth;
            if (index >= path.length)
                path = Arrays.copyOf(path, path.length * 2);
            path[index] = (byte) nextByte;
        }

        @Override
        public void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            for (int i = 0; i < count; ++i)
                addByte(buffer.getByte(pos + i), returnedDepth + i);
            returnedDepth += count;
            returnedTransition = buffer.getByte(pos + count - 1) & 0xFF;
            if (chainedReceiver != null)
                chainedReceiver.addPathBytes(buffer, pos, count);
        }
    }

    class WithContent<T, C extends TrieImpl.Cursor<T>> extends Walkable<C> implements TrieImpl.Cursor<T>
    {
        WithContent(C source, int minDepth, int expectedDepth, int expectedTransition)
        {
            super(source, minDepth, expectedDepth, expectedTransition);
        }

        WithContent(WithContent<T, C> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public T content()
        {
            return source.content();
        }

        @Override
        public WithContent<T, C> duplicate()
        {
            return new WithContent<>(this);
        }
    }

    class Trie<T> extends WithContent<T, TrieImpl.Cursor<T>> implements TrieImpl.Cursor<T>
    {
        Trie(TrieImpl.Cursor<T> source)
        {
            this(source, 0, 0, -1);
        }

        Trie(TrieImpl.Cursor<T> source, int minDepth, int expectedDepth, int expectedTransition)
        {
            super(source, minDepth, expectedDepth, expectedTransition);
        }

        Trie(Trie<T> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public Trie<T> duplicate()
        {
            return new Trie<>(this);
        }
    }

    class Range<M extends RangeTrie.RangeMarker<M>> extends WithContent<M, RangeTrieImpl.Cursor<M>> implements RangeTrieImpl.Cursor<M>
    {
        M currentCoveringState = null;
        boolean permitEqualCoveringState = false;

        Range(RangeTrieImpl.Cursor<M> source)
        {
            this(source, 0, 0, -1);
        }

        Range(RangeTrieImpl.Cursor<M> source, int minDepth, int expectedDepth, int expectedTransition)
        {
            super(source, minDepth, expectedDepth, expectedTransition);
        }

        Range(Range<M> copyFrom)
        {
            super(copyFrom);
            this.currentCoveringState = copyFrom.currentCoveringState;
            this.permitEqualCoveringState = copyFrom.permitEqualCoveringState;
        }

        @Override
        public int advance()
        {
            return verifyState(source.advance());
        }

        @Override
        public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
        {
            return verifyState(source.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return verifySkipState(source.skipTo(skipDepth, skipTransition));
        }

        @Override
        public M coveringState()
        {
            Preconditions.checkState(currentCoveringState == source.coveringState(),
                                     "Covering state changed without advance: %s -> %s", currentCoveringState, source.coveringState());
            // == above is correct, we do not want covering state to be recreated
            return currentCoveringState;
        }

        @Override
        public M content()
        {
            return source.content();
        }

        private int verifyState(int depth)
        {
            final M coveringState = source.coveringState();
            if (currentCoveringState != coveringState)
            {
                boolean equal = Objects.equals(currentCoveringState, coveringState);
                Preconditions.checkState(equal,
                                         "Unexpected change to covering state: %s -> %s",
                                         currentCoveringState, coveringState);

                if (permitEqualCoveringState)
                {
                    currentCoveringState = coveringState;
                    permitEqualCoveringState = false;
                }
                else
                {
                    Preconditions.checkState(currentCoveringState == coveringState,
                                             "Covering state is equal but a different object. This is not allowed for performance reasons.");
                }
            }

            M content = source.content();
            if (content != null)
            {
                Preconditions.checkState(Objects.equals(currentCoveringState, content.leftSideAsCovering()),
                                         "Range end does not close covering state: %s -> %s",
                                         currentCoveringState, content.leftSideAsCovering());
                currentCoveringState = content.rightSideAsCovering();
                permitEqualCoveringState = true;
            }
            return depth;
        }

        private int verifySkipState(int depth)
        {
            // The covering state information is invalidated by a skip.
            currentCoveringState = source.coveringState();
            permitEqualCoveringState = false;
            return verifyState(depth);
        }

        @Override
        public Range<M> duplicate()
        {
            return new Range<>(this);
        }
    }

    class DeletionAware<T, D extends RangeTrie.RangeMarker<D>>
    extends WithContent<T, DeletionAwareTrieImpl.Cursor<T, D>>
    implements DeletionAwareTrieImpl.Cursor<T, D>
    {
        int deletionBranchDepth;

        DeletionAware(DeletionAwareTrieImpl.Cursor<T, D> source)
        {
            this(source, 0, 0, -1);
        }

        DeletionAware(DeletionAwareTrieImpl.Cursor<T, D> source, int minDepth, int expectedDepth, int expectedTransition)
        {
            super(source, minDepth, expectedDepth, expectedTransition);
            this.deletionBranchDepth = -1;
        }

        public DeletionAware(DeletionAware<T, D> copyFrom)
        {
            super(copyFrom);
            this.deletionBranchDepth = copyFrom.deletionBranchDepth;
        }

        @Override
        public int depth()
        {
            Preconditions.checkState(returnedDepth == source.depth(),
                                     "Depth changed without advance: %d -> %d", returnedDepth, source.depth());
            return returnedDepth;
        }

        @Override
        public int incomingTransition()
        {
            return source.incomingTransition();
        }

        @Override
        public int advance()
        {
            return verifyDeletionBranch(source.advance());
        }

        @Override
        public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
        {
            return verifyDeletionBranch(source.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return verifyDeletionBranch(source.skipTo(skipDepth, skipTransition));
        }

        @Override
        public RangeTrieImpl.Cursor<D> deletionBranch()
        {
            // deletionBranch must be verified
            return new Range<>(source.deletionBranch(), returnedDepth, returnedDepth, returnedTransition);
        }

        @Override
        public T content()
        {
            return source.content();
        }

        int verifyDeletionBranch(int depth)
        {
            if (depth <= deletionBranchDepth)
                deletionBranchDepth = -1;

            var deletionBranch = source.deletionBranch();
            if (deletionBranch != null)
            {
                Preconditions.checkState(deletionBranchDepth == -1,
                                         "Deletion branch at depth %d covered by another deletion branch at parent depth %d",
                                         depth, deletionBranchDepth);
                Preconditions.checkState(deletionBranch.depth() == depth,
                                         "Deletion branch depth %d does not match cursor depth %d",
                                         deletionBranch.depth(), depth);
                Preconditions.checkState(deletionBranch.incomingTransition() == source.incomingTransition(),
                                         "Deletion branch initial transition %d does not match cursor transition %d",
                                         deletionBranch.incomingTransition(), source.incomingTransition());
                Preconditions.checkState(deletionBranch.coveringState() == null,
                                         "Deletion branch starts with active deletion %s",
                                         deletionBranch.coveringState());
                deletionBranch.skipTo(-1, 0);
                Preconditions.checkState(deletionBranch.coveringState() == null,
                                         "Deletion branch ends with active deletion %s",
                                         deletionBranch.coveringState());
                deletionBranchDepth = depth;
            }
            return depth;
        }

        @Override
        public DeletionAware<T, D> duplicate()
        {
            return new DeletionAware<>(this);
        }
    }
}
