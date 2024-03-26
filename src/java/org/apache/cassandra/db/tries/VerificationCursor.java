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

import com.google.common.base.Preconditions;

import org.agrona.DirectBuffer;
import org.apache.cassandra.utils.Hex;

public interface VerificationCursor
{
    int EXHAUSTED_DEPTH = -1;
    int EXHAUSTED_TRANSITION = -1;
    int INITIAL_TRANSITION = -1;


    /**
     * Verifies:
     * - advance does advance, depth <= prevDepth + 1 and transition is higher than previous at the same depth
     *   (this requires path tracking)
     * - skipTo is not called with earlier or equal position (including lower levels)
     * - maybeSkipTo is not called with earlier position that can't be identified with depth/incomingTransition only
     *   (i.e. seeks to lower depth with an incoming transition that lower than the previous at that depth)
     * - exhausted state is depth = -1, incomingTransition = -1 (maybe change to 0?)
     * - start state is depth = 0, incomingTransition = -1 (maybe change to 0?)
     */
    class Walkable<C extends CursorWalkable.Cursor> implements CursorWalkable.Cursor, CursorWalkable.TransitionsReceiver
    {
        final C source;
        final int minDepth;
        int returnedDepth;
        int returnedTransition;
        byte[] path;

        transient CursorWalkable.TransitionsReceiver chainedReceiver = null;
        transient boolean advanceMultipleCalledReceiver;

        Walkable(C cursor, int minDepth, int expectedDepth, int expectedTransition)
        {
            this.source = cursor;
            this.minDepth = minDepth;
            this.returnedDepth = expectedDepth;
            this.returnedTransition = expectedTransition;
            this.path = new byte[16];
            Preconditions.checkState(source.depth() == expectedDepth && source.incomingTransition() == expectedTransition,
                                     "Invalid initial depth %s with incoming transition %s (must be %s, %s)",
                                     source.depth(), source.incomingTransition(),
                                     expectedDepth, expectedTransition);
        }


        @SuppressWarnings("unchecked")
        Walkable(Walkable<C> copyFrom)
        {
            this.source = (C) copyFrom.source.duplicate();
            this.minDepth = copyFrom.minDepth;
            this.returnedDepth = copyFrom.returnedDepth;
            this.returnedTransition = copyFrom.returnedTransition;
            this.path = Arrays.copyOf(copyFrom.path, copyFrom.path.length);
        }

        @Override
        public int depth()
        {
            Preconditions.checkState(returnedDepth == source.depth(),
                                     "Depth changed without advance: %s -> %s", returnedDepth, source.depth());
            return returnedDepth;
        }

        @Override
        public int incomingTransition()
        {
            Preconditions.checkState(returnedTransition == source.incomingTransition(),
                                     "Transition changed without advance: %s -> %s", returnedTransition, source.incomingTransition());
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
                                     "advanceMultiple returned depth %s did not match depth %s after added characters",
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
                                     "Skip descends more than one level: %s -> %s",
                                     returnedDepth,
                                     skipDepth);
            if (skipDepth <= returnedDepth && skipDepth > minDepth)
                Preconditions.checkState(getByte(skipDepth) < skipTransition,
                                         "Skip goes backwards to %s at depth %s where it already visited %s",
                                         skipTransition, skipDepth, getByte(skipDepth));

        }

        private int verify(int depth)
        {
            Preconditions.checkState(depth <= returnedDepth + 1,
                                     "Cursor advanced more than one level: %s -> %s",
                                     returnedDepth,
                                     depth);
            Preconditions.checkState(depth < 0 || depth > minDepth,
                                     "Cursor ascended to depth %s beyond its minimum depth %s",
                                     depth, minDepth);
            final int transition = source.incomingTransition();
            if (depth < 0)
            {
                Preconditions.checkState(depth == EXHAUSTED_DEPTH && transition == EXHAUSTED_TRANSITION,
                                         "Cursor exhausted state should be %s, %s but was %s, %s",
                                         EXHAUSTED_DEPTH, EXHAUSTED_TRANSITION,
                                         depth, transition);
            }
            else if (depth <= returnedDepth)
            {
                Preconditions.checkState(getByte(depth) < transition,
                                         "Cursor went backwards to %s at depth %s where it already visited %s",
                                         transition, depth, getByte(depth));
            }
            returnedDepth = depth;
            returnedTransition = transition;
            if (depth >= 0)
                addByte(returnedTransition, depth);
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
            addByte(nextByte, ++returnedDepth);
            returnedTransition = nextByte;
            if (chainedReceiver != null)
                chainedReceiver.addPathByte(nextByte);
        }

        private void addByte(int nextByte, int depth)
        {
            int index = depth - minDepth - 1;
            if (index >= path.length)
                path = Arrays.copyOf(path, path.length * 2);
            path[index] = (byte) nextByte;
        }

        private int getByte(int depth)
        {
            return path[depth - minDepth - 1] & 0xFF;
        }

        @Override
        public void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            for (int i = 0; i < count; ++i)
                addByte(buffer.getByte(pos + i), returnedDepth + 1 + i);
            returnedDepth += count;
            returnedTransition = buffer.getByte(pos + count - 1) & 0xFF;
            if (chainedReceiver != null)
                chainedReceiver.addPathBytes(buffer, pos, count);
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder();
            builder.append(source.getClass().getTypeName()
                                 .replace(source.getClass().getPackageName() + '.', ""));
            if (returnedDepth < 0)
            {
                builder.append(" exhausted");
            }
            else
            {
                builder.append(" at ");
                builder.append(Hex.bytesToHex(path, 0, returnedDepth - minDepth));
            }
            return builder.toString();
        }
    }

    abstract class WithContent<T, C extends TrieImpl.Cursor<T>> extends Walkable<C> implements TrieImpl.Cursor<T>
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
        public abstract WithContent<T, C> duplicate();
    }

    class Deterministic<T> extends WithContent<T, TrieImpl.Cursor<T>> implements TrieImpl.Cursor<T>
    {
        Deterministic(TrieImpl.Cursor<T> source)
        {
            this(source, 0, 0, INITIAL_TRANSITION);
        }

        Deterministic(TrieImpl.Cursor<T> source, int minDepth, int expectedDepth, int expectedTransition)
        {
            super(source, minDepth, expectedDepth, expectedTransition);
        }

        Deterministic(Deterministic<T> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public Deterministic<T> duplicate()
        {
            return new Deterministic<>(this);
        }
    }

    class NonDeterministic<T extends NonDeterministicTrie.Mergeable<T>>
    extends WithContent<T, NonDeterministicTrieImpl.Cursor<T>>
    implements NonDeterministicTrieImpl.Cursor<T>
    {
        NonDeterministic(NonDeterministicTrieImpl.Cursor<T> source)
        {
            this(source, 0, 0, INITIAL_TRANSITION);
        }

        NonDeterministic(NonDeterministicTrieImpl.Cursor<T> source, int minDepth, int expectedDepth, int expectedTransition)
        {
            super(source, minDepth, expectedDepth, expectedTransition);
        }

        NonDeterministic(NonDeterministic<T> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> alternateBranch()
        {
            var alternate = source.alternateBranch();
            if (alternate == null)
                return null;
            return new NonDeterministic<>(alternate, returnedDepth, returnedDepth, returnedTransition);
        }

        @Override
        public NonDeterministic<T> duplicate()
        {
            return new NonDeterministic<>(this);
        }
    }

    abstract class WithRanges<M extends RangeTrie.RangeMarker<M>, C extends RangeTrieImpl.Cursor<M>>
    extends WithContent<M, C>
    implements RangeTrieImpl.Cursor<M>
    {
        M currentCoveringState = null;
        M nextCoveringState = null;

        WithRanges(C source, int minDepth, int expectedDepth, int expectedTransition)
        {
            super(source, minDepth, expectedDepth, expectedTransition);
        }

        WithRanges(WithRanges<M, C> copyFrom)
        {
            super(copyFrom);
            this.currentCoveringState = copyFrom.currentCoveringState;
            this.nextCoveringState = copyFrom.nextCoveringState;
        }

        @Override
        public int advance()
        {
            currentCoveringState = nextCoveringState;
            return verifyState(super.advance());
        }

        @Override
        public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
        {
            currentCoveringState = nextCoveringState;
            return verifyState(super.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return verifySkipState(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public M coveringState()
        {
            Preconditions.checkState(currentCoveringState == source.coveringState(),
                                     "Covering state changed without advance: %s -> %s. %s",
                                     currentCoveringState, source.coveringState(),
                                     agree(currentCoveringState, source.coveringState())
                                     ? "The values are equal but different object. This is not permitted for performance reasons."
                                     : "");
            // == above is correct, we do not want covering state to be recreated
            return currentCoveringState;
        }

        boolean agree(M left, M right)
        {
            return left == right || left != null && left.agreesWith(right);
        }

        private int verifyState(int depth)
        {
//            System.out.println(String.format("%s covering state %s content %s",
//                                             this, currentCoveringState, source.content()));
            M coveringState = source.coveringState();
            boolean equal = agree(currentCoveringState, coveringState);
            Preconditions.checkState(equal,
                                     "Unexpected change to covering state: %s -> %s",
                                     currentCoveringState, coveringState);
            currentCoveringState = coveringState;
//
//            Preconditions.checkState(currentCoveringState == coveringState,
//                                     "Covering state is equal but a different object. This is not allowed for performance reasons.");

            M content = source.content();
            if (content != null)
            {
                Preconditions.checkState(agree(currentCoveringState, content.leftSideAsCovering()),
                                         "Range end %s does not close covering state %s",
                                         content.leftSideAsCovering(), currentCoveringState);
                nextCoveringState = content.rightSideAsCovering();
            }

            if (depth < 0)
                verifyEndState();
            return depth;
        }

        void verifyEndState()
        {
            Preconditions.checkState(currentCoveringState == null,
                                     "End state is not null: %s", currentCoveringState);
        }

        private int verifySkipState(int depth)
        {
            // The covering state information is invalidated by a skip.
            currentCoveringState = source.coveringState();
            nextCoveringState = currentCoveringState;
            return verifyState(depth);
        }

        public abstract WithRanges<M, C> duplicate();
    }

    class Range<M extends RangeTrie.RangeMarker<M>> extends WithRanges<M, RangeTrieImpl.Cursor<M>> implements RangeTrieImpl.Cursor<M>
    {
        Range(RangeTrieImpl.Cursor<M> source)
        {
            this(source, 0, 0, INITIAL_TRANSITION);
        }

        Range(RangeTrieImpl.Cursor<M> source, int minDepth, int expectedDepth, int expectedTransition)
        {
            super(source, minDepth, expectedDepth, expectedTransition);
        }

        Range(Range<M> copyFrom)
        {
            super(copyFrom);
        }

        @Override
        public Range<M> duplicate()
        {
            return new Range<>(this);
        }
    }

    class TrieSet extends WithRanges<TrieSetImpl.RangeState, TrieSetImpl.Cursor> implements TrieSetImpl.Cursor
    {
        TrieSet(TrieSetImpl.Cursor source)
        {
            this(source, 0, 0, INITIAL_TRANSITION);
            // start state can be non-null for sets
            currentCoveringState = source.coveringState();
            nextCoveringState = source.content() != null ? source.content().rightSideAsCovering() : currentCoveringState;
        }

        TrieSet(TrieSetImpl.Cursor source, int minDepth, int expectedDepth, int expectedTransition)
        {
            super(source, minDepth, expectedDepth, expectedTransition);
        }

        TrieSet(TrieSet copyFrom)
        {
            super(copyFrom);
        }

        @Override
        void verifyEndState()
        {
            // end state can be non-null for sets
        }

        @Override
        public TrieSetImpl.RangeState state()
        {
            return source.state();
        }

        @Override
        public TrieSet duplicate()
        {
            return new TrieSet(this);
        }
    }

    class DeletionAware<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends WithContent<T, DeletionAwareTrieImpl.Cursor<T, D>>
    implements DeletionAwareTrieImpl.Cursor<T, D>
    {
        int deletionBranchDepth;

        DeletionAware(DeletionAwareTrieImpl.Cursor<T, D> source)
        {
            this(source, 0, 0, INITIAL_TRANSITION);
        }

        DeletionAware(DeletionAwareTrieImpl.Cursor<T, D> source, int minDepth, int expectedDepth, int expectedTransition)
        {
            super(source, minDepth, expectedDepth, expectedTransition);
            this.deletionBranchDepth = -1;
            verifyDeletionBranch(expectedDepth);
        }

        public DeletionAware(DeletionAware<T, D> copyFrom)
        {
            super(copyFrom);
            this.deletionBranchDepth = copyFrom.deletionBranchDepth;
        }

        @Override
        public int incomingTransition()
        {
            return source.incomingTransition();
        }

        @Override
        public int advance()
        {
            return verifyDeletionBranch(super.advance());
        }

        @Override
        public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
        {
            return verifyDeletionBranch(super.advanceMultiple(receiver));
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            return verifyDeletionBranch(super.skipTo(skipDepth, skipTransition));
        }

        @Override
        public RangeTrieImpl.Cursor<D> deletionBranch()
        {
            // deletionBranch must be verified
            final RangeTrieImpl.Cursor<D> deletionBranch = source.deletionBranch();
            if (deletionBranch == null)
                return null;
            return new Range<>(deletionBranch, returnedDepth, returnedDepth, returnedTransition);
        }

        int verifyDeletionBranch(int depth)
        {
            if (depth <= deletionBranchDepth)
                deletionBranchDepth = -1;

            var deletionBranch = source.deletionBranch();
            if (deletionBranch != null)
            {
                Preconditions.checkState(deletionBranchDepth == -1,
                                         "Deletion branch at depth %s covered by another deletion branch at parent depth %s",
                                         depth, deletionBranchDepth);
                Preconditions.checkState(deletionBranch.depth() == depth,
                                         "Deletion branch depth %s does not match cursor depth %s",
                                         deletionBranch.depth(), depth);
                Preconditions.checkState(deletionBranch.incomingTransition() == source.incomingTransition(),
                                         "Deletion branch initial transition %s does not match cursor transition %s",
                                         deletionBranch.incomingTransition(), source.incomingTransition());
                Preconditions.checkState(deletionBranch.coveringState() == null,
                                         "Deletion branch starts with active deletion %s",
                                         deletionBranch.coveringState());
                deletionBranch.skipTo(EXHAUSTED_DEPTH, EXHAUSTED_TRANSITION);
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
