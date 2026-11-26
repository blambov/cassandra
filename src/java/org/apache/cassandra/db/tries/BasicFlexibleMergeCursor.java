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

import javax.annotation.Nullable;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// A merge of a cursor with a second one that can be attached at any position of the first, to be walked together
/// with the branch at that position, and is dropped when it is exhausted.
///
/// This has the part of the merge that only needs to advance; [FlexibleMergeCursor] adds skipping.
abstract class BasicFlexibleMergeCursor<C extends BasicCursor<?>, D extends BasicCursor<?>, T> implements BasicCursor<T>
{
    final C c1;
    @Nullable D c2;
    /// The position of c1 at which c2 is attached, and the depth correction for it. c2's positions are converted
    /// with [DepthAdjustedCursor#toAdjustedDepth(long, long, long)].
    long c2rootPosition;
    long c2depthCorrection;
    long currentPosition;

    enum State
    {
        AT_C1,
        AT_C2,
        AT_BOTH,
        C1_ONLY,    // c2 is null
    }
    State state;

    BasicFlexibleMergeCursor(C c1)
    {
        this.c1 = c1;
        this.c2 = null;
        state = State.C1_ONLY;
        currentPosition = c1.getPositionAndAssertFresh();
        // We can't call postAdvance here because the class may not be completely initialized.
        // The concrete class should do that instead
    }

    BasicFlexibleMergeCursor(C c1, D c2)
    {
        c1.assertFresh();
        if (c2 != null)
            c2.assertFresh();
        this.c1 = c1;
        this.c2 = c2;
        this.c2rootPosition = c1.encodedPosition();
        this.c2depthCorrection = 0;
        state = c2 != null ? State.AT_BOTH : State.C1_ONLY;
        currentPosition = c1.encodedPosition();
        if (c2 != null)
            currentPosition = Cursor.unionFlagsMatchingPositions(currentPosition, c2.encodedPosition());
        // We can't call postAdvance here because the class may not be completely initialized.
        // The concrete class should do that instead
    }

    public void addCursor(D c2)
    {
        assert state == State.C1_ONLY : "Attempting to add further cursors to a cursor that already has two sources";
        this.c2 = c2;
        this.c2rootPosition = currentPosition;
        this.c2depthCorrection = Cursor.depthCorrectionValue(currentPosition);
        this.state = State.AT_BOTH;
        currentPosition = Cursor.unionFlags(currentPosition, c2.getPositionAndAssertFresh(), Cursor.FLAGS_MASK);
    }

    abstract long postAdvance(long depth);

    @Override
    public void close()
    {
        try
        {
            c1.close();
        }
        finally
        {
            // Released, but not dropped: [Cursor#close] leaves `tailCursor` callable, and the versions of it in the
            // descendants of [FlexibleMergeCursor] read c2. Only leaving the branch drops it.
            if (c2 != null)
                c2.close();
        }
    }

    /// Release the second source, which is only attached for the span of a branch and dropped when it is left.
    void closeC2()
    {
        if (c2 != null)
        {
            c2.close();
            c2 = null;
        }
    }

    @Override
    public long advance()
    {
        switch (state)
        {
            case C1_ONLY:
                return inC1Only(c1.advance());
            case AT_C1:
                return checkOrder(c1.advance(), c2.encodedPosition());
            case AT_C2:
                return checkOrder(c1.encodedPosition(), c2.advance());
            case AT_BOTH:
                return checkOrder(c1.advance(), c2.advance());
            default:
                throw new AssertionError();
        }
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        switch (state)
        {
            case C1_ONLY:
                return inC1Only(c1.advanceMultiple(receiver));
            // If we are in a branch that's only covered by one of the sources, we can use its advanceMultiple as it is
            // only different from advance if it takes multiple steps down, which does not change the order of the
            // cursors.
            // Since it might ascend, we still have to check the order after the call.
            case AT_C1:
                return checkOrder(c1.advanceMultiple(receiver), c2.encodedPosition());
            case AT_C2:
                return checkOrder(c1.encodedPosition(), c2.advanceMultiple(receiver));
            // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
            case AT_BOTH:
                return checkOrder(c1.advance(), c2.advance());
            default:
                throw new AssertionError();
        }
    }

    long inC1Only(long c1pos)
    {
        return postAdvance(currentPosition = c1pos);
    }

    long checkOrder(long c1pos, long c2posUncorrected)
    {
        if (Cursor.isExhausted(c2posUncorrected))
            return leaveC2(c1pos);

        long c2pos = DepthAdjustedCursor.toAdjustedDepth(c2posUncorrected, c2rootPosition, c2depthCorrection);
        long cmp = Cursor.compare(c1pos, c2pos);
        if (cmp < 0)
        {
            state = State.AT_C1;
            return postAdvance(currentPosition = c1pos);
        }
        if (cmp > 0)
        {
            state = State.AT_C2;
            return postAdvance(currentPosition = c2pos);
        }
        // c1pos == c2pos
        state = State.AT_BOTH;
        currentPosition = Cursor.unionFlagsMatchingPositions(c1pos, c2pos);
        return postAdvance(currentPosition);
    }

    long leaveC2(long c1pos)
    {
        state = State.C1_ONLY;
        closeC2();
        return postAdvance(currentPosition = c1pos);
    }

    @Override
    public long encodedPosition()
    {
        return currentPosition;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return c1.byteComparableVersion();
    }
}
