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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// A merged view of two basic cursors.
///
/// This is accomplished by walking the two cursors in parallel; the merged cursor takes the position and features of the
/// smaller and advances with it; when the two cursors are equal, both are advanced.
///
/// Crucial for the efficiency of this is the fact that when they are advanced like this, we can compare cursors'
/// positions by their `depth` descending and then `incomingTransition` ascending.
/// See [Trie.md](./Trie.md) for further details.
///
/// This has the part of the merge that only needs to advance; [MergeCursor] adds skipping.
abstract class BasicMergeCursor<C extends BasicCursor<?>, D extends BasicCursor<?>, R> implements BasicCursor<R>
{
    final C c1;
    final D c2;

    boolean atC1;
    boolean atC2;
    long currentPosition;

    BasicMergeCursor(C c1, D c2)
    {
        c1.assertFresh();
        c2.assertFresh();
        this.c1 = c1;
        this.c2 = c2;
        atC1 = atC2 = true;
        currentPosition = Cursor.unionFlagsMatchingPositions(c1.encodedPosition(), c2.encodedPosition());
    }

    @Override
    public void close()
    {
        try
        {
            c1.close();
        }
        finally
        {
            c2.close();
        }
    }

    @Override
    public long advance()
    {
        return checkOrder(atC1 ? c1.advance() : c1.encodedPosition(),
                          atC2 ? c2.advance() : c2.encodedPosition());
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        // While we are on a shared position, we must descend one byte at a time to maintain the cursor ordering.
        if (atC1 && atC2)
            return checkOrder(c1.advance(), c2.advance());

        // If we are in a branch that's only covered by one of the sources, we can use its advanceMultiple as it is
        // only different from advance if it takes multiple steps down, which does not change the order of the
        // cursors.
        // Since it might ascend, we still have to check the order after the call.
        if (atC1)
            return checkOrder(c1.advanceMultiple(receiver), c2.encodedPosition());
        else // atC2
            return checkOrder(c1.encodedPosition(), c2.advanceMultiple(receiver));
    }

    long checkOrder(long c1pos, long c2pos)
    {
        long cmp = Cursor.compare(c1pos, c2pos);
        atC1 = cmp <= 0;
        atC2 = cmp >= 0;
        if (atC1 && atC2)
            return currentPosition = Cursor.unionFlagsMatchingPositions(c1pos, c2pos);
        else
            return currentPosition = atC1 ? c1pos : c2pos;
    }

    @Override
    public long encodedPosition()
    {
        return currentPosition;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        assert c1.byteComparableVersion() == c2.byteComparableVersion() :
            "Merging cursors with different byteComparableVersions: " +
            c1.byteComparableVersion() + " vs " + c2.byteComparableVersion();
        return c1.byteComparableVersion();
    }
}
