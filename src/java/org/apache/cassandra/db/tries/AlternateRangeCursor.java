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

import java.util.Objects;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class AlternateRangeCursor<T extends NonDeterministicTrie.Mergeable<T>> implements NonDeterministicTrieImpl.Cursor<T>
{
    ByteSource lsrc;
    ByteSource rsrc;

    int lnext;
    int rnext;
    int incomingTransition;
    int depth;

    final T leftValue;
    final T rightValue;

    AlternateRangeCursor(ByteComparable left, T leftValue, ByteComparable right, T rightValue)
    {
        lsrc = left.asComparableBytes(TrieImpl.BYTE_COMPARABLE_VERSION);
        rsrc = right.asComparableBytes(TrieImpl.BYTE_COMPARABLE_VERSION);
        this.leftValue = leftValue;
        this.rightValue = rightValue;
        lnext = lsrc.next();
        rnext = rsrc.next();
        depth = 0;
        incomingTransition = -1;
    }

    AlternateRangeCursor(AlternateRangeCursor<T> copyFrom)
    {
        ByteSource.Duplicatable dupe = ByteSource.duplicatable(copyFrom.lsrc);
        copyFrom.lsrc = dupe;
        lsrc = dupe.duplicate();
        dupe = ByteSource.duplicatable(copyFrom.rsrc);
        copyFrom.rsrc = dupe;
        rsrc = dupe.duplicate();
        lnext = copyFrom.lnext;
        rnext = copyFrom.rnext;
        incomingTransition = copyFrom.incomingTransition;
        depth = copyFrom.depth;
        leftValue = copyFrom.leftValue;
        rightValue = copyFrom.rightValue;
    }

    @Override
    public int depth()
    {
        return depth;
    }

    @Override
    public int incomingTransition()
    {
        return incomingTransition;
    }

    @Override
    public T content()
    {
        return null;
    }

    @Override
    public int advance()
    {
        return descend();
    }

    private int exhausted()
    {
        incomingTransition = -1;
        depth = -1;
        return depth;
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        if (skipDepth <= depth || skipTransition > lnext)
            return exhausted();
        else
            return descend();
    }

    private int descend()
    {
        if (nextsSplit())
            return exhausted();

        incomingTransition = lnext;
        ++depth;

        lnext = lsrc.next();
        rnext = rsrc.next();
        return depth;
    }

    private boolean nextsSplit()
    {
        return lnext != rnext || lnext == ByteSource.END_OF_STREAM;
    }

    @Override
    public NonDeterministicTrieImpl.Cursor<T> duplicate()
    {
        return new AlternateRangeCursor<>(this);
    }

    @Override
    public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
    {
        if (nextsSplit())
            return exhausted();

        while (true)
        {
            incomingTransition = lnext;
            ++depth;
            lnext = lsrc.next();
            rnext = rsrc.next();
            if (nextsSplit())
                return depth;
            receiver.addPathByte(incomingTransition);
        }
    }

    @Override
    public NonDeterministicTrieImpl.Cursor<T> alternateBranch()
    {
        if (nextsSplit())
            return new TailCursor<>(lsrc, lnext, rsrc, rnext, depth, incomingTransition, leftValue, rightValue);
        else
            return null;
    }

    private static class TailCursor<T extends NonDeterministicTrie.Mergeable<T>> implements NonDeterministicTrieImpl.Cursor<T>
    {
        ByteSource src;
        int next;
        int depth;
        int incomingTransition;
        T value;

        ByteSource otherSrc;
        int otherNext;
        int otherDepth;
        T otherValue;

        public TailCursor(ByteSource lsrc, int lnext, ByteSource rsrc, int rnext, int startDepth, int startIncomingTransition, T leftValue, T rightValue)
        {
            this.src = lsrc;
            this.next = lnext;
            this.depth = startDepth;
            this.incomingTransition = startIncomingTransition;
            this.otherSrc = rsrc;
            this.otherNext = rnext;
            this.otherDepth = startDepth;
            this.value = leftValue;
            this.otherValue = rightValue;
        }

        TailCursor(TailCursor<T> copyFrom)
        {
            boolean copyFromSwitched = copyFrom.src == copyFrom.otherSrc;
            ByteSource.Duplicatable dupe = ByteSource.duplicatable(copyFrom.src);
            copyFrom.src = dupe;
            this.src = dupe.duplicate();
            if (!copyFromSwitched)
            {
                dupe = ByteSource.duplicatable(copyFrom.otherSrc);
                copyFrom.otherSrc = dupe;
                this.otherSrc = dupe.duplicate();
            }
            else
            {
                this.otherSrc = this.src;
                copyFrom.otherSrc = copyFrom.src;
            }

            this.next = copyFrom.next;
            this.depth = copyFrom.depth;
            this.incomingTransition = copyFrom.incomingTransition;
            this.otherNext = copyFrom.otherNext;
            this.otherDepth = copyFrom.otherDepth;
            this.otherValue = copyFrom.otherValue;
            this.value = copyFrom.value;
        }

        @Override
        public int depth()
        {
            return depth;
        }

        @Override
        public int incomingTransition()
        {
            return incomingTransition;
        }

        @Override
        public T content()
        {
            if (next == ByteSource.END_OF_STREAM)
            {
                assert otherNext != ByteSource.END_OF_STREAM || Objects.equals(value, otherValue)
                    : "left and right values must be equal when left and right bounds are the same";
                return value;
            }
            return null;
        }

        private int exhausted()
        {
            incomingTransition = -1;
            depth = -1;
            return depth;
        }

        @Override
        public int advance()
        {
            if (next == ByteSource.END_OF_STREAM)
                if (!switchSource())
                    return exhausted();

            return descend();
        }

        private boolean switchSource()
        {
            if (src == otherSrc)
                return false;

            src = otherSrc;
            next = otherNext;
            depth = otherDepth;
            value = otherValue;
            return true;
        }

        private int descend()
        {
            if (next == ByteSource.END_OF_STREAM)
                return exhausted();

            incomingTransition = next;
            next = src.next();
            return ++depth;
        }


        @Override
        public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
        {
            if (next == ByteSource.END_OF_STREAM)
                return advance();

            while (true)
            {
                incomingTransition = next;
                next = src.next();
                ++depth;
                if (next == ByteSource.END_OF_STREAM)
                    return depth;
                receiver.addPathByte(incomingTransition);
            }
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            if (depth + 1 == skipDepth && skipTransition <= next)
                return descend();
            if (!switchSource())
                return exhausted();
            if (depth + 1 == skipDepth && skipTransition <= next)
                return descend();
            else
                return exhausted();
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> alternateBranch()
        {
            return null;
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> duplicate()
        {
            return new TailCursor<>(this);
        }
    }
}
