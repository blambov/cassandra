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

public class AlternateRangeTrie<T> implements TrieWithImpl<T>
{
    final ByteComparable left;
    final ByteComparable right;
    final T leftValue;
    final T rightValue;

    public AlternateRangeTrie(ByteComparable left, T leftValue, ByteComparable right, T rightValue)
    {
        this.left = left;
        this.right = right;
        this.leftValue = leftValue;
        this.rightValue = rightValue;
    }

    @Override
    public Cursor<T> cursor()
    {
        return new HeadCursor();
    }

    private class HeadCursor implements Cursor<T>
    {
        ByteSource lsrc;
        ByteSource rsrc;

        int lnext;
        int rnext;
        int incomingTransition;
        int depth;

        HeadCursor()
        {
            lsrc = left.asComparableBytes(BYTE_COMPARABLE_VERSION);
            rsrc = right.asComparableBytes(BYTE_COMPARABLE_VERSION);
            lnext = lsrc.next();
            rnext = rsrc.next();
        }

        HeadCursor(HeadCursor copyFrom)
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

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            if (skipDepth <= depth || skipTransition > lnext)
                return depth = -1;
            else
                return descend();
        }

        private int descend()
        {
            if (nextsSplit())
                return depth = -1;

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
        public Cursor<T> duplicate()
        {
            return new HeadCursor(this);
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            if (nextsSplit())
                return depth = -1;

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
        public Cursor<T> alternateBranch()
        {
            if (nextsSplit())
                return new TailCursor(lsrc, lnext, rsrc, rnext, depth, incomingTransition);
            else
                return null;
        }
    }

    private class TailCursor implements Cursor<T>
    {
        ByteSource src;
        int next;
        int depth;
        int incomingTransition;
        T value;

        ByteSource otherSrc;
        int otherNext;
        int otherDepth;

        public TailCursor(ByteSource lsrc, int lnext, ByteSource rsrc, int rnext, int startDepth, int startIncomingTransition)
        {
            this.src = lsrc;
            this.next = lnext;
            this.depth = startDepth;
            this.incomingTransition = startIncomingTransition;
            this.otherSrc = rsrc;
            this.otherNext = rnext;
            this.otherDepth = startDepth;
            this.value = leftValue;
        }

        TailCursor(TailCursor copyFrom)
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
                assert otherNext != ByteSource.END_OF_STREAM || Objects.equals(value, rightValue)
                    : "left and right values must be equal when left and right bounds are the same";
                return value;
            }
            return null;
        }

        @Override
        public int advance()
        {
            if (next == ByteSource.END_OF_STREAM)
                if (!switchSource())
                    return depth = -1;

            return descend();
        }

        private boolean switchSource()
        {
            if (src == otherSrc)
                return false;

            src = otherSrc;
            next = otherNext;
            depth = otherDepth;
            value = rightValue;
            return true;
        }

        private int descend()
        {
            if (next == ByteSource.END_OF_STREAM)
                return depth = -1;

            incomingTransition = next;
            next = src.next();
            return ++depth;
        }


        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
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
                return depth = -1;
            if (depth + 1 == skipDepth && skipTransition <= next)
                return descend();
            else
                return depth = -1;
        }

        @Override
        public Cursor<T> duplicate()
        {
            return new TailCursor(this);
        }
    }
}
