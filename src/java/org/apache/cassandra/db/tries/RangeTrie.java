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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class RangeTrie extends Trie<Boolean>
{
    final ByteComparable left;
    final ByteComparable right;

    private RangeTrie(ByteComparable left, ByteComparable right)
    {
        this.left = left;
        this.right = right;
    }

    public static Trie<Boolean> create(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        if (!includeLeft && left != null)
            left = add0(left);
        if (includeRight && right != null)
            right = add0(right);
        return create(left, right);
    }

    public static Trie<Boolean> create(ByteComparable left, ByteComparable right)
    {
        return new RangeTrie(left, right);
    }

    private static ByteComparable add0(ByteComparable v)
    {
        return version -> add0(v.asComparableBytes(version));
    }

    private static ByteSource add0(ByteSource src)
    {
        return new ByteSource()
        {
            boolean done = false;
            @Override
            public int next()
            {
                if (done)
                    return END_OF_STREAM;
                int v = src.next();
                if (v != END_OF_STREAM)
                    return v;
                done = true;
                return 0;
            }
        };
    }

    @Override
    protected Cursor<Boolean> cursor()
    {
        ByteSource lsrc, rsrc;
        int lnext, rnext;

        if (left != null)
        {
            lsrc = left.asComparableBytes(BYTE_COMPARABLE_VERSION);
            lnext = lsrc.next();
        }
        else
        {
            lsrc = null;
            lnext = ByteSource.END_OF_STREAM;
        }

        if (right != null)
        {
            rsrc = right.asComparableBytes(BYTE_COMPARABLE_VERSION);
            rnext = rsrc.next();
        }
        else
        {
            rsrc = null;
            rnext = Integer.MAX_VALUE;
        }


        if (rnext == ByteSource.END_OF_STREAM)
        {
            // Right is empty. Range is only valid (and empty) if left is null or empty.
            assert lnext == ByteSource.END_OF_STREAM
                : "Invalid range " + left.byteComparableAsString(BYTE_COMPARABLE_VERSION) + " to " +
                  right.byteComparableAsString(BYTE_COMPARABLE_VERSION);
            // Because we can't start with -1 depth, the range cursor would always return the root, with true content.
            // To fix this, use an empty cursor to avoid complicating the RangeCursor code.
            return Trie.<Boolean>empty().cursor();
        }

        return new RangeCursor(lsrc, lnext, rsrc, rnext);
    }

    private static class RangeCursor implements Cursor<Boolean>
    {
        final ByteSource lsrc;
        final ByteSource rsrc;
        int lnext;
        int rnext;
        int rdepth;
        boolean onLeftPrefix;
        int depth;
        int incomingTransition;

        public RangeCursor(ByteSource lsrc, int lnext, ByteSource rsrc, int rnext)
        {
            this.lsrc = lsrc;
            this.lnext = lnext;
            this.onLeftPrefix = lnext != ByteSource.END_OF_STREAM;
            this.rsrc = rsrc;
            this.rnext = rnext;
            this.rdepth = 1;
            this.depth = 0;
            this.incomingTransition = -1;
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
        public Boolean content()
        {
            return onLeftPrefix ? null : Boolean.TRUE;
        }

        @Override
        public int advance()
        {
            // This should rarely be used.
            if (onLeftPrefix)
                return descendAlongLeft(lnext);
            else
            {
                // when advance is called inside the span, we just dive deeper -- making sure to quit if we happen
                // to hit the right bound
                return descendAndCheckRightBound(0);
            }
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            if (onLeftPrefix && (depth + 1 == skipDepth && skipTransition <= lnext))
                return descendAlongLeft(lnext);
            else
            {
                onLeftPrefix = false;
                return advanceAndCheckRightBound(skipDepth, skipTransition);
            }
        }

        private int descendAlongLeft(int transition)
        {
            lnext = lsrc.next();
            onLeftPrefix = lnext != ByteSource.END_OF_STREAM;
            return descendAndCheckRightBound(transition);
        }

        private int descendAndCheckRightBound(int transition)
        {
            // called when we descend along left -- we are guaranteed to be before right
            assert rdepth <= depth || rdepth == depth + 1 && transition <= rnext;
            return descendAndCheckRightBound(depth + 1, transition);
        }

        private int descendAndCheckRightBound(int newDepth, int transition)
        {
            incomingTransition = transition;
            if (newDepth == rdepth && rnext == transition)
            {
                // at right bound
                rnext = rsrc.next();
                ++rdepth;
                if (rnext == ByteSource.END_OF_STREAM)
                    return depth = -1;
            }
            return depth = newDepth;
        }

        private int advanceAndCheckRightBound(int newDepth, int transition)
        {
            if (newDepth < rdepth || newDepth == rdepth && transition > rnext)
                return depth = -1;  // beyond right bound
            return descendAndCheckRightBound(newDepth, transition);
        }
    }
}
