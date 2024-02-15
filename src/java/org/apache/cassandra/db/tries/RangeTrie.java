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

public class RangeTrie extends Trie<Trie.Contained>
{
    final ByteComparable left;
    final ByteComparable right;

    private RangeTrie(ByteComparable left, ByteComparable right)
    {
        this.left = left;
        this.right = right;
    }

    public static Trie<Contained> create(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        return RangesTrie.create(left, includeLeft, right, includeRight);
//        if (!includeLeft && left != null)
//            left = add0(left);
//        if (includeRight && right != null)
//            right = add0(right);
//        return create(left, right);
    }

    public static Trie<Contained> create(ByteComparable left, ByteComparable right)
    {
        return new RangesTrie(left, right);
//        return new RangeTrie(left, right);
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
    protected Cursor<Contained> cursor()
    {
        return new RangeCursor(left != null ? left.asComparableBytes(BYTE_COMPARABLE_VERSION) : null,
                               right != null ? right.asComparableBytes(BYTE_COMPARABLE_VERSION) : null);
    }

    enum State
    {
        FOLLOWING_BOTH(null),
        FOLLOWING_LEFT(null),
        AT_LEFT_BOUNDARY(Trie.Contained.START),
        FOLLOWING_RIGHT(Trie.Contained.INSIDE_PREFIX),
        AT_RIGHT_BOUNDARY(Trie.Contained.END),
        EXHAUSTED(null);

        final Contained toReport;


        State(Contained toReport)
        {
            this.toReport = toReport;
        }
    }


    private static class RangeCursor implements Cursor<Contained>
    {
        ByteSource lsrc;
        ByteSource rsrc;
        int lnext;
        int rnext;
        int rdepth;
        int depth;
        int incomingTransition;
        State state;

        public RangeCursor(ByteSource lsrc, ByteSource rsrc)
        {
            this.lsrc = lsrc;
            this.rsrc = rsrc;
            this.rdepth = 1;
            this.depth = 0;
            this.incomingTransition = -1;
            if (lsrc == null)
                lnext = ByteSource.END_OF_STREAM;
            else
                lnext = lsrc.next();

            if (rsrc == null)
            {
                rdepth = -1;
                rnext = 0;
            }
            else
            {
                rdepth = 1;
                rnext = rsrc.next();
            }

            if (lnext == ByteSource.END_OF_STREAM)
            {
                if (rnext == ByteSource.END_OF_STREAM)
                    state = State.AT_RIGHT_BOUNDARY;
                else
                    state = State.FOLLOWING_RIGHT;
            }
            else
            {
                assert rnext != ByteSource.END_OF_STREAM;
                state = State.FOLLOWING_BOTH;
            }
        }

        RangeCursor(RangeCursor copyFrom)
        {
            if (copyFrom.state == State.FOLLOWING_LEFT || copyFrom.state == State.FOLLOWING_BOTH)
            {
                ByteSource.Duplicatable dupe = ByteSource.duplicatable(copyFrom.lsrc);
                copyFrom.lsrc = dupe;
                this.lsrc = dupe.duplicate();
            }
            else
                this.lsrc = null;
            if (copyFrom.state != State.EXHAUSTED && copyFrom.state != State.AT_RIGHT_BOUNDARY)
            {
                ByteSource.Duplicatable dupe = ByteSource.duplicatable(copyFrom.rsrc);
                copyFrom.rsrc = dupe;
                this.rsrc = dupe.duplicate();
            }
            else
                this.rsrc = null;

            this.lnext = copyFrom.lnext;
            this.state = copyFrom.state;
            this.rnext = copyFrom.rnext;
            this.rdepth = copyFrom.rdepth;
            this.depth = copyFrom.depth;
            this.incomingTransition = copyFrom.incomingTransition;
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
        public Contained content()
        {
            return state.toReport;
        }

        @Override
        public int advance()
        {
            switch (state)
            {
                case FOLLOWING_BOTH:
                    incomingTransition = lnext;
                    ++depth;
                    if (lnext == rnext)
                    {
                        lnext = lsrc.next();
                        rnext = rsrc.next();
                        if (lnext == ByteSource.END_OF_STREAM)
                        {
                            if (rnext == ByteSource.END_OF_STREAM)
                                state = State.AT_RIGHT_BOUNDARY;
                            else
                                state = State.FOLLOWING_RIGHT;
                        }
                    }
                    else
                    {
                        assert lnext < rnext : "Invalid range (left before right?)";
                        rdepth = depth;
                        state = State.FOLLOWING_LEFT;
                    }
                    return depth;
                case FOLLOWING_LEFT:
                    incomingTransition = lnext;
                    ++depth;
                    lnext = lsrc.next();
                    if (lnext == ByteSource.END_OF_STREAM)
                        state = State.AT_LEFT_BOUNDARY;
                    return depth;
                case AT_LEFT_BOUNDARY:
                    return switchToRight(rdepth);
                case FOLLOWING_RIGHT:
                    incomingTransition = rnext;
                    ++depth;
                    rnext = rsrc.next();
                    if (rnext == ByteSource.END_OF_STREAM)
                        state = State.AT_RIGHT_BOUNDARY;
                    return depth;
                case AT_RIGHT_BOUNDARY:
                    assert rsrc != null : "Advance called on exhausted cursor.";
                    incomingTransition = 0;
                    depth = -1;
                    state = State.EXHAUSTED;
                    return depth;
                case EXHAUSTED:
                    throw new AssertionError("Advance called on exhausted cursor.");
                default:
                    throw new AssertionError("Invalid cursor state.");
            }
        }

        private int exhausted()
        {
            state = State.EXHAUSTED;
            depth = -1;
            incomingTransition = 0;
            return depth;
        }

        private int switchToRight(int rdepth)
        {
            incomingTransition = rnext;
            depth = rdepth;
            rnext = rsrc.next();
            if (rnext == ByteSource.END_OF_STREAM)
                state = State.AT_RIGHT_BOUNDARY;
            else
                state = State.FOLLOWING_RIGHT;
            return depth;
        }


        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            assert skipDepth <= depth + 1;
            switch (state)
            {
                case FOLLOWING_BOTH:
                    // this path is equivalent to the one in the next case for rdepth = depth + 1 but save a couple of checks
                    if (skipDepth < depth + 1 || skipTransition > rnext)
                        return exhausted();
                    else if (skipTransition > lnext) // skipDepth must be depth + 1 here, no need to check
                        return switchToRight(depth + 1);
                    else
                        return advance();
                case FOLLOWING_LEFT:
                    if (skipDepth == depth + 1 && skipTransition <= lnext)
                        return advance();
                    else if (skipDepth > rdepth || skipDepth == rdepth && skipTransition <= rnext)
                        return switchToRight(rdepth);
                    else
                        return exhausted();
                case AT_LEFT_BOUNDARY:
                {
                    if (skipDepth > rdepth || skipDepth == rdepth && skipTransition <= rnext)
                        return switchToRight(rdepth);
                    else
                        return exhausted();
                }
                case FOLLOWING_RIGHT:
                {
                    if (skipDepth == depth + 1 && skipTransition <= rnext)
                        return advance();
                    else
                        return exhausted();
                }
                default:
                    return advance();
            }
        }

        @Override
        public Cursor<Contained> duplicate()
        {
            return new RangeCursor(this);
        }
    }
}
