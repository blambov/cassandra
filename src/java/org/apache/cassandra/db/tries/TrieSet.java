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

/**
 * A simplified version of Trie used for sets (whose ultimate function is to intersect a Trie).
 *
 * Sets cannot be asynchronous and support a special value to denote a branch is fully included in the set,
 * which is used to speed up intersections.
 *
 * Like Trie nodes, set nodes are stateful and not thread-safe. If the consumer can use multiple threads when accessing
 * a node (e.g. with asynchronous trie walks), it must enforce a happens-before relationship between calls to the
 * methods of a node.
 */
public abstract class TrieSet extends Trie<TrieSet.InSet>
{
    enum InSet
    {
        // null content value specifies that the specific point is not contained in the set (e.g. points on the left range path)
        INCLUDED, // this point is contained in the set (e.g. points on the right range path)
        BRANCH; // the whole branch is contained in the set (e.g. interior nodes for a range)
    }

    private static final TrieSet FULL_SET = new TrieSet()
    {
        @Override
        protected Cursor<InSet> cursor()
        {
            return new Cursor<InSet>()
            {
                int level = 0;

                @Override
                public int advance()
                {
                    return level = -1;
                }

                @Override
                public int ascend()
                {
                    return level = -1;
                }

                @Override
                public int level()
                {
                    return level;
                }

                @Override
                public int incomingTransition()
                {
                    return -1;
                }

                @Override
                public InSet content()
                {
                    return InSet.BRANCH;
                }
            };
        }
    };

    /**
     * Range of keys between the given boundaries.
     * A null argument for any of the limits means that the set should be unbounded on that side.
     * The keys must be correctly ordered, including with respect to the `includeLeft` and `includeRight` constraints.
     * (i.e. range(x, false, x, false) is an invalid call but range(x, true, x, false) is inefficient
     * but fine for an empty set).
     */
    public static TrieSet range(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        return new RangeTrieSet(left, includeLeft, right, includeRight);
    }

    public static TrieSet full()
    {
        return FULL_SET;
    }
}
