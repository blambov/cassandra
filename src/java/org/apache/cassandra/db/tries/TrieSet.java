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

import javax.annotation.Nonnull;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public abstract class TrieSet
{
    public enum Contained
    {
        /**
         * Prefix of the start position, outside the covered set.
         */
        OUTSIDE_PREFIX,
        /**
         * Exact start position.
         */
        START,
        /**
         * Prefix of the end position, inside the covered set (i.e. not a prefix of a START). This is
         * reported after advancing past START, but also when skipping to a position inside the set.
         * Indicates that all positions between that start position or skipTo location and the current are completely
         * inside the set.
         */
        INSIDE_PREFIX,
        /**
         * Exact end position.
         */
        END;

        /**
         * @return True if the exact position is in the set.
         */
        boolean isInSet()
        {
            return this == START || this == INSIDE_PREFIX;
        }

        /**
         * @return True if the positions to the left of the current are in the set.
         */
        boolean lesserInSet()
        {
            return this == INSIDE_PREFIX || this == END;
        }

        boolean isPrefix()
        {
            return this == OUTSIDE_PREFIX || this == INSIDE_PREFIX;
        }
    }
    // TODO: For reverse iteration this should still work (assuming reverse is lexicographic on negated transitions)
    // Note: Cursor will report different Contained values for forward and reverse iteration.
    // Note: At the start of the iteration (when the cursor is positioned at the root), START and INSIDE_PREFIX are
    // equivalent (since there are no positions to the left which can be in the set).
    // Note: TrieSet will report a contained() value when exhausted. This will be INSIDE_PREFIX (or, equivalently, END)
    // if the set has no upper limit.

    protected interface Cursor
    {
        int depth();
        int incomingTransition();

        /**
         * @return Relationship of the current position to the set. Cannot be null.
         */
        @Nonnull
        Contained contained();
        int advance();
        int skipTo(int skipDepth, int skipTransition);
        Cursor duplicate();
    }

    protected abstract Cursor cursor();

    // Version of the byte comparable conversion to use for all operations
    protected static final ByteComparable.Version BYTE_COMPARABLE_VERSION = Trie.BYTE_COMPARABLE_VERSION;

    /**
     * Process the trie using the given Walker.
     */
    public <R> R process(Trie.Walker<Contained, R> walker)
    {
        return process(walker, cursor());
    }

    static <R> R process(Trie.Walker<Contained, R> walker, Cursor cursor)
    {
        assert cursor.depth() == 0 : "The provided cursor has already been advanced.";
        int prevDepth = cursor.depth();
        while (true)
        {
            Contained contained = cursor.contained();
            if (!contained.isPrefix())
                walker.content(contained);

            int currDepth = cursor.advance();
            if (currDepth <= 0)
                break;
            if (currDepth <= prevDepth)
                walker.resetPathLength(currDepth - 1);
            walker.addPathByte(cursor.incomingTransition());
            prevDepth = currDepth;
        }
        Contained contained = cursor.contained();
        if (!contained.isPrefix())
        {
            walker.resetPathLength(0);
            walker.content(contained);
        }
        return walker.complete();
    }

    /**
     * Constuct a textual representation of the trie using the given content-to-string mapper.
     */
    public String dump()
    {
        return process(new TrieDumper<>(Object::toString));
    }

    public static TrieSet singleton(ByteComparable b)
    {
        return RangesTrieSet.create(b, true, b, true);
    }

    public static TrieSet range(ByteComparable left, ByteComparable right)
    {
        return RangesTrieSet.create(left, right);
    }

    public static TrieSet range(ByteComparable left, boolean leftInclusive, ByteComparable right, boolean rightInclusive)
    {
        return RangesTrieSet.create(left, leftInclusive, right, rightInclusive);
    }

    public static TrieSet ranges(ByteComparable... boundaries)
    {
        return new RangesTrieSet(boundaries);
    }

    public TrieSet negation()
    {
        return new NegatedTrieSet(this);
    }

    public TrieSet union(TrieSet other)
    {
        return new UnionTrieSet(this, other);
    }

    public TrieSet intersection(TrieSet other)
    {
        return new IntersectionTrieSet(this, other);
    }
}
