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

/**
 * TrieSet union.
 * <p>
 * The implementation is an application of De Morgan's law: an intersection with changed interpretation of the
 * contained() values to apply negation to both inputs and the output.
 */
public class UnionTrieSet implements TrieSetImpl
{
    final TrieSetImpl set1;
    final TrieSetImpl set2;

    public UnionTrieSet(TrieSetImpl set1, TrieSetImpl set2)
    {
        this.set1 = set1;
        this.set2 = set2;
    }

    @Override
    public Cursor cursor()
    {
        return new UnionCursor(set1.cursor(), set2.cursor());
    }

    static class UnionCursor extends IntersectionTrieSet.IntersectionCursor
    {
        public UnionCursor(Cursor c1, Cursor c2)
        {
            super(c1, c2);
        }

        public UnionCursor(UnionCursor copyFrom)
        {
            super(copyFrom);
        }

        @Override
        boolean lesserInSet(Cursor cursor)
        {
            return !cursor.contained().lesserInSet();
        }

        @Override
        Contained combineContained(Contained cl, Contained cr)
        {
            if (cl == Contained.INSIDE_PREFIX || cr == Contained.INSIDE_PREFIX)
                return Contained.INSIDE_PREFIX;
            else if (cl == Contained.OUTSIDE_PREFIX)
                return cr;
            else if (cr == Contained.OUTSIDE_PREFIX)
                return cl;
            else if (cl == cr)
                return cl;
            else // start and end combination
                return Contained.INSIDE_PREFIX;
        }

        @Override
        public Cursor duplicate()
        {
            return new UnionCursor(this);
        }
    }
}
