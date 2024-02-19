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

public class IntersectionTrieSet extends TrieSet
{
    final TrieSet set1;
    final TrieSet set2;

    public IntersectionTrieSet(TrieSet set1, TrieSet set2)
    {
        this.set1 = set1;
        this.set2 = set2;
    }

    @Override
    protected Cursor cursor()
    {
        return new IntersectionCursor(set1.cursor(), set2.cursor());
    }

    static class IntersectionCursor extends CombinationTrieSetCursor
    {
        public IntersectionCursor(Cursor c1, Cursor c2)
        {
            super(c1, c2);
        }

        public IntersectionCursor(IntersectionCursor copyFrom)
        {
            super(copyFrom);
        }

        boolean lesserInSet(Cursor cursor)
        {
            return cursor.contained().lesserInSet();
        }

        protected Contained combineContained(Contained cl, Contained cr)
        {
            if (cl == Contained.OUTSIDE_PREFIX || cr == Contained.OUTSIDE_PREFIX)
                return Contained.OUTSIDE_PREFIX;
            else if (cl == Contained.INSIDE_PREFIX)
                return cr;
            else if (cr == Contained.INSIDE_PREFIX)
                return cl;
            else if (cl == cr)
                return cl;
            else // start and end combination
                return Contained.OUTSIDE_PREFIX;
        }

        @Override
        public Cursor duplicate()
        {
            return new IntersectionCursor(this);
        }
    }
}
