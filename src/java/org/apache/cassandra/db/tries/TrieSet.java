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

public interface TrieSet
{
    static TrieSet singleton(ByteComparable b)
    {
        return RangesTrieSet.create(b, true, b, true);
    }

    static TrieSet range(ByteComparable left, ByteComparable right)
    {
        return RangesTrieSet.create(left, right);
    }

    static TrieSet range(ByteComparable left, boolean leftInclusive, ByteComparable right, boolean rightInclusive)
    {
        return RangesTrieSet.create(left, leftInclusive, right, rightInclusive);
    }

    static TrieSet ranges(ByteComparable... boundaries)
    {
        return RangesTrieSet.create(boundaries);
    }

    default TrieSet negation()
    {
        return (TrieSetWithImpl) dir -> new TrieSetNegatedCursor(impl().cursor(dir));
    }

    default TrieSet union(TrieSet other)
    {
        return (TrieSetWithImpl) dir -> new TrieSetIntersectionCursor.UnionCursor(dir, impl().cursor(dir), other.impl().cursor(dir));
    }

    default TrieSet intersection(TrieSet other)
    {
        return (TrieSetWithImpl) dir -> new TrieSetIntersectionCursor(dir, impl().cursor(dir), other.impl().cursor(dir));
    }

    private TrieSetImpl impl()
    {
        return TrieSetImpl.impl(this);
    }
}
