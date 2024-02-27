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

public interface DeletionAwareTrie<T, D extends T>
{
    public enum BoundSide
    {
        BEFORE, AFTER, AT
    }

    public interface DeletionHandler<T, D extends T>
    {
        boolean has(D deletionMarker, BoundSide side);
        T delete(T content, D deletionMarker, BoundSide contentRelativeToDeletion);
        /*
         * Convert one side of the given marker to a marker of the given side.
         * The other sides of the source deletion marker are ignored.
         * Used e.g. to convert the marker after a covered region into a start and end pair in intersections.
         */
        D asBound(D deletionMarker, BoundSide deletionSide, BoundSide targetSide);

        /**
         * Verify that the given deletion marker closes the active deletion. Used in assertions.
         * @param deletionMarker Newly-encountered marker.
         * @param activeMarker The marker that was active before it. May be null.
         * @return false if something is wrong.
         */
        boolean closes(D deletionMarker, D activeMarker);
    }

    default DeletionAwareTrie<T, D> subtrie(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        if (left == null && right == null)
            return this;
        return intersect(RangesTrieSet.create(left, includeLeft, right, includeRight));
    }

    default DeletionAwareTrie<T, D> subtrie(ByteComparable left, ByteComparable right)
    {
        return intersect(RangesTrieSet.create(left, right));
    }
    default DeletionAwareTrie<T, D> intersect(TrieSet set)
    {
        return new DeletionAwareIntersectionTrie<>(impl(), TrieSetImpl.impl(set));
    }

    default Trie<T> withDeletions()
    {
        // TODO: Perform resolution of multiple deletions
        return new MergeAlternativeBranchesTrie<>(impl(), Trie.throwingResolver(), false);
    }

    private DeletionAwareTrieImpl<T, D> impl()
    {
        return DeletionAwareTrieImpl.impl(this);
    }
}
