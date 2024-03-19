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

import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface DeletionAwareTrie<T, D extends RangeTrie.RangeMarker<D>>
{

    /**
     * Call the given consumer on all content values in the trie in order.
     * Note: This will not present any deletions; use mergedTrie() to get a view of the trie with deletions included.
     */
    default void forEachValue(BaseTrie.ValueConsumer<T> consumer)
    {
        impl().process(consumer);
    }

    /**
     * Call the given consumer on all (path, content) pairs with non-null content in the trie in order.
     * Note: This will not present any deletions; use mergedTrie() to get a view of the trie with deletions included.
     */
    default void forEachEntry(BiConsumer<ByteComparable, T> consumer)
    {
        impl().process(new TrieEntriesWalker.WithConsumer<>(consumer));
        // Note: we can't do the ValueConsumer trick here, because the implementation requires state and cannot be
        // implemented with default methods alone.
    }

    /**
     * Constuct a textual representation of the trie.
     * Note: This will not present any deletions; use mergedTrie() to get a view of the trie with deletions included.
     */
    default String dump()
    {
        return dump(Object::toString);
    }

    /**
     * Constuct a textual representation of the trie using the given content-to-string mapper.
     * Note: This will not present any deletions; use mergedTrie() to get a view of the trie with deletions included.
     */
    default String dump(Function<T, String> contentToString)
    {
        return impl().process(new TrieDumper<>(contentToString));
    }

    /**
     * Returns a singleton trie mapping the given byte path to content.
     */
    static <T, D extends RangeTrie.RangeMarker<D>> DeletionAwareTrie<T, D> singleton(ByteComparable b, T v)
    {
        return (DeletionAwareTrieWithImpl<T, D>) () -> new SingletonCursor.DeletionAware<>(b, v);
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
        return (DeletionAwareTrieWithImpl<T, D>) () -> new IntersectionCursor.DeletionAware<>(impl().cursor(), TrieSetImpl.impl(set).cursor());
    }

    default Trie<T> contentOnlyTrie()
    {
        return (TrieWithImpl<T>) impl()::cursor;
    }

//    default RangeTrie<D> deletionOnlyTrie()
//    {
//        // Merge deletion branches
//        return new MergeAlternativeBranchesTrie.DeletionAware<>(impl(), false);
//    }
//
//    default Trie<T> mergedTrie()
//    {
//        return new MergeAlternativeBranchesTrie.DeletionAware<>(impl(), true);
//    }

    private DeletionAwareTrieImpl<T, D> impl()
    {
        return DeletionAwareTrieImpl.impl(this);
    }
}
