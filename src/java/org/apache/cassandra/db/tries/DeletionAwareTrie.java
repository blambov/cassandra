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

import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface DeletionAwareTrie<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
{
    interface Deletable
    {
        // Marker interface, no specific methods
    }

    interface DeletionMarker<T extends Deletable, D extends DeletionMarker<T, D>> extends RangeTrie.RangeMarker<D>
    {
        T delete(T content);
    }


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
    static <T extends Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    DeletionAwareTrie<T, D> singleton(ByteComparable b, T v)
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

    default DeletionAwareTrie<T, D> mergeWith(DeletionAwareTrie<T, D> other,
                                              Trie.MergeResolver<T> mergeResolver,
                                              Trie.MergeResolver<D> deletionMerger,
                                              BiFunction<D, T, T> deleter)
    {
        return (DeletionAwareTrieWithImpl<T, D>) () -> new MergeCursor.DeletionAware<>(mergeResolver,
                                                                                       deletionMerger,
                                                                                       deleter,
                                                                                       impl().cursor(),
                                                                                       DeletionAwareTrieImpl.impl(other).cursor());
    }


    /**
     * Constructs a view of the merge of multiple tries. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     * <p>
     * If there is content for a given key in more than one sources, the resolver will be called to obtain the
     * combination. (The resolver will not be called if there's content from only one source.)
     */
    static <T extends Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    DeletionAwareTrie<T, D> merge(Collection<? extends DeletionAwareTrie<T, D>> sources,
                                  Trie.CollectionMergeResolver<T> mergeResolver,
                                  Trie.CollectionMergeResolver<D> deletionResolver,
                                  BiFunction<D, T, T> deleter)
    {
        switch (sources.size())
        {
            case 0:
                return empty();
            case 1:
                return sources.iterator().next();
            case 2:
            {
                Iterator<? extends DeletionAwareTrie<T, D>> it = sources.iterator();
                DeletionAwareTrie<T, D> t1 = it.next();
                DeletionAwareTrie<T, D> t2 = it.next();
                return t1.mergeWith(t2, mergeResolver, deletionResolver, deleter);
            }
            default:
                return (DeletionAwareTrieWithImpl<T, D>)
                       () -> new CollectionMergeCursor.DeletionAware<>(mergeResolver, deletionResolver, deleter, sources);
        }
    }

    default Trie<T> contentOnlyTrie()
    {
        return (TrieWithImpl<T>) impl()::cursor;
    }

    default RangeTrie<D> deletionOnlyTrie()
    {
        // We must walk the main trie to find deletion branch roots.
        return (RangeTrieWithImpl<D>) () -> new DeletionAwareTrieImpl.DeletionsTrieCursor<>(impl().cursor());
    }

    default <Z> Trie<Z> mergedTrie(BiFunction<T, D, Z> resolver)
    {
        return (TrieWithImpl<Z>) () -> new DeletionAwareTrieImpl.LiveAndDeletionsMergeCursor<>(resolver, impl().cursor());
    }

    @SuppressWarnings("unchecked")
    static <T extends Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    DeletionAwareTrie<T, D> empty()
    {
        return (DeletionAwareTrie<T, D>) DeletionAwareTrieImpl.EMPTY;
    }

    private DeletionAwareTrieImpl<T, D> impl()
    {
        return DeletionAwareTrieImpl.impl(this);
    }
}
