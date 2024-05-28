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
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface DeletionAwareTrie<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>> extends BaseTrie<T>
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
    @Override
    default void forEachValue(BaseTrie.ValueConsumer<T> consumer, Direction direction)
    {
        impl().process(consumer, direction);
    }

    /**
     * Call the given consumer on all (path, content) pairs with non-null content in the trie in order.
     * Note: This will not present any deletions; use mergedTrie() to get a view of the trie with deletions included.
     */
    @Override
    default void forEachEntry(BiConsumer<ByteComparable, T> consumer, Direction direction)
    {
        impl().process(new TrieEntriesWalker.WithConsumer<>(consumer), direction);
        // Note: we can't do the ValueConsumer trick here, because the implementation requires state and cannot be
        // implemented with default methods alone.
    }

    /**
     * Returns the ordered entry set of this trie's content in an iterator.
     */
    @Override
    default Iterator<Map.Entry<ByteComparable, T>> entryIterator(Direction direction)
    {
        return new TrieEntriesIterator.AsEntries<>(impl().cursor(direction));
    }

    /**
     * Returns the ordered entry set of this trie's content in an iterator, filtered by the given type.
     */
    @Override
    default <U extends T> Iterator<Map.Entry<ByteComparable, U>> filteredEntryIterator(Direction direction, Class<U> clazz)
    {
        return new TrieEntriesIterator.AsEntriesFilteredByType<>(impl().cursor(direction), clazz);
    }

    /**
     * Returns the ordered set of values of this trie in an iterator.
     */
    @Override
    default Iterator<T> valueIterator(Direction direction)
    {
        return new TrieValuesIterator<>(impl().cursor(direction));
    }

    /**
     * Constuct a textual representation of the trie using the given content-to-string mapper.
     * Note: This will not present any deletions; use mergedTrie() to get a view of the trie with deletions included.
     */
    @Override
    default String dump(Function<T, String> contentToString)
    {
        return impl().process(new TrieDumper<>(contentToString), Direction.FORWARD);
    }

    /**
     * Returns a singleton trie mapping the given byte path to content.
     */
    static <T extends Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    DeletionAwareTrie<T, D> singleton(ByteComparable b, T v)
    {
        return (DeletionAwareTrieWithImpl<T, D>) dir -> new SingletonCursor.DeletionAware<>(dir, b, v);
    }

    @Override
    default DeletionAwareTrie<T, D> subtrie(ByteComparable left, ByteComparable right)
    {
        return intersect(RangesTrieSet.create(left, right));
    }

    @Override
    default DeletionAwareTrie<T, D> intersect(TrieSet set)
    {
        return (DeletionAwareTrieWithImpl<T, D>) dir -> new IntersectionCursor.DeletionAware<>(dir,
                                                                                               impl().cursor(dir),
                                                                                               TrieSetImpl.impl(set).cursor(dir));
    }

    default DeletionAwareTrie<T, D> mergeWith(DeletionAwareTrie<T, D> other,
                                              Trie.MergeResolver<T> mergeResolver,
                                              Trie.MergeResolver<D> deletionMerger,
                                              BiFunction<D, T, T> deleter)
    {
        return (DeletionAwareTrieWithImpl<T, D>)
               dir -> new MergeCursor.DeletionAware<>(dir,
                                                      mergeResolver,
                                                      deletionMerger,
                                                      deleter,
                                                      impl().cursor(dir),
                                                      DeletionAwareTrieImpl.impl(other).cursor(dir));
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
                       dir -> new CollectionMergeCursor.DeletionAware<>(dir,
                                                                        mergeResolver,
                                                                        deletionResolver,
                                                                        deleter,
                                                                        sources);
        }
    }

    default Trie<T> contentOnlyTrie()
    {
        return (TrieWithImpl<T>) impl()::cursor;
    }

    default RangeTrie<D> deletionOnlyTrie()
    {
        // We must walk the main trie to find deletion branch roots.
        return (RangeTrieWithImpl<D>) dir -> new DeletionAwareTrieImpl.DeletionsTrieCursor<>(dir, impl().cursor(dir));
    }

    default <Z> Trie<Z> mergedTrie(BiFunction<T, D, Z> resolver)
    {
        return (TrieWithImpl<Z>) dir -> new DeletionAwareTrieImpl.LiveAndDeletionsMergeCursor<>(dir, resolver, impl().cursor(dir));
    }

    @SuppressWarnings("unchecked")
    static <T extends Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    DeletionAwareTrie<T, D> empty()
    {
        return (DeletionAwareTrie<T, D>) DeletionAwareTrieImpl.EMPTY;
    }

    /**
     * Returns a Trie that is a view of this one, where the given prefix is prepended before the root.
     */
    @Override
    default DeletionAwareTrie<T, D> prefix(ByteComparable prefix)
    {
        return (DeletionAwareTrieWithImpl<T, D>) dir -> new PrefixedCursor.DeletionAware<>(dir, prefix, impl().cursor(dir));
    }

    default Iterable<Map.Entry<ByteComparable, DeletionAwareTrie<T, D>>> tailTries(Predicate<T> predicate, Direction direction)
    {
        return () -> new TrieTailsIterator.AsEntries<>(impl().cursor(direction), predicate, this::tailTrie);
    }

    @Override
    default DeletionAwareTrie<T, D> tailTrie(ByteComparable prefix)
    {
        return (DeletionAwareTrieWithImpl<T, D>) dir -> {
            DeletionAwareTrieImpl.Cursor<T, D> c = impl().cursor(dir);
            if (c.descendAlong(prefix.asComparableBytes(CursorWalkable.BYTE_COMPARABLE_VERSION)))
                return new TailCursor.DeletionAware<>(c);
            else
                return new DeletionAwareTrieImpl.EmptyCursor<T, D>();
        };
    }

    private DeletionAwareTrieImpl<T, D> impl()
    {
        return DeletionAwareTrieImpl.impl(this);
    }
}
