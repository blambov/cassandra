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
import java.util.function.Function;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface NonDeterministicTrie<T> extends BaseTrie<T>
{

    /**
     * Call the given consumer on all content values in the trie in order.
     */
    default void forEachValue(ValueConsumer<T> consumer)
    {
        impl().process(consumer);
    }

    /**
     * Call the given consumer on all (path, content) pairs with non-null content in the trie in order.
     */
    default void forEachEntry(BiConsumer<ByteComparable, T> consumer)
    {
        impl().process(new TrieEntriesWalker.WithConsumer<>(consumer));
        // Note: we can't do the ValueConsumer trick here, because the implementation requires state and cannot be
        // implemented with default methods alone.
    }

    /**
     * Constuct a textual representation of the trie.
     */
    default String dump()
    {
        return dump(Object::toString);
    }

    /**
     * Constuct a textual representation of the trie using the given content-to-string mapper.
     */
    default String dump(Function<T, String> contentToString)
    {
        return impl().process(new TrieDumper<>(contentToString));
    }

    /**
     * Returns a singleton trie mapping the given byte path to content.
     */
    static <T> NonDeterministicTrie<T> singleton(ByteComparable b, T v)
    {
        return (NonDeterministicTrieWithImpl<T>) () -> new SingletonCursor<>(b, v);
    }

    /**
     * Returns a view of the subtrie containing everything in this trie whose keys fall between the given boundaries.
     * The view is live, i.e. any write to the source will be reflected in the subtrie.
     * <p>
     * This method will not check its arguments for correctness. The resulting trie may be empty or throw an exception
     * if the right bound is smaller than the left.
     * <p>
     * @param left the left bound for the returned subtrie. If {@code null}, the resulting subtrie is not left-bounded.
     * @param includeLeft whether {@code left} is an inclusive bound of not.
     * @param right the right bound for the returned subtrie. If {@code null}, the resulting subtrie is not right-bounded.
     * @param includeRight whether {@code right} is an inclusive bound of not.
     * @return a view of the subtrie containing all the keys of this trie falling between {@code left} (inclusively if
     * {@code includeLeft}) and {@code right} (inclusively if {@code includeRight}).
     */
    default NonDeterministicTrie<T> subtrie(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        if (left == null && right == null)
            return this;
        return intersect(RangesTrieSet.create(left, includeLeft, right, includeRight));
    }

    /**
     * Returns a view of the subtrie containing everything in this trie whose keys fall between the given boundaries,
     * left-inclusive and right-exclusive.
     * The view is live, i.e. any write to the source will be reflected in the subtrie.
     * <p>
     * This method will not check its arguments for correctness. The resulting trie may be empty or throw an exception
     * if the right bound is smaller than the left.
     * <p>
     * Equivalent to calling subtrie(left, true, right, false).
     * <p>
     * @param left the left bound for the returned subtrie. If {@code null}, the resulting subtrie is not left-bounded.
     * @param right the right bound for the returned subtrie. If {@code null}, the resulting subtrie is not right-bounded.
     * @return a view of the subtrie containing all the keys of this trie falling between {@code left} (inclusively if
     * {@code includeLeft}) and {@code right} (inclusively if {@code includeRight}).
     */
    default NonDeterministicTrie<T> subtrie(ByteComparable left, ByteComparable right)
    {
        return intersect(RangesTrieSet.create(left, right));
    }

    /**
     * Returns a view of this trie that is an intersection of its content with the given set.
     * <p>
     * The view is live, i.e. any write to the source will be reflected in the intersection.
     */
    default NonDeterministicTrie<T> intersect(TrieSet set)
    {
        return (NonDeterministicTrieWithImpl<T>) () -> new IntersectionCursor.NonDeterministic<>(impl().cursor(), TrieSetImpl.impl(set).cursor());
    }

    /**
     * Returns the ordered entry set of this trie's content as an iterable.
     */
    default Iterable<Map.Entry<ByteComparable, T>> entrySet()
    {
        return this::entryIterator;
    }

    /**
     * Returns the ordered entry set of this trie's content in an iterator.
     */
    default Iterator<Map.Entry<ByteComparable, T>> entryIterator()
    {
        return new TrieEntriesIterator.AsEntries<>(impl().cursor());
    }

    /**
     * Returns the ordered set of values of this trie as an iterable.
     */
    default Iterable<T> values()
    {
        return this::valueIterator;
    }

    /**
     * Returns the ordered set of values of this trie in an iterator.
     */
    default Iterator<T> valueIterator()
    {
        return new TrieValuesIterator<>(impl().cursor());
    }

    /**
     * Returns the values in any order. For some tries this is much faster than the ordered iterable.
     */
    default Iterable<T> valuesUnordered()
    {
        return values();
    }

    /**
     * Resolver of content of merged nodes, used for two-source merges (i.e. mergeWith).
     */
    interface MergeResolver<T>
    {
        // Note: No guarantees about argument order.
        // E.g. during t1.mergeWith(t2, resolver), resolver may be called with t1 or t2's items as first argument.
        T resolve(T b1, T b2);
    }

    /**
     * Constructs a view of the merge of this trie with the given one. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     * <p>
     * If there is content for a given key in both sources, the resolver will be called to obtain the combination.
     * (The resolver will not be called if there's content from only one source.)
     */
    default NonDeterministicTrie<T> mergeWith(NonDeterministicTrie<T> other, Trie.MergeResolver<T> resolver)
    {
        return (NonDeterministicTrieWithImpl<T>) () -> new MergeCursor.NonDeterministic<>(resolver, impl(), other.impl());
    }

    /**
     * Constructs a view of the merge of multiple tries. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     * <p>
     * If there is content for a given key in more than one sources, the resolver will be called to obtain the
     * combination. (The resolver will not be called if there's content from only one source.)
     */
    static <T> NonDeterministicTrie<T> merge(Collection<? extends NonDeterministicTrie<T>> sources, Trie.CollectionMergeResolver<T> resolver)
    {
        switch (sources.size())
        {
            case 0:
                return empty();
            case 1:
                return sources.iterator().next();
            case 2:
            {
                Iterator<? extends NonDeterministicTrie<T>> it = sources.iterator();
                NonDeterministicTrie<T> t1 = it.next();
                NonDeterministicTrie<T> t2 = it.next();
                return t1.mergeWith(t2, resolver);
            }
            default:
                return (NonDeterministicTrieWithImpl<T>) () -> new CollectionMergeCursor.NonDeterministic<>(resolver, sources);
        }
    }

    @SuppressWarnings("unchecked")
    static <T> NonDeterministicTrie<T> empty()
    {
        return (NonDeterministicTrie<T>) NonDeterministicTrieImpl.EMPTY;
    }
//
    default Trie<T> deterministic(Trie.CollectionMergeResolver<T> resolver)
    {
        return new MergeAlternativeBranchesTrie<>(impl(), resolver, false);
    }

    private NonDeterministicTrieWithImpl<T> impl()
    {
        return NonDeterministicTrieImpl.impl(this);
    }
}
