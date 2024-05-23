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

public interface NonDeterministicTrie<T extends NonDeterministicTrie.Mergeable<T>> extends BaseTrie<T>
{
    interface Mergeable<T>
    {
        T mergeWith(T other);
    }

    /**
     * Call the given consumer on all content values in the trie in order.
     */
    @Override
    default void forEachValue(ValueConsumer<T> consumer, Direction direction)
    {
        impl().process(consumer, direction);
    }

    /**
     * Call the given consumer on all (path, content) pairs with non-null content in the trie in order.
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
        return new TrieEntriesIterator.AsEntries<>(impl().alternativesMergingCursor(direction));
    }

    /**
     * Returns the ordered set of values of this trie in an iterator.
     */
    @Override
    default Iterator<T> valueIterator(Direction direction)
    {
        return new TrieValuesIterator<>(impl().alternativesMergingCursor(direction));
    }

    /**
     * Returns the values in any order. For some tries this is much faster than the ordered iterable.
     */
    default Iterable<T> valuesUnordered()
    {
        return values();
    }

    /**
     * Constuct a textual representation of the trie using the given content-to-string mapper.
     */
    @Override
    default String dump(Function<T, String> contentToString)
    {
        return impl().process(new TrieDumper<>(contentToString), Direction.FORWARD);
    }

    /**
     * Returns a singleton trie mapping the given byte path to content.
     */
    static <T extends NonDeterministicTrie.Mergeable<T>> NonDeterministicTrie<T> singleton(ByteComparable b, T v)
    {
        return (NonDeterministicTrieWithImpl<T>) dir -> new SingletonCursor.NonDeterministic<>(dir, b, v);
    }

    /**
     * Returns a view of the subtrie containing everything in this trie whose keys fall between the given boundaries,
     * inclusive of both bounds and any prefix of the bounds.
     * <p>
     * The view is live, i.e. any write to the source will be reflected in the subtrie.
     * <p>
     * This method will not check its arguments for correctness. The resulting trie may throw an exception if the right
     * bound is smaller than the left.
     * <p>
     * This package is designed to walk tries efficiently using cursors that necessarily present prefix nodes before
     * children. Lexicographically correct slices (where e.g. the left bound and prefixes of the right are included in
     * the set but prefixes of the left are not) are not contiguous in this representation in both iteration directions
     * (because a prefix of the left bound must necessarily be presented before the left bound itself in reverse order)
     * and are thus not supported. However, if the encoded keys are prefix-free, this limitation is immaterial.
     * <p>
     * @param left the left bound for the returned subtrie. If {@code null}, the resulting subtrie is not left-bounded.
     * @param right the right bound for the returned subtrie. If {@code null}, the resulting subtrie is not right-bounded.
     * @return a view of the subtrie containing all the keys of this trie falling between {@code left} and {@code right},
     * including both bounds and any prefix of the bounds.
     */
    @Override
    default NonDeterministicTrie<T> subtrie(ByteComparable left, ByteComparable right)
    {
        return intersect(RangesTrieSet.create(left, right));
    }

    /**
     * Returns a view of this trie that is an intersection of its content with the given set.
     * <p>
     * The view is live, i.e. any write to the source will be reflected in the intersection.
     */
    @Override
    default NonDeterministicTrie<T> intersect(TrieSet set)
    {
        return (NonDeterministicTrieWithImpl<T>)
               dir -> new IntersectionCursor.NonDeterministic<>(dir,
                                                                impl().cursor(dir),
                                                                TrieSetImpl.impl(set).cursor(dir));
    }

    /**
     * Constructs a view of the merge of this trie with the given one. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     * <p>
     * If there is content for a given key in both sources, the resolver will be called to obtain the combination.
     * (The resolver will not be called if there's content from only one source.)
     */
    default NonDeterministicTrie<T> mergeWith(NonDeterministicTrie<T> other)
    {
        return (NonDeterministicTrieWithImpl<T>) dir -> new MergeCursor.NonDeterministic<>(dir, impl(), other.impl());
    }

    /**
     * Constructs a view of the merge of multiple tries. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     * <p>
     * If there is content for a given key in more than one sources, the resolver will be called to obtain the
     * combination. (The resolver will not be called if there's content from only one source.)
     */
    static <T extends NonDeterministicTrie.Mergeable<T>>
    NonDeterministicTrie<T> merge(Collection<? extends NonDeterministicTrie<T>> sources)
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
                return t1.mergeWith(t2);
            }
            default:
                return (NonDeterministicTrieWithImpl<T>) dir -> new CollectionMergeCursor.NonDeterministic<>(dir, sources);
        }
    }

    @SuppressWarnings("unchecked")
    static <T extends NonDeterministicTrie.Mergeable<T>> NonDeterministicTrie<T> empty()
    {
        return (NonDeterministicTrie<T>) NonDeterministicTrieImpl.EMPTY;
    }

    default Trie<T> deterministic()
    {
        return new MergeAlternativeBranchesTrie<>(impl(), false);
    }

    default Trie<T> mainPathOnly()
    {
        return (TrieWithImpl<T>) impl()::cursor;
    }

    default Trie<T> alternatesOnly()
    {
        return new MergeAlternativeBranchesTrie<>(impl(), true);
    }

    private NonDeterministicTrieWithImpl<T> impl()
    {
        return NonDeterministicTrieImpl.impl(this);
    }
}
