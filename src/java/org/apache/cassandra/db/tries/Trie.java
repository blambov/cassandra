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
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

import org.agrona.DirectBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Base class for tries.
 * <p>
 * Normal users of tries will only use the public methods, which provide various transformations of the trie, conversion
 * of its content to other formats (e.g. iterable of values), and several forms of processing.
 * <p>
 * For any unimplemented data extraction operations one can build on the {@link TrieEntriesWalker} (for-each processing)
 * and {@link TrieEntriesIterator} (to iterator) base classes, which provide the necessary mechanisms to handle walking
 * the trie.
 * <p>
 * The internal representation of tries using this interface is defined in the {@link TrieImpl.Cursor} interface.
 * <p>
 * Cursors are a method of presenting the internal structure of a trie without representing nodes as objects, which is
 * still useful for performing the basic operations on tries (iteration, slicing/intersection and merging). A cursor
 * will list the nodes of a trie in order, together with information about the path that was taken to reach them.
 * <p>
 * To begin traversal over a trie, one must retrieve a cursor by calling {@link #cursor()}. Because cursors are
 * stateful, the traversal must always proceed from one thread. Should concurrent reads be required, separate calls to
 * {@link TrieImpl#cursor()} must be made. Any modification that has completed before the construction of a cursor must
 * be visible, but any later concurrent modifications may be presented fully, partially or not at all; this also means
 * that if multiple are made, the cursor may see any part of any subset of them.
 * <p>
 * Note: This model only supports depth-first traversals. We do not currently have a need for breadth-first walks.
 * <p>
 * See Trie.md for further description of the trie representation model.
 * <p>
 * @param <T> The content type of the trie.
 */
public interface Trie<T>
{
    // done: determinization / mergeAlternatives (needed to run any tests)
    // done: alternate branch merge
    // done: alternate branch intersect
    // done: alternateView
    // done: no-content branch removal (for alternateView)
    // done: adding alternate paths (put) to InMemoryTrie
    // done: putAlternateRange for range tombstones, should place alternate branch at LCA of start and end (or higher if no live branch exists there)
    // done: alternate range trie constructor for apply-version of putAlternateRange
    // done: adding alternate branches (apply) to InMemoryTrie -- no resolution/simplification of alternate necessary
    // done: test put/apply alternate range with LCA test
    // done: test duplicate impl (ByteSource and Cursor)
    // done: range-deletion-aware intersections (pluggable) (where active range is presented at boundary)
    // TODO: deletion-aware merge (pluggable)
    // TODO: deletion-aware InMemoryTrie methods (pluggable)
    // TODO: simplification of alternate path for flush (pluggable?)
    // TODO: figure out if duplicate should only cover branch or full backtrack
    // TODO: deletion summarization (with timestamps? pluggable)
    // TODO: consider mayHaveAlternatives flag

    // TODO: reverse iteration
    // TODO: consistency levels / copy on write + node reuse

    /**
     * Adapter interface providing the methods a {@link TrieImpl.Walker} to a {@link Consumer}, so that the latter can be used
     * with {@link TrieImpl#process}.
     *
     * This enables calls like
     *     trie.forEachEntry(x -> System.out.println(x));
     * to be mapped directly to a single call to {@link TrieImpl#process} without extra allocations.
     */
    interface ValueConsumer<T> extends Consumer<T>, TrieImpl.Walker<T, Void>
    {
        @Override
        default void content(T content)
        {
            accept(content);
        }

        @Override
        default Void complete()
        {
            return null;
        }

        @Override
        default void resetPathLength(int newDepth)
        {
            // not tracking path
        }

        @Override
        default void addPathByte(int nextByte)
        {
            // not tracking path
        }

        @Override
        default void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            // not tracking path
        }
    }

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
    static <T> Trie<T> singleton(ByteComparable b, T v)
    {
        return new SingletonTrie<>(b, v);
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
    default Trie<T> subtrie(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        if (left == null && right == null)
            return this;
        return new IntersectionTrie<>(impl(), RangesTrieSet.create(left, includeLeft, right, includeRight));
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
    default Trie<T> subtrie(ByteComparable left, ByteComparable right)
    {
        return new IntersectionTrie<>(impl(), RangesTrieSet.create(left, right));
    }

    /**
     * Returns a view of this trie that is an intersection of its content with the given set.
     * <p>
     * The view is live, i.e. any write to the source will be reflected in the intersection.
     */
    default Trie<T> intersect(TrieSet set)
    {
        return new IntersectionTrie<>(impl(), TrieSetImpl.impl(set));
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
        return new TrieEntriesIterator.AsEntries<>(impl());
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
        return new TrieValuesIterator<>(impl());
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
    default Trie<T> mergeWith(Trie<T> other, MergeResolver<T> resolver)
    {
        return new MergeTrie<>(resolver, impl(), other.impl());
    }

    /**
     * Resolver of content of merged nodes.
     * <p>
     * The resolver's methods are only called if more than one of the merged nodes contain content, and the
     * order in which the arguments are given is not defined. Only present non-null values will be included in the
     * collection passed to the resolving methods.
     * <p>
     * Can also be used as a two-source resolver.
     */
    interface CollectionMergeResolver<T> extends MergeResolver<T>
    {
        T resolve(Collection<T> contents);

        @Override
        default T resolve(T c1, T c2)
        {
            return resolve(ImmutableList.of(c1, c2));
        }
    }

    /**
     * Returns a resolver that throws whenever more than one of the merged nodes contains content.
     * Can be used to merge tries that are known to have distinct content paths.
     */
    @SuppressWarnings("unchecked")
    static <T> CollectionMergeResolver<T> throwingResolver()
    {
        return (CollectionMergeResolver<T>) TrieImpl.THROWING_RESOLVER;
    }

    /**
     * Constructs a view of the merge of multiple tries. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     * <p>
     * If there is content for a given key in more than one sources, the resolver will be called to obtain the
     * combination. (The resolver will not be called if there's content from only one source.)
     */
    static <T> Trie<T> merge(Collection<? extends Trie<T>> sources, CollectionMergeResolver<T> resolver)
    {
        switch (sources.size())
        {
        case 0:
            return empty();
        case 1:
            return sources.iterator().next();
        case 2:
        {
            Iterator<? extends Trie<T>> it = sources.iterator();
            Trie<T> t1 = it.next();
            Trie<T> t2 = it.next();
            return t1.mergeWith(t2, resolver);
        }
        default:
            return new CollectionMergeTrie<>(sources, resolver);
        }
    }

    /**
     * Constructs a view of the merge of multiple tries, where each source must have distinct keys. The view is live,
     * i.e. any write to any of the sources will be reflected in the merged view.
     * <p>
     * If there is content for a given key in more than one sources, the merge will throw an assertion error.
     */
    static <T> Trie<T> mergeDistinct(Collection<? extends Trie<T>> sources)
    {
        switch (sources.size())
        {
        case 0:
            return empty();
        case 1:
            return sources.iterator().next();
        case 2:
        {
            Iterator<? extends Trie<T>> it = sources.iterator();
            Trie<T> t1 = it.next();
            Trie<T> t2 = it.next();
            return new MergeTrie.Distinct<>(t1.impl(), t2.impl());
        }
        default:
            return new CollectionMergeTrie.Distinct<>(sources);
        }
    }

    @SuppressWarnings("unchecked")
    static <T> Trie<T> empty()
    {
        return (Trie<T>) TrieImpl.EMPTY;
    }

    default Trie<T> mergeAlternativeBranches(CollectionMergeResolver<T> resolver)
    {
        return new MergeAlternativeBranchesTrie<>(impl(), resolver, false);
    }

    default Trie<T> alternateView(CollectionMergeResolver<T> resolver)
    {
        return new MergeAlternativeBranchesTrie<>(impl(), resolver, true);
    }

    private TrieWithImpl<T> impl()
    {
        return TrieImpl.impl(this);
    }
}
