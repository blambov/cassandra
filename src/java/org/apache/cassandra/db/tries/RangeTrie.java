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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface RangeTrie<M extends RangeTrie.RangeMarker<M>> extends BaseTrie<M>
{

    interface RangeMarker<M extends RangeMarker<M>>
    {
        M toContent();
        M leftSideAsCovering(/*side*/); // TODO: For reverse iteration this should accept a direction
        M rightSideAsCovering();  // TODO: combine with above when reversed iteration is done
        M asReportableStart(); // from covering state; TODO: direction parameter and combine with next
        M asReportableEnd();

        boolean lesserIncluded();
        default boolean agreesWith(M other)
        {
            return equals(other);
        }
    }

    /**
     * Call the given consumer on all content values in the trie in order.
     */
    @Override
    default void forEachValue(ValueConsumer<M> consumer, Direction direction)
    {
        impl().process(consumer, direction);
    }

    /**
     * Call the given consumer on all (path, content) pairs with non-null content in the trie in order.
     */
    @Override
    default void forEachEntry(BiConsumer<ByteComparable, M> consumer, Direction direction)
    {
        impl().process(new TrieEntriesWalker.WithConsumer<>(consumer), direction);
        // Note: we can't do the ValueConsumer trick here, because the implementation requires state and cannot be
        // implemented with default methods alone.
    }

    /**
     * Returns the ordered entry set of this trie's content in an iterator.
     */
    @Override
    default Iterator<Map.Entry<ByteComparable, M>> entryIterator(Direction direction)
    {
        return new TrieEntriesIterator.AsEntries<>(impl().cursor(direction));
    }

    /**
     * Returns the ordered set of values of this trie in an iterator.
     */
    @Override
    default Iterator<M> valueIterator(Direction direction)
    {
        return new TrieValuesIterator<>(impl().cursor(direction));
    }

    /**
     * Constuct a textual representation of the trie using the given content-to-string mapper.
     */
    @Override
    default String dump(Function<M, String> contentToString)
    {
        return impl().process(new TrieDumper<>(contentToString), Direction.FORWARD);
    }

    /**
     * Returns a singleton trie mapping the given byte path to content.
     */
    static <T extends RangeMarker<T>> RangeTrie<T> singleton(ByteComparable b, T v)
    {
        return (RangeTrieWithImpl<T>) dir -> new SingletonCursor.Range<>(b, v);
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
    default RangeTrie<M> subtrie(ByteComparable left, ByteComparable right)
    {
        return intersect(RangesTrieSet.create(left, right));
    }

    /**
     * Returns a view of this trie that is an intersection of its content with the given set.
     * <p>
     * The view is live, i.e. any write to the source will be reflected in the intersection.
     */
    @Override
    default RangeTrie<M> intersect(TrieSet set)
    {
        return intersect(set, RangeTrieImpl.rangeAndSetIntersectionController());
    }

    default RangeTrie<M> intersect(TrieSet set, RangeIntersectionCursor.IntersectionController<TrieSetImpl.RangeState, M, M> controller)
    {
        return (RangeTrieWithImpl<M>) dir -> new RangeIntersectionCursor<>(dir,
                                                                           controller,
                                                                           TrieSetImpl.impl(set).cursor(dir),
                                                                           impl().cursor(dir));
    }

    /**
     * Constructs a view of the merge of this trie with the given one. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     * <p>
     * If there is content for a given key in both sources, the resolver will be called to obtain the combination.
     * (The resolver will not be called if there's content from only one source.)
     */
    default RangeTrie<M> mergeWith(RangeTrie<M> other, Trie.MergeResolver<M> resolver)
    {
        return (RangeTrieWithImpl<M>) dir -> new MergeCursor.Range<>(dir, resolver, impl(), other.impl());
    }

    /**
     * Constructs a view of the merge of multiple tries. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     * <p>
     * If there is content for a given key in more than one sources, the resolver will be called to obtain the
     * combination. (The resolver will not be called if there's content from only one source.)
     */
    static <M extends RangeTrie.RangeMarker<M>> RangeTrie<M> merge(Collection<? extends RangeTrie<M>> sources, Trie.CollectionMergeResolver<M> resolver)
    {
        switch (sources.size())
        {
            case 0:
                return empty();
            case 1:
                return sources.iterator().next();
            case 2:
            {
                Iterator<? extends RangeTrie<M>> it = sources.iterator();
                RangeTrie<M> t1 = it.next();
                RangeTrie<M> t2 = it.next();
                return t1.mergeWith(t2, resolver);
            }
            default:
                return (RangeTrieWithImpl<M>) dir -> new CollectionMergeCursor.Range<>(dir, resolver, sources);
        }
    }

    /**
     * Applies these ranges to a given trie. The meaning of the application is defined by the given mapper:
     * whenever the trie's content falls under a range, the mapper is called to return the content that should be
     * presented.
     */
    default <T> Trie<T> applyTo(Trie<T> source, BiFunction<M, T, T> mapper)
    {
        return (TrieWithImpl<T>) dir -> new MergeCursor.RangeOnTrie<>(dir, mapper, impl(), TrieImpl.impl(source));
    }

    @SuppressWarnings("unchecked")
    static <M extends RangeMarker<M>> RangeTrie<M> empty()
    {
        return (RangeTrie<M>) RangeTrieImpl.EMPTY;
    }

    private RangeTrieWithImpl<M> impl()
    {
        return RangeTrieImpl.impl(this);
    }
}
