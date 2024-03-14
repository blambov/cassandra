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

public interface RangeTrie<T extends RangeTrieImpl.RangeMarker<T>> extends BaseTrie<T>
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
    static <T extends RangeTrieImpl.RangeMarker<T>> RangeTrie<T> singleton(ByteComparable b, T v)
    {
        return (RangeTrieWithImpl<T>) () -> new SingletonCursor.Range<>(b, v);
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
    default RangeTrie<T> subtrie(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
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
    default RangeTrie<T> subtrie(ByteComparable left, ByteComparable right)
    {
        return intersect(RangesTrieSet.create(left, right));
    }

    /**
     * Returns a view of this trie that is an intersection of its content with the given set.
     * <p>
     * The view is live, i.e. any write to the source will be reflected in the intersection.
     */
    default RangeTrie<T> intersect(TrieSet set)
    {
        return intersect(set, RangeTrieImpl.rangeAndSetIntersectionController());
    }


    default RangeTrie<T> intersect(TrieSet set, RangeIntersectionCursor.IntersectionController<TrieSetImpl.RangeState, T, T> controller)
    {
        return (RangeTrieWithImpl<T>) () -> new RangeIntersectionCursor<>(controller, TrieSetImpl.impl(set).cursor(), impl().cursor());
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

    interface MergeResolver<M extends RangeTrieImpl.RangeMarker<M>> extends Trie.MergeResolver<M>
    {
        M resolve(M left, boolean atC1, M right, boolean atC2);

        default M resolve(M left, M right)
        {
            return resolve(left, true, right, true);
        }
    }

    /**
     * Constructs a view of the merge of this trie with the given one. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     * <p>
     * If there is content for a given key in both sources, the resolver will be called to obtain the combination.
     * (The resolver will not be called if there's content from only one source.)
     */
    default RangeTrie<T> mergeWith(RangeTrie<T> other, MergeResolver<T> resolver)
    {
        return (RangeTrieWithImpl<T>) () -> new MergeCursor.Range<>(resolver, impl(), other.impl());
//        return (RangeTrieWithImpl<T>) () -> new RangeIntersectionCursor<>(new RangeIntersectionCursor.IntersectionController<T, T, T>()
//        {
//            @Override
//            public T combineState(T lState, T rState)
//            {
//                if (lState == null)
//                    return rState;
//                if (rState == null)
//                    return lState;
//                return resolver.resolve(lState, rState);
//            }
//
//            @Override
//            public boolean includeLesserLeft(T lState)
//            {
//                return true;
//            }
//
//            @Override
//            public boolean includeLesserRight(T rState)
//            {
//                return true;
//            }
//
//            @Override
//            public T combineStateCoveringLeft(T rState, T lCoveringState)
//            {
//                if (lCoveringState == null)
//                    return rState;
//                return combineState(lCoveringState.leftSideAsActive(), rState);
//            }
//
//            @Override
//            public T combineStateCoveringRight(T lState, T rCoveringState)
//            {
//                if (rCoveringState == null)
//                    return lState;
//                return combineState(lState, rCoveringState.leftSideAsActive());
//            }
//        }, impl().cursor(), other.impl().cursor());
    }

    /**
     * Constructs a view of the merge of multiple tries. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     * <p>
     * If there is content for a given key in more than one sources, the resolver will be called to obtain the
     * combination. (The resolver will not be called if there's content from only one source.)
     */
//    static <T extends RangeTrieImpl.RangeMarker<T>> RangeTrie<T> merge(Collection<? extends RangeTrie<T>> sources, Trie.CollectionMergeResolver<T> resolver)
//    {
//        switch (sources.size())
//        {
//            case 0:
//                return empty();
//            case 1:
//                return sources.iterator().next();
//            case 2:
//            {
//                Iterator<? extends RangeTrie<T>> it = sources.iterator();
//                RangeTrie<T> t1 = it.next();
//                RangeTrie<T> t2 = it.next();
//                return t1.mergeWith(t2, resolver);
//            }
//            default:
//                return (RangeTrieWithImpl<T>) () -> new CollectionMergeCursor.Range<>(resolver, sources);
//        }
//    }

    @SuppressWarnings("unchecked")
    static <T extends RangeTrieImpl.RangeMarker<T>> RangeTrie<T> empty()
    {
        return (RangeTrie<T>) RangeTrieImpl.EMPTY;
    }

    private RangeTrieWithImpl<T> impl()
    {
        return RangeTrieImpl.impl(this);
    }
}
