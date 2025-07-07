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
import java.util.function.BiFunction;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface DeletionAwareTrie<T, D extends RangeState<D>>
extends BaseTrie<T, DeletionAwareCursor<T, D>, DeletionAwareTrie<T, D>>
{
    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> singleton(ByteComparable b, ByteComparable.Version byteComparableVersion, T v)
    {
        return dir -> new SingletonCursor.DeletionAware<>(dir, b.asComparableBytes(byteComparableVersion), byteComparableVersion, v);
    }

    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> deletion(ByteComparable prefixInDataTrie, ByteComparable left, ByteComparable right, ByteComparable.Version byteComparableVersion, D deletion)
    {
        RangeTrie<D> rangeTrie = RangeTrie.range(left, right, byteComparableVersion, deletion);
        return dir -> new SingletonCursor.DeletionBranch<>(dir,
                                                           prefixInDataTrie.asComparableBytes(byteComparableVersion), byteComparableVersion,
                                                           rangeTrie);
    }

    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> deletionBranch(ByteComparable prefixInDataTrie, ByteComparable.Version byteComparableVersion, RangeTrie<D> rangeTrie)
    {
        return dir -> new SingletonCursor.DeletionBranch<>(dir,
                                                           prefixInDataTrie.asComparableBytes(byteComparableVersion), byteComparableVersion,
                                                           rangeTrie);
    }

    @Override
    default DeletionAwareTrie<T, D> intersect(TrieSet set)
    {
        return dir -> new IntersectionCursor.DeletionAware<>(cursor(dir), set.cursor(dir));
    }

    interface MergeResolver<T, D extends RangeState<D>> extends Trie.MergeResolver<T>
    {
        D resolveMarkers(D left, D right);
        T applyMarker(D marker, T content);
        boolean deletionsAtFixedPoints();
    }

    /// @param deletionsAtFixedPoints TODO
    default DeletionAwareTrie<T, D> mergeWith(DeletionAwareTrie<T, D> other,
                                              Trie.MergeResolver<T> mergeResolver,
                                              Trie.MergeResolver<D> deletionResolver,
                                              BiFunction<D, T, T> deleter,
                                              boolean deletionsAtFixedPoints)
    {
        return dir -> new MergeCursor.DeletionAware<>(mergeResolver,
                                                      deletionResolver,
                                                      deleter,
                                                      cursor(dir),
                                                      other.cursor(dir),
                                                      deletionsAtFixedPoints);
    }

    default DeletionAwareTrie<T, D> mergeWith(DeletionAwareTrie<T, D> other, MergeResolver<T, D> mergeResolver)
    {
        return mergeWith(other, mergeResolver, mergeResolver::resolveMarkers, mergeResolver::applyMarker, mergeResolver.deletionsAtFixedPoints());
    }

    interface CollectionMergeResolver<T, D extends RangeState<D>>
    extends MergeResolver<T, D>, Trie.CollectionMergeResolver<T>
    {
        D resolveMarkers(Collection<D> markers);

        @Override
        default D resolveMarkers(D c1, D c2)
        {
            return resolveMarkers(ImmutableList.of(c1, c2));
        }
    }

    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> merge(Collection<? extends DeletionAwareTrie<T, D>> sources,
                                  CollectionMergeResolver<T, D> mergeResolver)
    {
        return merge(sources,
                     mergeResolver::resolve,
                     mergeResolver::resolveMarkers,
                     mergeResolver::applyMarker,
                     mergeResolver.deletionsAtFixedPoints());
    }

    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> merge(Collection<? extends DeletionAwareTrie<T, D>> sources,
                                  Trie.CollectionMergeResolver<T> mergeResolver,
                                  Trie.CollectionMergeResolver<D> deletionResolver,
                                  BiFunction<D, T, T> deleter,
                                  boolean deletionsAtFixedPoints)
    {
        switch (sources.size())
        {
            case 0:
                throw new AssertionError();
            case 1:
                return sources.iterator().next();
            case 2:
            {
                Iterator<? extends DeletionAwareTrie<T, D>> it = sources.iterator();
                DeletionAwareTrie<T, D> t1 = it.next();
                DeletionAwareTrie<T, D> t2 = it.next();
                // Create a combined resolver for the pairwise merge
                return t1.mergeWith(t2, mergeResolver, deletionResolver, deleter, deletionsAtFixedPoints);
            }
            default:
                return dir -> new CollectionMergeCursor.DeletionAware<>(mergeResolver,
                                                                        deletionResolver,
                                                                        deleter,
                                                                        deletionsAtFixedPoints,
                                                                        dir,
                                                                        sources,
                                                                        DeletionAwareTrie::cursor);
        }
    }

    interface DeletionAwareWalker<B, R> extends Cursor.Walker<B, R>
    {
        /// Called when a deletion branch is found. Return null to skip over it, or the walker to use to descend inside
        /// it.
        boolean enterDeletionsBranch();

        /// Called when the deletion branch is exited.
        void exitDeletionsBranch();
    }

    default String dump(Function<T, String> contentToString)
    {
        return dump(contentToString, Object::toString);
    }

    default String dump(Function<T, String> contentToString,
                        Function<D, String> rangeToString)
    {
        return process(Direction.FORWARD, new TrieDumper.DeletionAware<>(contentToString, rangeToString));
    }

    /// Process the trie using the given [DeletionAwareWalker].
    default <R> R process(Direction direction, DeletionAwareWalker<? super T, R> walker)
    {
        return cursor(direction).process(walker);
    }

    default Trie<T> contentOnlyTrie()
    {
        return this::cursor;
    }

    default RangeTrie<D> deletionOnlyTrie()
    {
        // We must walk the main trie to find deletion branch roots.
        return dir -> new DeletionAwareCursor.DeletionsTrieCursor<>(cursor(dir));
    }

    default <Z> Trie<Z> mergedTrie(BiFunction<T, D, Z> resolver)
    {
        return dir -> new DeletionAwareCursor.LiveAndDeletionsMergeCursor<>(resolver, cursor(dir));
    }

    @SuppressWarnings("unchecked")
    static <T, D extends RangeState<D>>
    DeletionAwareTrie<T, D> empty(ByteComparable.Version byteComparableVersion)
    {
        return direction -> new DeletionAwareCursor.Empty<>(direction, byteComparableVersion);
    }

    @Override
    default DeletionAwareTrie<T, D> prefixedBy(ByteComparable prefix)
    {
        return dir -> new PrefixedCursor.DeletionAware<>(prefix, cursor(dir));
    }

    /// @inheritDoc
    ///
    /// Note: if the cursor is positioned below a deletion branch root, the tail will not include any information about
    /// that deletion branch, even if it applies to the current position.
    @Override
    default DeletionAwareTrie<T, D> tailTrie(ByteComparable prefix)
    {
        DeletionAwareCursor<T, D> c = cursor(Direction.FORWARD);
        if (c.descendAlong(prefix.asComparableBytes(c.byteComparableVersion())))
            return c::tailCursor;
        else
            return empty(c.byteComparableVersion());
    }

    /// Returns an entry set containing all tail tree constructed at the points that contain content of
    /// the given type.
    default Iterable<Map.Entry<ByteComparable, DeletionAwareTrie<T, D>>> tailTries(Direction direction, Class<? extends T> clazz)
    {
        return () -> new TrieTailsIterator.AsEntriesDeletionAware<>(cursor(direction), clazz);
    }

    DeletionAwareCursor<T, D> makeCursor(Direction direction);

    @Override
    default DeletionAwareCursor<T, D> cursor(Direction direction)
    {
        return Trie.DEBUG ? new VerificationCursor.DeletionAware<>(makeCursor(direction))
                          : makeCursor(direction);
    }
}
