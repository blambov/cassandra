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
    DeletionAwareTrie<T, D> deletion(ByteComparable prefixInMainTrie, ByteComparable left, ByteComparable right, ByteComparable.Version byteComparableVersion, D deletion)
    {
        RangeTrie<D> rangeTrie = RangeTrie.range(left, right, byteComparableVersion, deletion);
        return dir -> new SingletonCursor.DeletionBranch<>(dir,
                                                           prefixInMainTrie.asComparableBytes(byteComparableVersion), byteComparableVersion,
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
    }

    default DeletionAwareTrie<T, D> mergeWith(DeletionAwareTrie<T, D> other,
                                              Trie.MergeResolver<T> mergeResolver,
                                              Trie.MergeResolver<D> deletionResolver,
                                              BiFunction<D, T, T> deleter)
    {
        return dir -> new MergeCursor.DeletionAware<>(mergeResolver,
                                                      deletionResolver,
                                                      deleter,
                                                      cursor(dir),
                                                      other.cursor(dir));
    }

    default DeletionAwareTrie<T, D> mergeWith(DeletionAwareTrie<T, D> other, MergeResolver<T, D> mergeResolver)
    {
        return mergeWith(other, mergeResolver, mergeResolver::resolveMarkers, mergeResolver::applyMarker);
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
                return t1.mergeWith(t2, mergeResolver);
            }
            default:
                throw new AssertionError("not implemented");
//                return dir -> new CollectionMergeCursor.DeletionAware<>(dir, mergeResolver, sources);
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

    @Override
    default DeletionAwareTrie<T, D> tailTrie(ByteComparable prefix)
    {
        // TODO: What happens if the tail is after a splitting point?
        // TODO: What if the tail is covered by a deletion? We are not allowed to have open-ended deletion branches...
        DeletionAwareCursor<T, D> c = cursor(Direction.FORWARD);
        if (c.descendAlong(prefix.asComparableBytes(c.byteComparableVersion())))
            return c::tailCursor;
        else
            return empty(c.byteComparableVersion());
    }

    DeletionAwareCursor<T, D> makeCursor(Direction direction);

    @Override
    default DeletionAwareCursor<T, D> cursor(Direction direction)
    {
        return Trie.DEBUG ? new VerificationCursor.DeletionAware<>(makeCursor(direction))
                          : makeCursor(direction);
    }
}
