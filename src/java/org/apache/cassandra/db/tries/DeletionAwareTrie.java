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

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface DeletionAwareTrie<T extends DeletionAwareTrie.Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
extends BaseTrie<T, DeletionAwareCursor<T, D>, DeletionAwareTrie<T, D>>
{
    interface Deletable
    {
        // Marker interface, no specific methods
    }

    interface DeletionMarker<T extends Deletable, D extends DeletionMarker<T, D>> extends RangeState<D>
    {
        // TODO: Consider adding a applyTo/resolve methods; possibly in the whole hierarchy
//        T applyTo(T content);
//        D resolve(D other); // this could be in RangeState
    }

    static <T extends Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    DeletionAwareTrie<T, D> singleton(ByteComparable b, ByteComparable.Version byteComparableVersion, T v)
    {
        return dir -> new SingletonCursor.DeletionAware<>(dir, b.asComparableBytes(byteComparableVersion), byteComparableVersion, v);
    }

    @Override
    default DeletionAwareTrie<T, D> intersect(TrieSet set)
    {
        return dir -> new IntersectionCursor.DeletionAware<>(cursor(dir), set.cursor(dir));
    }

    interface MergeResolver<T extends Deletable, D extends DeletionMarker<T, D>> extends Trie.MergeResolver<T>
    {
        D resolveMarkers(D left, D right);
        T applyMarker(D marker, T content);
    }

    default DeletionAwareTrie<T, D> mergeWith(DeletionAwareTrie<T, D> other, MergeResolver<T, D> mergeResolver)
    {
        return dir -> new MergeCursor.DeletionAware<>(mergeResolver,
                                                      cursor(dir),
                                                      other.cursor(dir));
    }


    interface CollectionMergeResolver<T extends Deletable, D extends DeletionMarker<T, D>>
    extends MergeResolver<T, D>, Trie.CollectionMergeResolver<T>
    {
        D resolveMarkers(Collection<D> markers);

        @Override
        default D resolveMarkers(D c1, D c2)
        {
            return resolveMarkers(ImmutableList.of(c1, c2));
        }
    }

    static <T extends Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
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
                return dir -> new CollectionMergeCursor.DeletionAware<>(dir, mergeResolver, sources);
        }
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
        return dir -> new DeletionAwareCursor.LiveAndDeletionsMergeCursor<>(dir, resolver, cursor(dir));
    }

    @SuppressWarnings("unchecked")
    static <T extends Deletable, D extends DeletionAwareTrie.DeletionMarker<T, D>>
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
