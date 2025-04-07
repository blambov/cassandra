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

import com.google.common.base.Preconditions;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface RangeTrie<M extends RangeMarker<M>> extends BaseTrie<M, RangeCursor<M>, RangeTrie<M>>
{
    default <R> R process(Direction direction, Cursor.Walker<M, R> walker)
    {
        return cursor(direction).process(walker);
    }

    /// Returns a singleton range trie covering the given branch.
    static <M extends RangeMarker<M>> RangeTrie<M> singleton(ByteComparable b, ByteComparable.Version byteComparableVersion, M v)
    {
        Preconditions.checkArgument(v.toContent() == v);
//        Preconditions.checkArgument(v.precedingState(Direction.FORWARD) == null);
//        Preconditions.checkArgument(v.precedingState(Direction.REVERSE) == null);
        return dir -> new SingletonCursor.Range<>(dir, b.asComparableBytes(byteComparableVersion), byteComparableVersion, v);
    }

    /// Returns a range trie covering a single range.
    static <M extends RangeMarker<M>> RangeTrie<M> range(ByteComparable left, ByteComparable right, ByteComparable.Version byteComparableVersion, M v)
    {
//        Preconditions.checkArgument(v.toContent() == v);
//        Preconditions.checkArgument(v.precedingState(Direction.FORWARD) == null);
//        Preconditions.checkArgument(v.precedingState(Direction.REVERSE) == null);
        return singleton(ByteComparable.EMPTY, byteComparableVersion, v).intersect(TrieSet.range(byteComparableVersion, left, right));
    }

    @Override
    default RangeTrie<M> intersect(TrieSet set)
    {
        return dir -> new RangeIntersectionCursor(cursor(dir), set.cursor(dir));
    }

    /// Constructs a view of the merge of this trie with the given one. The view is live, i.e. any write to any of the
    /// sources will be reflected in the merged view.
    ///
    /// If there is content for a given key in both sources, the resolver will be called to obtain the combination.
    /// (The resolver will not be called if there's content from only one source.)
    default RangeTrie<M> mergeWith(RangeTrie<M> other, Trie.MergeResolver<M> resolver)
    {
        return dir -> new MergeCursor.Range<>(resolver, cursor(dir), other.cursor(dir));
    }

    /// Constructs a view of the merge of multiple tries. The view is live, i.e. any write to any of the
    /// sources will be reflected in the merged view.
    ///
    /// If there is content for a given key in more than one sources, the resolver will be called to obtain the
    /// combination. (The resolver will not be called if there's content from only one source.)
    static <M extends RangeMarker<M>> RangeTrie<M> merge(Collection<? extends RangeTrie<M>> sources, Trie.CollectionMergeResolver<M> resolver)
    {
        switch (sources.size())
        {
            case 0:
                throw new AssertionError();
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
                return dir -> new CollectionMergeCursor.Range<>(resolver, dir, sources, RangeTrie::cursor);
        }
    }

    @SuppressWarnings("unchecked")
    static <M extends RangeMarker<M>> RangeTrie<M> empty(ByteComparable.Version version)
    {
        return dir -> RangeCursor.empty(dir, version);
    }

    @Override
    default RangeTrie<M> prefixedBy(ByteComparable prefix)
    {
        return dir -> new PrefixedCursor.Range<>(prefix, cursor(dir));
    }

    @Override
    default RangeTrie<M> tailTrie(ByteComparable prefix)
    {
        RangeCursor<M> c = cursor(Direction.FORWARD);
        if (c.descendAlong(prefix.asComparableBytes(c.byteComparableVersion())))
            return c::tailCursor;
        else
            return c::precedingStateCursor;
    }

    RangeCursor<M> makeCursor(Direction direction);

    @Override
    default RangeCursor<M> cursor(Direction direction)
    {
        return Trie.DEBUG ? new VerificationCursor.Range<>(makeCursor(direction), 0, 0, -1)
                          : makeCursor(direction);
    }
}
