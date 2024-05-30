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
import java.util.function.Predicate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CassandraRelevantProperties;
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
 * The internal representation of tries using this interface is defined in the {@link CursorWalkable.Cursor} interface.
 * <p>
 * Cursors are a method of presenting the internal structure of a trie without representing nodes as objects, which is
 * still useful for performing the basic operations on tries (iteration, slicing/intersection and merging). A cursor
 * will list the nodes of a trie in order, together with information about the path that was taken to reach them.
 * <p>
 * To begin traversal over a trie, one must retrieve a cursor by calling {@link CursorWalkable#cursor}. Because cursors
 * are stateful, the traversal must always proceed from one thread. Should concurrent reads be required, separate calls
 * to {@link CursorWalkable#cursor(Direction)} must be made. Any modification that has completed before the construction
 * of a cursor must be visible, but any later concurrent modifications may be presented fully, partially or not at all;
 * this also means that if multiple are made, the cursor may see any part of any subset of them.
 * <p>
 * Note: This model only supports depth-first traversals. We do not currently have a need for breadth-first walks.
 * <p>
 * See Trie.md for further description of the trie representation model.
 * <p>
 * @param <T> The content type of the trie.
 */
public interface Trie<T> extends BaseTrie<T>
{
    // Necessary:
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
    // done: deletion-aware merge (pluggable)
    // done: deletion-aware InMemoryTrie methods (pluggable)

    // TODO: reverse iteration
    // TODO: define slices as always including prefixes and exact matches

    // TODO: Singleton/range deletion cursor to use for apply operations in PartitionUpdate

    // Necessary post-POC improvements:
    // TODO: node reuse
    // TODO: apply existing deletions on updates in InMemoryDATree.apply
    // TODO: delete on the way back in apply
    // TODO: consistency/copy-on-write levels
    // Optimizations:
    // TODO: simplification of deletion branch for flush (pluggable?)
    // TODO: figure out if duplicate should only cover branch or full backtrack
    // TODO: deletion summarization (with timestamps? pluggable)

    // TODO: RangeTrie using state() that combines content and coveringState appears to be better after all.
    // TODO: Make sure range trie state() and coveringState() (i.e. state().leftSideAsCovering()) are never recomputed

    // TODO: introduce and return flags instead of depth
    // (e.g. DESCENDED, EXHAUSTED, SKIP_TO_MATCHED, HAS_CONTENT, HAS_ALTERNATIVE/DELETION, HAS_DELETION_STATE)
    // maybe combine depth with flags

    // TODO: mayHaveDeletions flag or in-tree metadata for newest/oldest deletion and newest/oldest timestamp

    // Maybe: Construct from content-only and deletion-only tries (this could solve the "where to root the deletion in the memtable" question)
    // Maybe: Change test to work with the above

    // Maybe: deletion-only tries of merges/intersections can be transformed separately from deletion-only tries

    // Cleanup:
    // TODO: comments are very out-of-date
    // TODO: Deletion-aware merges may report different results when deletionBranch is called differently (if common root is not queried)


    static final boolean DEBUG = CassandraRelevantProperties.TRIE_DEBUG.getBoolean();

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
    static <T> Trie<T> singleton(ByteComparable b, T v)
    {
        return (TrieWithImpl<T>) dir -> new SingletonCursor<>(dir, b, v);
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
    default Trie<T> subtrie(ByteComparable left, ByteComparable right)
    {
        return intersect(RangesTrieSet.create(left, right));
    }

    /**
     * Returns a view of this trie that is an intersection of its content with the given set.
     * <p>
     * The view is live, i.e. any write to the source will be reflected in the intersection.
     */
    @Override
    default Trie<T> intersect(TrieSet set)
    {
        return (TrieWithImpl<T>) dir -> new IntersectionCursor.Deterministic<>(dir, impl().cursor(dir), TrieSetImpl.impl(set).cursor(dir));
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
        return (TrieWithImpl<T>) dir -> new MergeCursor.Deterministic<>(dir, resolver, impl(), other.impl());
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
            return (TrieWithImpl<T>) dir -> new CollectionMergeCursor.Deterministic<>(dir, resolver, sources);
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
            return new TrieWithImpl<T>()
            {
                @Override
                public Cursor<T> makeCursor(Direction direction)
                {
                    return new MergeCursor.Deterministic<>(direction, throwingResolver(), t1.impl(), t2.impl());
                }

                @Override
                public Iterable<T> valuesUnordered()
                {
                    return Iterables.concat(t1.valuesUnordered(), t2.valuesUnordered());
                }
            };
        }
        default:
            return new TrieWithImpl<T>()
            {
                @Override
                public Cursor<T> makeCursor(Direction direction)
                {
                    return new CollectionMergeCursor.Deterministic<>(direction, throwingResolver(), sources);
                }

                @Override
                public Iterable<T> valuesUnordered()
                {
                    return Iterables.concat(Iterables.transform(sources, Trie::valuesUnordered));
                }
            };
        }
    }

    @SuppressWarnings("unchecked")
    static <T> Trie<T> empty()
    {
        return (Trie<T>) TrieImpl.EMPTY;
    }

    /**
     * Returns a Trie that is a view of this one, where the given prefix is prepended before the root.
     */
    @Override
    default Trie<T> prefix(ByteComparable prefix)
    {
        return (TrieWithImpl<T>) dir -> new PrefixedCursor.Deterministic<>(dir, prefix, impl().cursor(dir));
    }

    default Iterable<Map.Entry<ByteComparable, Trie<T>>> tailTries(Predicate<T> predicate, Direction direction)
    {
        return () -> new TrieTailsIterator.AsEntries<>(direction, impl().cursor(direction), predicate, this::tailTrie);
    }

    @Override
    default Trie<T> tailTrie(ByteComparable prefix)
    {
        // This could be done with a prewalked cursor, e.g. as
        //        TrieImpl.Cursor<T> c = impl().cursor(Direction.FORWARD);
        //        if (CursorWalkable.walk(c, prefix))
        //        {
        //            var tailCursor = new TailCursor.Deterministic<>(c);
        //            return (TrieWithImpl<T>) dir -> tailCursor.duplicate();
        //        }
        // but we can't currently handle direction switching. Instead, do the walk during cursor construction.
        return (TrieWithImpl<T>) dir -> {
            TrieImpl.Cursor<T> c = impl().cursor(dir);
            if (c.descendAlong(prefix.asComparableBytes(CursorWalkable.BYTE_COMPARABLE_VERSION)))
                return new TailCursor.Deterministic<>(c);
            else
                return new TrieImpl.EmptyCursor<>();
        };
    }

    private TrieWithImpl<T> impl()
    {
        return TrieImpl.impl(this);
    }
}
