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

import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

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
 * To begin traversal over a trie, one must retrieve a cursor by calling {@link TrieImpl#cursor()}. Because cursors are
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
public interface BaseTrie<T>
{
    /**
     * Adapter interface providing the methods a {@link TrieImpl.Walker} to a {@link Consumer}, so that the latter can be used
     * with {@link TrieImpl#process}.
     * <p>
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
    void forEachValue(ValueConsumer<T> consumer);

    /**
     * Call the given consumer on all (path, content) pairs with non-null content in the trie in order.
     */
    void forEachEntry(BiConsumer<ByteComparable, T> consumer);

    /**
     * Constuct a textual representation of the trie.
     */
    String dump();

    /**
     * Constuct a textual representation of the trie using the given content-to-string mapper.
     */
    String dump(Function<T, String> contentToString);

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
    BaseTrie<T> subtrie(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight);

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
    BaseTrie<T> subtrie(ByteComparable left, ByteComparable right);

    /**
     * Returns a view of this trie that is an intersection of its content with the given set.
     * <p>
     * The view is live, i.e. any write to the source will be reflected in the intersection.
     */
    BaseTrie<T> intersect(TrieSet set);

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
    Iterator<Map.Entry<ByteComparable, T>> entryIterator();

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
    Iterator<T> valueIterator();
}
