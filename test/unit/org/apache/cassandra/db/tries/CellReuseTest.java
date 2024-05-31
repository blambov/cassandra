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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.collect.Streams;
import org.junit.Assert;
import org.junit.Test;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.apache.cassandra.db.tries.TrieUtil.assertMapEquals;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;

public class CellReuseTest
{
    static Predicate<InMemoryTrie.NodeFeatures<Object>> FORCE_COPY_PARTITION = features -> {
        var c = features.content();
        if (c != null && c instanceof Boolean)
            return (Boolean) c;
        else
            return false;
    };

    static Predicate<InMemoryTrie.NodeFeatures<Object>> NO_ATOMICITY = features -> false;

    private static final int COUNT = 10000;
    Random rand = new Random(2);

    @Test
    public void testCellReusePartitionCopying() throws Exception
    {
        testCellReuse(FORCE_COPY_PARTITION);
    }

    @Test
    public void testCellReuseNoCopying() throws Exception
    {
        testCellReuse(NO_ATOMICITY);
    }

    public void testCellReuse(Predicate<InMemoryTrie.NodeFeatures<Object>> forceCopyPredicate) throws Exception
    {
        ByteComparable[] src = generateKeys(rand, COUNT);
        InMemoryDTrie<Object> trieLong = makeInMemoryDTrie(src,
                                                           opOrder -> InMemoryDTrie.longLived(BufferType.ON_HEAP, opOrder),
                                                           forceCopyPredicate);

        // dump some information first
        System.out.println(String.format(" LongLived ON_HEAP sizes %10s %10s count %d",
                                         FBUtilities.prettyPrintMemory(trieLong.sizeOnHeap()),
                                         FBUtilities.prettyPrintMemory(trieLong.sizeOffHeap()),
                                         Streams.stream(trieLong.values()).count()));

        Pair<BitSet, BitSet> longReachable = reachableCells(trieLong);
        BitSet reachable = longReachable.left;
        int lrcells = reachable.cardinality();
        int lrobjs = longReachable.right.cardinality();
        System.out.println(String.format(" LongLived reachable cells %,d objs %,d cell space %,d obj space %,d",
                                         lrcells,
                                         lrobjs,
                                         lrcells * 32,
                                         lrobjs * 4
        ));

        IntArrayList availableList = ((MemtableAllocationStrategy.OpOrderReuseStrategy) trieLong.allocator).cells.allAvailable();
        BitSet available = new BitSet(reachable.size());
        for (int v : availableList)
            available.set(v >> 5);

        // Check no reachable cell is marked for reuse
        BitSet intersection = new BitSet(available.size());
        intersection.or(available);
        intersection.and(reachable);
        assertCellSetEmpty(intersection, trieLong, " reachable cells marked as available");

        // Check all unreachable cells are marked for reuse
        BitSet unreachable = new BitSet(reachable.size());
        unreachable.or(reachable);
        unreachable.flip(0, ((MemtableAllocationStrategy.OpOrderReuseStrategy) trieLong.allocator).allocatedPos >> 5);
        unreachable.andNot(available);
        assertCellSetEmpty(unreachable, trieLong, " unreachable cells not marked as available");
    }

    static class TestException extends RuntimeException
    {
    }

    @Test
    public void testAbortedMutation() throws Exception
    {
        ByteComparable[] src = generateKeys(rand, COUNT);
        OpOrder order = new OpOrder();
        InMemoryDTrie<Object> trie = InMemoryDTrie.longLived(order);
        InMemoryDTrie<Object> check = InMemoryDTrie.shortLived();
        int step = Math.min(100, COUNT / 100);
        int throwStep = (COUNT + 10) / 5;   // do 4 throwing inserts
        int nextThrow = throwStep;

        for (int i = 0; i < src.length; i += step)
            try (OpOrder.Group g = order.start())
            {
                int last = Math.min(i + step, src.length);
                addToInMemoryDTrie(Arrays.copyOfRange(src, i, last), trie, FORCE_COPY_PARTITION);
                addToInMemoryDTrie(Arrays.copyOfRange(src, i, last), check, NO_ATOMICITY);
                if (i >= nextThrow)
                {
                    nextThrow += throwStep;
                    try
                    {
                        addThrowingEntry(src[rand.nextBoolean() ? last : i],    // try both inserting new value and
                                                                                // overwriting existing
                                         trie, FORCE_COPY_PARTITION);
                        ++i;
                        Assert.fail("Expected failed mutation");
                    }
                    catch (TestException e)
                    {
                        // expected
                    }
                }
            }

        assertMapEquals(trie.filteredEntrySet(ByteBuffer.class).iterator(),
                        check.filteredEntrySet(ByteBuffer.class).iterator());
    }

    private void assertCellSetEmpty(BitSet set, InMemoryDTrie<?> trie, String message)
    {
        if (set.isEmpty())
            return;

        for (int i = set.nextSetBit(0); i >= 0; i = set.nextSetBit(i + 1))
        {
            System.out.println(String.format("Cell at %d: %08x %08x %08x %08x %08x %08x %08x %08x",
                                             (i << 5),
                                             trie.getInt((i << 5) + 0),
                                             trie.getInt((i << 5) + 1),
                                             trie.getInt((i << 5) + 2),
                                             trie.getInt((i << 5) + 3),
                                             trie.getInt((i << 5) + 4),
                                             trie.getInt((i << 5) + 5),
                                             trie.getInt((i << 5) + 6),
                                             trie.getInt((i << 5) + 7)
            ));

        }
        Assert.fail(set.cardinality() + message);
    }

    private Pair<BitSet, BitSet> reachableCells(InMemoryDTrie<?> trie)
    {
        BitSet set = new BitSet();
        BitSet objs = new BitSet();
        mark(trie, trie.root, set, objs);
        return Pair.create(set, objs);
    }

    private void mark(InMemoryTrie<?> trie, int node, BitSet set, BitSet objs)
    {
        set.set(node >> 5);
        switch (trie.offset(node))
        {
            case InMemoryTrie.SPLIT_OFFSET:
                for (int i = 0; i < InMemoryTrie.SPLIT_START_LEVEL_LIMIT; ++i)
                {
                    int mid = trie.getSplitBlockPointer(node, i, InMemoryTrie.SPLIT_START_LEVEL_LIMIT);
                    if (mid != InMemoryTrie.NONE)
                    {
                        set.set(mid >> 5);
                        for (int j = 0; j < InMemoryTrie.SPLIT_OTHER_LEVEL_LIMIT; ++j)
                        {
                            int tail = trie.getSplitBlockPointer(mid, j, InMemoryTrie.SPLIT_OTHER_LEVEL_LIMIT);
                            if (tail != InMemoryTrie.NONE)
                            {
                                set.set(tail >> 5);
                                for (int k = 0; k < InMemoryTrie.SPLIT_OTHER_LEVEL_LIMIT; ++k)
                                    markChild(trie, trie.getSplitBlockPointer(tail, k, InMemoryTrie.SPLIT_OTHER_LEVEL_LIMIT), set, objs);
                            }
                        }
                    }
                }
                break;
            case InMemoryTrie.SPARSE_OFFSET:
                for (int i = 0; i < InMemoryTrie.SPARSE_CHILD_COUNT; ++i)
                    markChild(trie, trie.getInt(node + InMemoryTrie.SPARSE_CHILDREN_OFFSET + i * 4), set, objs);
                break;
            case InMemoryTrie.PREFIX_OFFSET:
                objs.set(~trie.getInt(node + InMemoryTrie.PREFIX_CONTENT_OFFSET));
                markChild(trie, trie.followPrefixTransition(node), set, objs);
                break;
            default:
                assert trie.offset(node) <= InMemoryTrie.CHAIN_MAX_OFFSET && trie.offset(node) >= InMemoryTrie.CHAIN_MIN_OFFSET;
                markChild(trie, trie.getInt((node & -32) + InMemoryTrie.LAST_POINTER_OFFSET), set, objs);
                break;
        }
    }

    private void markChild(InMemoryTrie<?> trie, int child, BitSet set, BitSet objs)
    {
        if (child == InMemoryTrie.NONE)
            return;
        if (child > 0)
            mark(trie, child, set, objs);
        else
            objs.set(~child);
    }

    static InMemoryDTrie<Object> makeInMemoryDTrie(ByteComparable[] src,
                                                       Function<OpOrder, InMemoryDTrie<Object>> creator,
                                                       Predicate<InMemoryDTrie.NodeFeatures<Object>> forceCopyPredicate)
    throws InMemoryTrie.SpaceExhaustedException
    {
        OpOrder order = new OpOrder();
        InMemoryDTrie<Object> trie = creator.apply(order);
        int step = Math.min(100, COUNT / 100);
        for (int i = 0; i < src.length; i += step)
            try (OpOrder.Group g = order.start())
            {
                addToInMemoryDTrie(Arrays.copyOfRange(src, i, i + step), trie, forceCopyPredicate);
            }

        return trie;
    }

    static void addToInMemoryDTrie(ByteComparable[] src,
                                   InMemoryDTrie<Object> trie,
                                   Predicate<InMemoryDTrie.NodeFeatures<Object>> forceCopyPredicate)
    throws InMemoryTrie.SpaceExhaustedException
    {
        for (ByteComparable b : src)
        {
            // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
            // (so that all sources have the same value).
            int payload = asString(b).hashCode();
            ByteBuffer v = ByteBufferUtil.bytes(payload);
            Trie<Object> update = Trie.singleton(b, v);
            update = InMemoryTrieThreadedTest.withRootMetadata(update, Boolean.TRUE);
            update = update.prefix(source("prefix"));
            applyUpdating(trie, update, forceCopyPredicate);
        }
    }

    static ByteComparable source(String key)
    {
        return ByteComparable.fixedLength(key.getBytes(StandardCharsets.UTF_8));
    }

    static void addThrowingEntry(ByteComparable b,
                                 InMemoryDTrie<Object> trie,
                                 Predicate<InMemoryDTrie.NodeFeatures<Object>> forceCopyPredicate)
    throws InMemoryTrie.SpaceExhaustedException
    {
        int payload = asString(b).hashCode();
        ByteBuffer v = ByteBufferUtil.bytes(payload);
        Trie<Object> update = Trie.singleton(b, v);

        // Create an update with two metadata entries, so that the lower is already a copied node.
        // Abort processing on the lower metadata, where the new branch is not attached yet (so as not to affect the
        // contents).
        update = InMemoryTrieThreadedTest.withRootMetadata(update, Boolean.TRUE);
        update = update.prefix(source("fix"));
        update = InMemoryTrieThreadedTest.withRootMetadata(update, Boolean.TRUE);
        update = update.prefix(source("pre"));

        trie.apply(update,
                   (existing, upd) ->
                   {
                       if (upd instanceof Boolean)
                       {
                           if (upd != null && !((Boolean) upd))
                               throw new TestException();
                           return null;
                       }
                       else
                           return upd;
                   },
                   forceCopyPredicate);
    }

    public static <T> void applyUpdating(InMemoryDTrie<T> trie, Trie<T> mutation,
                                                            final Predicate<InMemoryDTrie.NodeFeatures<T>> needsForcedCopy)
    throws InMemoryTrie.SpaceExhaustedException
    {
        trie.apply(mutation, (x, y) -> y, needsForcedCopy);
    }
}
