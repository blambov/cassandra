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
import java.util.function.Function;

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

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.apache.cassandra.db.tries.TrieUtil.assertMapEquals;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;

public class CellReuseTest
{
    private static final int COUNT = 10000;
    Random rand = new Random(2);

    // TODO: enable when force copy is implemented
//    @Test
//    public void testCellReusePartitionCopying() throws Exception
//    {
//        testCellReuse(FORCE_COPY_PARTITION, 0);
//    }
//
//    @Test
//    public void testCellReuseNoCopying() throws Exception
//    {
//        testCellReuse(InMemoryDTrieThreadedTest.NO_ATOMICITY);
//    }

    @Test
    public void testCellReuseNoCopying() throws Exception
    {
        testCellReuse(NO_ATOMICITY, 0);
    }

    @Test
    public void testCellReuseWithDeletions() throws Exception
    {
        testCellReuse(NO_ATOMICITY, 0.1);
    }

    public void testCellReuse(Predicate<InMemoryTrie.NodeFeatures<Object>> forceCopyPredicate, double deletionProbability) throws Exception
    {
        ByteComparable[] src = generateKeys(rand, COUNT);
        InMemoryDTrie<Object> trieLong = makeInMemoryDTrie(src,
                                                           opOrder -> InMemoryDTrie.longLived(BufferType.ON_HEAP, opOrder),
                                                           forceCopyPredicate,
                                                           deletionProbability);

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
        InMemoryDTrie<ByteBuffer> trie = InMemoryDTrie.longLived(order);
        InMemoryDTrie<ByteBuffer> check = InMemoryDTrie.shortLived();
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
                    catch (ExecutionException e)
                    {
                        Assert.assertTrue("Expected TestException, got " + e.getCause().getMessage(),
                                          e.getCause() instanceof TestException);
                    }
                }
            }

        assertMapEquals(trie.entrySet().iterator(), check.entrySet().iterator());
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
                                             trie.getInt((i << 5) + 4),
                                             trie.getInt((i << 5) + 8),
                                             trie.getInt((i << 5) + 12),
                                             trie.getInt((i << 5) + 16),
                                             trie.getInt((i << 5) + 20),
                                             trie.getInt((i << 5) + 24),
                                             trie.getInt((i << 5) + 28)
            ));

        }
        Assert.fail(set.cardinality() + message);
    }

    private Pair<BitSet, BitSet> reachableCells(InMemoryDTrie<?> trie)
    {
//        System.out.println(trie.dump());
        BitSet set = new BitSet();
        BitSet objs = new BitSet();
        mark(trie, trie.root, set, objs);
        return Pair.create(set, objs);
    }

    private void mark(InMemoryDTrie<?,?> trie, int node, BitSet set, BitSet objs)
    {
        set.set(node >> 5);
//        System.out.println(trie.dumpNode(node));
        switch (trie.offset(node))
        {
            case InMemoryDTrie.SPLIT_OFFSET:
                for (int i = 0; i < 4; ++i)
                {
                    int mid = trie.getInt(node + InMemoryDTrie.SPLIT_POINTER_OFFSET + i * 4);
                    if (mid != InMemoryDTrie.NONE)
                    {
//                        System.out.println(trie.dumpNode(mid));
                        set.set(mid >> 5);
                        for (int j = 0; j < 8; ++j)
                        {
                            int tail = trie.getInt(mid + j * 4);
                            if (tail != InMemoryDTrie.NONE)
                            {
//                                System.out.println(trie.dumpNode(tail));
                                set.set(tail >> 5);
                                for (int k = 0; k < 8; ++k)
                                    markChild(trie, trie.getInt(tail + k * 4), set, objs);
                            }
                        }
                    }
                }
                break;
            case InMemoryDTrie.SPARSE_OFFSET:
                for (int i = 0; i < InMemoryDTrie.SPARSE_CHILD_COUNT; ++i)
                    markChild(trie, trie.getInt(node + InMemoryDTrie.SPARSE_CHILDREN_OFFSET + i * 4), set, objs);
                break;
            case InMemoryTrie.PREFIX_OFFSET:
                int content = trie.getInt(node + InMemoryTrie.PREFIX_CONTENT_OFFSET);
                if (content < 0)
                    objs.set(~content);
                else
                    markChild(trie, content, set, objs);

                markChild(trie, trie.getPrefixChild(node, trie.getUnsignedByte(node + InMemoryReadTrie.PREFIX_FLAGS_OFFSET)), set, objs);
                break;
            default:
                assert trie.offset(node) <= InMemoryDTrie.MULTI_MAX_OFFSET && trie.offset(node) >= InMemoryDTrie.MULTI_MIN_OFFSET;
                markChild(trie, trie.getInt((node & -32) + InMemoryDTrie.LAST_POINTER_OFFSET), set, objs);
                break;
        }
    }

    private void markChild(InMemoryDTrie<?,?> trie, int child, BitSet set, BitSet objs)
    {
        if (child == InMemoryDTrie.NONE)
            return;
        if (child > 0)
            mark(trie, child, set, objs);
        else
            objs.set(~child);
    }

    InMemoryDTrie<ByteBuffer> makeInMemoryDTrie(ByteComparable[] src,
                                            Function<OpOrder, InMemoryDTrie<ByteBuffer>> creator,
                                            Predicate<InMemoryDTrie.NodeFeatures<Boolean>> forceCopyPredicate,
                                            double deletionProbability)
    throws TrieSpaceExhaustedException
    {
        OpOrder order = new OpOrder();
        InMemoryDTrie<ByteBuffer> trie = creator.apply(order);
        int step = Math.max(Math.min(100, COUNT / 100), 1);
        for (int i = 0; i < src.length; i += step)
            try (OpOrder.Group g = order.start())
            {
                if (i > 0 && rand.nextDouble() < deletionProbability)
                    trie.delete(randomRange(src, i));
                else
                    addToInMemoryDTrie(Arrays.copyOfRange(src, i, i + step), trie, forceCopyPredicate);
            }

        return trie;
    }

    private TrieSet randomRange(ByteComparable[] src, int i)
    {
        ByteComparable c1 = src[rand.nextInt(i)];
        ByteComparable c2 = src[rand.nextInt(i)];
        if (ByteComparable.compare(c1, c2, VERSION) > 0)
        {
            ByteComparable t = c1; c1 = c2; c2 = t;
        }
        return TrieSet.range(prefixed(c1), prefixed(c2));
    }

    static ByteComparable prefixed(ByteComparable c)
    {
        byte[] v = c.asArray(VERSION);
        byte[] prefix = source("prefix").asArray(VERSION);
        byte[] combined = Arrays.copyOf(prefix, prefix.length + v.length);
        System.arraycopy(v, 0, combined, prefix.length, v.length);
        return ByteComparable.fixedLength(combined);
    }

    static void addToInMemoryDTrie(ByteComparable[] src,
                                   InMemoryDTrie<Object> trie,
                                   Predicate<InMemoryDTrie.NodeFeatures<Object>> forceCopyPredicate)
    throws TrieSpaceExhaustedException
    {
        for (ByteComparable b : src)
        {
            // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
            // (so that all sources have the same value).
            int payload = asString(b).hashCode();
            ByteBuffer v = ByteBufferUtil.bytes(payload);
            Trie<ByteBuffer> update = Trie.singleton(b, v);
            update = InMemoryTrieThreadedTest.withRootMetadata(update, Boolean.TRUE);
            update = update.prefix(source("prefix"));
            applyUpdating(trie, update, forceCopyPredicate).get();
        }
    }

    static ByteComparable source(String key)
    {
        return ByteComparable.fixedLength(key.getBytes(StandardCharsets.UTF_8));
    }

    static void addThrowingEntry(ByteComparable b,
                                 InMemoryDTrie<Object> trie,
                                 Predicate<InMemoryDTrie.NodeFeatures<Object>> forceCopyPredicate)
    throws TrieSpaceExhaustedException
    {
        int payload = asString(b).hashCode();
        ByteBuffer v = ByteBufferUtil.bytes(payload);
        Trie<ByteBuffer> update = Trie.singleton(b, v);

        // Create an update with two metadata entries, so that the lower is already a copied node.
        // Abort processing on the lower metadata, where the new branch is not attached yet (so as not to affect the
        // contents).
        update = InMemoryTrieThreadedTest.withRootMetadata(update, Boolean.FALSE);
        update = update.prefix(source("fix"));
        update = InMemoryTrieThreadedTest.withRootMetadata(update, Boolean.TRUE);
        update = update.prefix(source("pre"));

        trie.apply(update,
                   new InMemoryDTrie.UpsertTransformer<ByteBuffer, Boolean, ByteBuffer, Boolean>()
                   {
                       public ByteBuffer applyContent(ByteBuffer existing, ByteBuffer update, InMemoryDTrie.KeyState<Boolean> keyState)
                       {
                           return update;
                       }

                       public Boolean applyMetadata(Boolean existing, Boolean update, InMemoryDTrie.KeyState<Boolean> keyState)
                       {
                           if (update != null && !update)
                               throw new TestException();
                           return null;
                       }
                   },
                   forceCopyPredicate)
            .get();
    }

    public static <T> void applyUpdating(InMemoryDTrie<T> trie, Trie<T> mutation,
                                                            final Predicate<InMemoryDTrie.NodeFeatures<T>> needsForcedCopy)
    throws TrieSpaceExhaustedException
    {
        return trie.apply(mutation,
                          new InMemoryDTrie.UpsertTransformer<T, M, T, M>()
                          {
                              public T applyContent(T existing, T update, InMemoryDTrie.KeyState<M> keyState)
                              {
                                  return update;
                              }

                              public M applyMetadata(M existing, M update, InMemoryDTrie.KeyState<M> keyState)
                              {
                                  return update;
                              }
                          },
                          needsForcedCopy);
    }
}
