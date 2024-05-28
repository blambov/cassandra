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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.WrappedInt;
import org.apache.cassandra.utils.WrappedLong;
import org.apache.cassandra.utils.concurrent.OpOrder;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;

public class InMemoryTrieThreadedTest
{
    private static final int COUNT = 300000;
    private static final int OTHERS = COUNT / 10;
    private static final int PROGRESS_UPDATE = COUNT / 15;
    private static final int READERS = 8;
    private static final int WALKERS = 2;

    static String value(ByteComparable b)
    {
        return b.byteComparableAsString(VERSION);
    }

    @Test
    public void testThreaded() throws InterruptedException
    {
        Random rand = new Random(1);
        OpOrder readOrder = new OpOrder();
        ByteComparable[] src = generateKeys(rand, COUNT + OTHERS);
        InMemoryDTrie<String> trie = InMemoryDTrie.longLived(readOrder);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<>();
        AtomicBoolean writeCompleted = new AtomicBoolean(false);
        AtomicInteger writeProgress = new AtomicInteger(0);

        for (int i = 0; i < WALKERS; ++i)
            threads.add(new Thread(() -> {
                try
                {
                    while (!writeCompleted.get())
                    {
                        int min = writeProgress.get();
                        int count = 0;
                        try (OpOrder.Group group = readOrder.start())
                        {
                            for (Map.Entry<ByteComparable, String> en : trie.entrySet())
                            {
                                String v = value(en.getKey());
                                Assert.assertEquals(en.getKey().byteComparableAsString(VERSION), v, en.getValue());
                                ++count;
                            }
                        }
                        Assert.assertTrue("Got only " + count + " while progress is at " + min, count >= min);
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    errors.add(t);
                }
            }));

        for (int i = 0; i < READERS; ++i)
        {
            threads.add(new Thread(() -> {
                try
                {
                    Random r = ThreadLocalRandom.current();
                    while (!writeCompleted.get())
                    {
                        int min = writeProgress.get();

                        for (int i1 = 0; i1 < PROGRESS_UPDATE; ++i1)
                        {
                            int index = r.nextInt(COUNT + OTHERS);
                            ByteComparable b = src[index];
                            String v = value(b);
                            try (OpOrder.Group group = readOrder.start())
                            {
                                String result = trie.get(b);
                                if (result != null)
                                {
                                    Assert.assertTrue("Got not added " + index + " when COUNT is " + COUNT,
                                                      index < COUNT);
                                    Assert.assertEquals("Failed " + index, v, result);
                                }
                                else if (index < min)
                                    Assert.fail("Failed index " + index + " while progress is at " + min);
                            }
                        }
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    errors.add(t);
                }
            }));
        }

        threads.add(new Thread(() -> {
            try
            {
                for (int i = 0; i < COUNT; i++)
                {
                    ByteComparable b = src[i];

                    // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
                    // (so that all sources have the same value).
                    String v = value(b);
                    if (i % 2 == 0)
                        trie.apply(Trie.singleton(b, v), (x, y) -> y);
                    else
                        trie.putRecursive(b, v, (x, y) -> y);

                    if (i % PROGRESS_UPDATE == 0)
                        writeProgress.set(i);
                }
            }
            catch (Throwable t)
            {
                t.printStackTrace();
                errors.add(t);
            }
            finally
            {
                writeCompleted.set(true);
            }
        }));

        for (Thread t : threads)
            t.start();

        for (Thread t : threads)
            t.join();

        if (!errors.isEmpty())
            Assert.fail("Got errors:\n" + errors);
    }


    static class Value
    {
        final String pk;
        final String ck;
        final int value;
        final int seq;

        Value(String pk, String ck, int value, int seq)
        {
            this.pk = pk;
            this.ck = ck;
            this.value = value;
            this.seq = seq;
        }

        @Override
        public String toString()
        {
            return "Value{" +
                   "pk='" + pk + '\'' +
                   ", ck='" + ck + '\'' +
                   ", value=" + value +
                   ", seq=" + seq +
                   '}';
        }
    }

    @Test
    public void testConsistentUpdates() throws Exception
    {
        // Check that multi-path updates with below-partition-level copying are safe for concurrent readers,
        // and that content is atomically applied, i.e. that reader see either nothing from the update or all of it,
        // and consistent, i.e. that it is not possible to receive some newer updates while missing
        // older ones. (For example, if the sequence of additions is 3, 1, 5, without this requirement a reader
        // could see an enumeration which lists 3 and 5 but not 1.)
        testAtomicUpdates(5, FORCE_COPY_PARTITION, true, true);
    }

    @Test
    public void testAtomicUpdates() throws Exception
    {
        // Check that multi-path updates with below-branching-point copying are safe for concurrent readers,
        // and that content is atomically applied, i.e. that reader see either nothing from the update or all of it.
        testAtomicUpdates(5, FORCE_ATOMIC,true, false);
    }

    @Test
    public void testSafeUpdates() throws Exception
    {
        // Check that multi path updates without additional copying are safe for concurrent readers.
        testAtomicUpdates(5, NO_ATOMICITY, false, false);
    }

    @Test
    public void testConsistentSinglePathUpdates() throws Exception
    {
        // Check that single path updates with below-partition-level copying are safe for concurrent readers,
        // and that content is consistent, i.e. that it is not possible to receive some newer updates while missing
        // older ones. (For example, if the sequence of additions is 3, 1, 5, without this requirement a reader
        // could see an enumeration which lists 3 and 5 but not 1.)
        testAtomicUpdates(1, FORCE_COPY_PARTITION, true, true);
    }


    @Test
    public void testAtomicSinglePathUpdates() throws Exception
    {
        // When doing single path updates atomicity comes for free. This only checks that the branching checker is
        // not doing anything funny.
        testAtomicUpdates(1, FORCE_ATOMIC, true, false);
    }

    @Test
    public void testSafeSinglePathUpdates() throws Exception
    {
        // Check that single path updates without additional copying are safe for concurrent readers.
        testAtomicUpdates(1, NO_ATOMICITY, true, false);
    }

    public void testAtomicUpdates(int PER_MUTATION,
                                  Predicate<MemtableTrie.NodeFeatures<Boolean>> forcedCopyChecker,
                                  boolean checkAtomicity,
                                  boolean checkSequence)
    throws Exception
    {
        Random rand = new Random(1);
        ByteComparable[] ckeys = generateKeys(rand, COUNT);
        ByteComparable[] pkeys = generateKeys(rand, Math.min(100, COUNT / 10));  // to guarantee repetition

        /**
         * Adds COUNT partitions each with perPartition separate clusterings, where the sum of the values
         * of all clusterings is 0.
         * If the sum for any walk covering whole partitions is non-zero, we have had non-atomic updates.
         */

        OpOrder readOrder = new OpOrderSimple();
//        MemtableTrie<Value, Boolean> trie = new MemtableTrie<>(new MemtableAllocationStrategy.NoReuseStrategy(BufferType.OFF_HEAP));
        MemtableTrie<Value, Boolean> trie = MemtableTrie.longLived(readOrder);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<Thread>();
        AtomicBoolean writeCompleted = new AtomicBoolean(false);
        AtomicInteger writeProgress = new AtomicInteger(0);

        for (int i = 0; i < WALKERS; ++i)
            threads.add(new Thread()
            {
                public void run()
                {
                    try
                    {
                        Random r = ThreadLocalRandom.current();
                        while (!writeCompleted.get())
                        {
                            int min = writeProgress.get();
                            try (OpOrder.Group group = readOrder.start())
                            {
                                Flow<Map.Entry<ByteComparable, Value>> entries = trie.entrySet();
                                checkEntries("", min, true, checkAtomicity, false, PER_MUTATION, entries);
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace();
                        errors.add(t);
                    }
                }
            });

        for (int i = 0; i < READERS; ++i)
        {
            ByteComparable[] srcLocal = pkeys;
            threads.add(new Thread()
            {
                public void run()
                {
                    try
                    {
                        // await at least one ready partition
                        while (writeProgress.get() == 0) {}

                        Random r = ThreadLocalRandom.current();
                        while (!writeCompleted.get())
                        {
                            ByteComparable key = srcLocal[r.nextInt(srcLocal.length)];
                            int min = writeProgress.get() / (pkeys.length * PER_MUTATION) * PER_MUTATION;
                            Flow<Map.Entry<ByteComparable, Value>> entries;

                            try (OpOrder.Group group = readOrder.start())
                            {
                                entries = trie.tailTrie(key).entrySet();
                                checkEntries(" in branch " + key.byteComparableAsString(VERSION), min, false, checkAtomicity, checkSequence, PER_MUTATION, entries);
                            }

                            try (OpOrder.Group group = readOrder.start())
                            {
                                entries = trie.branch(key).entrySet();
                                checkEntries(" in branch " + key.byteComparableAsString(VERSION), min, true, checkAtomicity, checkSequence, PER_MUTATION, entries);
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace();
                        errors.add(t);
                    }
                }
            });
        }

        threads.add(new Thread()
        {
            public void run()
            {
                ThreadLocalRandom r = ThreadLocalRandom.current();
                final Trie.CollectionMergeResolver<Value, Boolean> mergeResolver = new Trie.CollectionMergeResolver<Value, Boolean>()
                {
                    public Value resolve(Collection<Value> contents)
                    {
                        throw new AssertionError("Test error, keys should be distinct.");
                    }

                    public Boolean resolveMetadata(Collection<Boolean> metadata)
                    {
                        return Boolean.TRUE;
                    }
                };

                try
                {
                    int lastUpdate = 0;
                    for (int i = 0; i < COUNT; i += PER_MUTATION)
                    {
                        ByteComparable b = pkeys[(i / PER_MUTATION) % pkeys.length];
                        ByteComparable cprefix = null;
                        if (r.nextBoolean())
                            cprefix = ckeys[i]; // Move the branching point below the partition level, otherwise Atomic
                        // becomes the same as Consistent

                        List<Trie<Value, Boolean>> sources = new ArrayList<>();
                        for (int j = 0; j < PER_MUTATION; ++j)
                        {

                            ByteComparable k = ckeys[i + j];
                            Trie<Value, Boolean> row = Trie.singleton(k, value(b, cprefix, k,
                                                                               j == 0 ? -PER_MUTATION + 1 : 1,
                                                                               (i / PER_MUTATION / pkeys.length) * PER_MUTATION + j));

                            if (cprefix != null)
                                row = Trie.prefix(cprefix, row);

                            row = withRootMetadata(row, Boolean.TRUE);
                            row = Trie.prefix(b, row);
                            sources.add(row);
                        }

                        final Trie<Value, Boolean> mutation = Trie.merge(sources,
                                                                         mergeResolver);

                        MemtableTrieTest.applySimpleResolve(trie, mutation, mergeResolver, forcedCopyChecker)
                                        .get();

                        if (i >= pkeys.length * PER_MUTATION && i - lastUpdate >= PROGRESS_UPDATE)
                        {
                            writeProgress.set(i);
                            lastUpdate = i;
                        }
                    }
                }
                catch (Throwable t)
                {
                    t.printStackTrace();
                    errors.add(t);
                }
                finally
                {
                    writeCompleted.set(true);
                }
            }
        });

        for (Thread t : threads)
            t.start();

        for (Thread t : threads)
            t.join();

        System.out.format("Reuse %s %s atomicity %s on-heap %,d (+%,d) off-heap %,d (+%,d)\n",
                          trie.allocator.getClass().getSimpleName(),
                          ((MemtableAllocationStrategy.NoReuseStrategy) (trie.allocator)).bufferType,
                          forcedCopyChecker == NO_ATOMICITY ? "none" :
                          forcedCopyChecker == FORCE_ATOMIC ? "atomic" : "consistent partition",
                          trie.sizeOnHeap(),
                          ((MemtableAllocationStrategy.NoReuseStrategy)trie.allocator).availableForAllocationOnHeap(),
                          trie.sizeOffHeap(),
                          ((MemtableAllocationStrategy.NoReuseStrategy)trie.allocator).availableForAllocationOffHeap());

        if (!errors.isEmpty())
            Assert.fail("Got errors:\n" + errors);
    }

    static <T, M> Trie<T, M> withRootMetadata(Trie<T, M> wrapped, M metadata)
    {
        return new Trie<T, M>()
        {
            protected <L> void requestRoot(Subscriber<T, M, L> subscriber, L parent)
            {
                Subscriber<T, M, L> ourSubscriber = new Subscriber<T, M, L>() {
                    public void onNode(Node<T, M, L> node)
                    {
                        if (node.parentLink == parent) // root
                        {
                            subscriber.onNode(new WrappingNode<T, M, L>(node)
                            {
                                public M metadata()
                                {
                                    return metadata;
                                }
                            });
                        }
                        else
                            subscriber.onNode(node);
                    }

                    public void onError(Throwable t)
                    {
                        subscriber.onError(t);
                    }
                };
                wrapped.requestRoot(ourSubscriber, parent);
            }
        };
    }

    public void checkEntries(String location,
                             int min,
                             boolean usePk,
                             boolean checkAtomicity,
                             boolean checkConsecutiveIds,
                             int PER_MUTATION,
                             Flow<Map.Entry<ByteComparable, Value>> entries) throws Exception
    {
        WrappedLong sum = new WrappedLong(0);
        WrappedInt count = new WrappedInt(0);
        WrappedLong idSum = new WrappedLong(0);
        WrappedLong idMax = new WrappedLong(0);
        entries.process(en ->
                        {
                            String v = (usePk ? en.getValue().pk : "") + en.getValue().ck;
                            String expected = en.getKey().byteComparableAsString(VERSION);
                            Assert.assertEquals(location, expected, v);
                            count.increment();
                            sum.add(en.getValue().value);
                            int seq = en.getValue().seq;
                            idSum.add(seq);
                            if (seq > idMax.get())
                                idMax.set(seq);
                        })
               .executeBlocking();

        Assert.assertTrue("Values" + location + " should be at least " + min + ", got " + count.get(), min <= count.get());

        if (checkAtomicity)
        {
            // If mutations apply atomically, the row count is always a multiple of the mutation size...
            Assert.assertTrue("Values" + location + " should be a multiple of " + PER_MUTATION + ", got " + count.get(), count.get() % PER_MUTATION == 0);
            // ... and the sum of the values is 0 (as the sum for each individual mutation is 0).
            Assert.assertEquals("Value sum" + location, 0, sum.get());
        }

        if (checkConsecutiveIds)
        {
            // If mutations apply consistently for the partition, for any row we see we have to have seen all rows that
            // were applied before that. In other words, the id sum should be the sum of the integers from 1 to the
            // highest id seen in the partition.
            Assert.assertEquals("Id sum" + location, idMax.get() * (idMax.get() + 1) / 2, idSum.get());
        }
    }
}
