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
import java.util.Collection;
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

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;

public class InMemoryTrieThreadedTest
{
    private static final int COUNT = 3000;
    private static final int OTHERS = COUNT / 10;
    private static final int PROGRESS_UPDATE = COUNT / 15;
    private static final int READERS = 8;
    private static final int WALKERS = 2;
    private static final Random rand = new Random();

    static Value value(ByteComparable b, ByteComparable cprefix, ByteComparable c, int add, int seqId)
    {
        return new Value(b.byteComparableAsString(VERSION),
                         (cprefix != null ? cprefix.byteComparableAsString(VERSION) : "") + c.byteComparableAsString(VERSION), add, seqId);
    }

    static String value(ByteComparable b)
    {
        return b.byteComparableAsString(VERSION);
    }

    @Test
    public void testThreaded() throws InterruptedException
    {
        ByteComparable[] src = generateKeys(rand, COUNT + OTHERS);
        InMemoryDTrie<String> trie = new InMemoryDTrie<>(BufferType.ON_HEAP);
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
                        for (Map.Entry<ByteComparable, String> en : trie.entrySet())
                        {
                            String v = value(en.getKey());
                            Assert.assertEquals(en.getKey().byteComparableAsString(VERSION), v, en.getValue());
                            ++count;
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

    // Note to reviewers: this set of tests will be expanded and make better sense with the next commit

    static class Content
    {}

    static class Value extends Content
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

    static class Metadata extends Content
    {
        final boolean isPartition;

        Metadata(boolean isPartition)
        {
            this.isPartition = isPartition;
        }

        static final Metadata TRUE = new Metadata(true);
    }

    @Test
    public void testSafeUpdates() throws Exception
    {
        // Check that multi path updates are safe for concurrent readers.
        testAtomicUpdates(5);
    }

    @Test
    public void testSafeSinglePathUpdates() throws Exception
    {
        // Check that single path updates are safe for concurrent readers.
        testAtomicUpdates(1);
    }

    public void testAtomicUpdates(int PER_MUTATION) throws Exception
    {
        ByteComparable[] ckeys = generateKeys(rand, COUNT);
        ByteComparable[] pkeys = generateKeys(rand, Math.min(100, COUNT / 10));  // to guarantee repetition

        /**
         * Adds COUNT partitions each with perPartition separate clusterings, where the sum of the values
         * of all clusterings is 0.
         * If the sum for any walk covering whole partitions is non-zero, we have had non-atomic updates.
         */

        InMemoryDTrie<Content> trie = new InMemoryDTrie<>(BufferType.ON_HEAP);
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
                            Iterable<Map.Entry<ByteComparable, Content>> entries = trie.entrySet();
                            checkEntries("", min, true, entries);
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
                            Iterable<Map.Entry<ByteComparable, Content>> entries;

                            entries = trie.tailTrie(key).entrySet();
                            checkEntries(" in tail " + key.byteComparableAsString(VERSION), min, false, entries);

                            entries = trie.subtrie(key, key).entrySet();
                            checkEntries(" in branch " + key.byteComparableAsString(VERSION), min, true, entries);
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
                final Trie.CollectionMergeResolver<Content> mergeResolver = new Trie.CollectionMergeResolver<Content>()
                {
                    @Override
                    public Content resolve(Content c1, Content c2)
                    {
                        if (c1 == c2 && c1 instanceof Metadata)
                            return c1;
                        throw new AssertionError("Test error, keys should be distinct.");
                    }

                    public Content resolve(Collection<Content> contents)
                    {
                        return contents.stream().reduce(this::resolve).get();
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
                            cprefix = ckeys[i]; // Also test branching point below the partition level

                        List<Trie<Content>> sources = new ArrayList<>();
                        for (int j = 0; j < PER_MUTATION; ++j)
                        {

                            ByteComparable k = ckeys[i + j];
                            Trie<Content> row = Trie.singleton(k, value(b, cprefix, k,
                                                                        j == 0 ? -PER_MUTATION + 1 : 1,
                                                                        (i / PER_MUTATION / pkeys.length) * PER_MUTATION + j));

                            if (cprefix != null)
                                row = row.prefix(cprefix);

                            row = withRootMetadata(row, Metadata.TRUE);
                            row = row.prefix(b);
                            sources.add(row);
                        }

                        final Trie<Content> mutation = Trie.merge(sources, mergeResolver);

                        trie.apply(mutation, (existing, update) -> existing == null ? update : mergeResolver.resolve(existing, update));

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

        if (!errors.isEmpty())
            Assert.fail("Got errors:\n" + errors);
    }

    static <T> Trie<T> withRootMetadata(Trie<T> wrapped, T metadata)
    {
        return wrapped.mergeWith(Trie.singleton(ByteComparable.EMPTY, metadata), Trie.throwingResolver());
    }

    public void checkEntries(String location,
                             int min,
                             boolean usePk,
                             Iterable<Map.Entry<ByteComparable, Content>> entries) throws Exception
    {
        long sum = 0;
        int count = 0;
        long idSum = 0;
        long idMax = 0;
        for (var en : entries)
        {
            if (!(en.getValue() instanceof Value))
                continue;
            final Value value = (Value) en.getValue();
            String valueKey = (usePk ? value.pk : "") + value.ck;
            String path = en.getKey().byteComparableAsString(VERSION);
            Assert.assertEquals(location, valueKey, path);
            ++count;
            sum += value.value;
            int seq = value.seq;
            idSum += seq;
            if (seq > idMax)
                idMax = seq;
        }

        Assert.assertTrue("Values" + location + " should be at least " + min + ", got " + count, min <= count);
    }
}
