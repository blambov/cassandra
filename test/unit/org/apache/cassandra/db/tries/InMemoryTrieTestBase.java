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
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public abstract class InMemoryTrieTestBase
{
    Random rand = new Random();

    abstract boolean usePut();

    @Test
    public void testSingle()
    {
        ByteComparable e = ByteComparable.of("test");
        InMemoryDTrie<String> trie = new InMemoryDTrie<>(strategy.create());
        putSimpleResolve(trie, e, "test", (x, y) -> y);
        System.out.println("Trie " + trie.dump());
        assertEquals("test", trie.get(e));
        assertEquals(null, trie.get(ByteComparable.of("teste")));
    }

    public enum ReuseStrategy
    {
        SHORT_LIVED
        {
            MemtableAllocationStrategy create()
            {
                return InMemoryTrie.shortLivedStrategy();
            }
        },
        LONG_LIVED
        {
            MemtableAllocationStrategy create()
            {
                return InMemoryTrie.longLivedStrategy(null);
            }
        };

        abstract MemtableAllocationStrategy create();
    }

    @Parameterized.Parameters()
    public static Object[] generateData()
    {
        return ReuseStrategy.values();
    }


    @Parameterized.Parameter(0)
    public static ReuseStrategy strategy = ReuseStrategy.LONG_LIVED;

    @Test
    public void testSplitMulti()
    {
        testEntries(new String[] { "testing", "tests", "trials", "trial", "aaaa", "aaaab", "abdddd", "abeeee" });
    }

    @Test
    public void testSplitMultiBug()
    {
        testEntriesHex(new String[] { "0c4143aeff", "0c4143ae69ff" });
    }


    @Test
    public void testSparse00bug()
    {
        String[] tests = new String[] {
        "40bd256e6fd2adafc44033303000",
        "40bdd47ec043641f2b403131323400",
        "40bd00bf5ae8cf9d1d403133323800",
        };
        InMemoryDTrie<String> trie = new InMemoryDTrie<>(strategy.create());
        for (String test : tests)
        {
            ByteComparable e = ByteComparable.fixedLength(ByteBufferUtil.hexToBytes(test));
            System.out.println("Adding " + TrieUtil.asString(e) + ": " + test);
            putSimpleResolve(trie, e, test, (x, y) -> y);
        }

        System.out.println(trie.dump());

        for (String test : tests)
            assertEquals(test, trie.get(ByteComparable.fixedLength(ByteBufferUtil.hexToBytes(test))));

        Arrays.sort(tests);

        int idx = 0;
        for (String s : trie.values())
        {
            if (s != tests[idx])
                throw new AssertionError("" + s + "!=" + tests[idx]);
            ++idx;
        }
        assertEquals(tests.length, idx);
    }

    @Test
    public void testUpdateContent()
    {
        String[] tests = new String[] {"testing", "tests", "trials", "trial", "testing", "trial", "trial"};
        String[] values = new String[] {"testing", "tests", "trials", "trial", "t2", "x2", "y2"};
        InMemoryDTrie<String> trie = new InMemoryDTrie<>(strategy.create());
        for (int i = 0; i < tests.length; ++i)
        {
            String test = tests[i];
            String v = values[i];
            ByteComparable e = ByteComparable.of(test);
            System.out.println("Adding " + TrieUtil.asString(e) + ": " + v);
            putSimpleResolve(trie, e, v, (x, y) -> "" + x + y);
            System.out.println("Trie " + trie.dump());
        }

        for (int i = 0; i < tests.length; ++i)
        {
            String test = tests[i];
            assertEquals(Stream.iterate(0, x -> x + 1)
                               .limit(tests.length)
                               .filter(x -> tests[x] == test)
                               .map(x -> values[x])
                               .reduce("", (x, y) -> "" + x + y),
                         trie.get(ByteComparable.of(test)));
        }
    }

    @Test
    public void testEntriesNullChildBug()
    {
        Object[] trieDef = new Object[]
                                   {
                                           new Object[] { // 0
                                                   ByteBufferUtil.bytes(1), // 01
                                                   ByteBufferUtil.bytes(2)  // 02
                                           },
                                           // If requestChild returns null, bad things can happen (DB-2982)
                                           null, // 1
                                           ByteBufferUtil.bytes(3), // 2
                                           new Object[] {  // 3
                                                   ByteBufferUtil.bytes(4), // 30
                                                   // Also try null on the Remaining.ONE path
                                                   null // 31
                                           },
                                           ByteBufferUtil.bytes(5), // 4
                                           // Also test requestUniqueDescendant returning null
                                           new Object[] { // 5
                                                   new Object[] { // 50
                                                           new Object[] { // 500
                                                                   null // 5000
                                                           }
                                                   }
                                           },
                                           ByteBufferUtil.bytes(6) // 6
                                   };

        SortedMap<ByteComparable, ByteBuffer> expected = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);
        expected.put(TrieUtil.comparable("00"), ByteBufferUtil.bytes(1));
        expected.put(TrieUtil.comparable("01"), ByteBufferUtil.bytes(2));
        expected.put(TrieUtil.comparable("2"), ByteBufferUtil.bytes(3));
        expected.put(TrieUtil.comparable("30"), ByteBufferUtil.bytes(4));
        expected.put(TrieUtil.comparable("4"), ByteBufferUtil.bytes(5));
        expected.put(TrieUtil.comparable("6"), ByteBufferUtil.bytes(6));

        Trie<ByteBuffer> trie = TrieUtil.specifiedTrie(trieDef);
        System.out.println(trie.dump(ByteBufferUtil::bytesToHex));
        System.out.println(TrieImpl.impl(trie).process(new TrieDumper<>(ByteBufferUtil::bytesToHex), Direction.REVERSE));
        TrieUtil.assertSameContent(trie, expected);
    }

    @Test
    public void testDirect()
    {
        ByteComparable[] src = TrieUtil.generateKeys(rand, TrieUtil.COUNT);
        SortedMap<ByteComparable, ByteBuffer> content = new TreeMap<>(TrieUtil.FORWARD_COMPARATOR);
        InMemoryDTrie<ByteBuffer> trie = makeInMemoryDTrie(src, content, usePut());
        int keysize = Arrays.stream(src)
                            .mapToInt(src1 -> ByteComparable.length(src1, TrieUtil.VERSION))
                            .sum();
        long ts = ObjectSizes.measureDeep(content);
        long onh = ObjectSizes.measureDeep(trie.contentArray);
        System.out.format("Trie size on heap %,d off heap %,d measured %,d keys %,d treemap %,d\n",
                          trie.sizeOnHeap(), trie.sizeOffHeap(), onh, keysize, ts);
        System.out.format("per entry on heap %.2f off heap %.2f measured %.2f keys %.2f treemap %.2f\n",
                          trie.sizeOnHeap() * 1.0 / TrieUtil.COUNT, trie.sizeOffHeap() * 1.0 / TrieUtil.COUNT, onh * 1.0 / TrieUtil.COUNT, keysize * 1.0 / TrieUtil.COUNT, ts * 1.0 / TrieUtil.COUNT);
        if (TrieUtil.VERBOSE)
            System.out.println("Trie " + trie.dump(ByteBufferUtil::bytesToHex));

        TrieUtil.assertSameContent(trie, content);

        trie.discardBuffers();
    }

    @Test
    public void testPrefixEvolution()
    {
        testEntries(new String[] { "testing",
                                   "test",
                                   "tests",
                                   "tester",
                                   "testers",
                                   // test changing type with prefix
                                   "types",
                                   "types1",
                                   "types",
                                   "types2",
                                   "types3",
                                   "types4",
                                   "types",
                                   "types5",
                                   "types6",
                                   "types7",
                                   "types8",
                                   "types",
                                   // test adding prefix to chain
                                   "chain123",
                                   "chain",
                                   // test adding prefix to sparse
                                   "sparse1",
                                   "sparse2",
                                   "sparse3",
                                   "sparse",
                                   // test adding prefix to split
                                   "split1",
                                   "split2",
                                   "split3",
                                   "split4",
                                   "split5",
                                   "split6",
                                   "split7",
                                   "split8",
                                   "split"
        });
    }

    @Test
    public void testPrefixUnsafeMulti()
    {
        // Make sure prefixes on inside a multi aren't overwritten by embedded metadata node.

        testEntries(new String[] { "test89012345678901234567890",
                                   "test8",
                                   "test89",
                                   "test890",
                                   "test8901",
                                   "test89012",
                                   "test890123",
                                   "test8901234",
                                   });
    }

    private void testEntries(String[] tests)
    {
        for (Function<String, ByteComparable> mapping :
                ImmutableList.<Function<String, ByteComparable>>of(ByteComparable::of,
                                                                   s -> ByteComparable.fixedLength(s.getBytes())))
        {
            testEntries(tests, mapping);
        }
    }

    private void testEntriesHex(String[] tests)
    {
        testEntries(tests, s -> ByteComparable.fixedLength(ByteBufferUtil.hexToBytes(s)));
        // Run the other translations just in case.
        testEntries(tests);
    }

    private void testEntries(String[] tests, Function<String, ByteComparable> mapping)

    {
        InMemoryDTrie<String> trie = new InMemoryDTrie<>(strategy.create());
        for (String test : tests)
        {
            ByteComparable e = mapping.apply(test);
            System.out.println("Adding " + TrieUtil.asString(e) + ": " + test);
            putSimpleResolve(trie, e, test, (x, y) -> y);
            System.out.println("Trie\n" + trie.dump());
        }

        for (String test : tests)
            assertEquals(test, trie.get(mapping.apply(test)));
    }

    static InMemoryDTrie<ByteBuffer> makeInMemoryDTrie(ByteComparable[] src,
                                                     Map<ByteComparable, ByteBuffer> content,
                                                     boolean usePut)

    {
        InMemoryDTrie<ByteBuffer> trie = new InMemoryDTrie<>(strategy.create());
        addToInMemoryDTrie(src, content, trie, usePut);
        return trie;
    }

    static void addToInMemoryDTrie(ByteComparable[] src,
                                  Map<ByteComparable, ? super ByteBuffer> content,
                                  InMemoryDTrie<? super ByteBuffer> trie,
                                  boolean usePut)

    {
        for (ByteComparable b : src)
        {
            // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
            // (so that all sources have the same value).
            int payload = TrieUtil.asString(b).hashCode();
            ByteBuffer v = ByteBufferUtil.bytes(payload);
            content.put(b, v);
            if (TrieUtil.VERBOSE)
                System.out.println("Adding " + TrieUtil.asString(b) + ": " + ByteBufferUtil.bytesToHex(v));
            putSimpleResolve(trie, b, v, (x, y) -> y, usePut);
            if (TrieUtil.VERBOSE)
                System.out.println(trie.dump(bb -> bb instanceof ByteBuffer
                                                   ? ByteBufferUtil.bytesToHex((ByteBuffer) bb)
                                                   : bb.toString()));
        }
    }

    <T> void putSimpleResolve(InMemoryDTrie<T> trie,
                                 ByteComparable key,
                                 T value,
                                 Trie.MergeResolver<T> resolver)
    {
        putSimpleResolve(trie, key, value, resolver, usePut());
    }

    static <T> void putSimpleResolve(InMemoryDTrie<T> trie,
                                     ByteComparable key,
                                     T value,
                                     Trie.MergeResolver<T> resolver,
                                     boolean usePut)
    {
        try
        {
            trie.putSingleton(key,
                              value,
                              (existing, update) -> existing != null ? resolver.resolve(existing, update) : update,
                              usePut);
        }
        catch (TrieSpaceExhaustedException e)
        {
            // Should not happen, test stays well below size limit.
            throw new AssertionError(e);
        }
    }
}
