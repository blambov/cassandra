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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;

public abstract class InMemoryTrieTestBase
{
    // Set this to true (in combination with smaller count) to dump the tries while debugging a problem.
    // Do not commit the code with VERBOSE = true.
    private static final boolean VERBOSE = false;

    private static final int COUNT = 100000;
    private static final int KEY_CHOICE = 25;
    private static final int MIN_LENGTH = 10;
    private static final int MAX_LENGTH = 50;

    Random rand = new Random();

    static final ByteComparable.Version VERSION = InMemoryDTrie.BYTE_COMPARABLE_VERSION;

    abstract boolean usePut();

    @Test
    public void testSingle()
    {
        ByteComparable e = ByteComparable.of("test");
        InMemoryDTrie<String> trie = new InMemoryDTrie<>(BufferType.OFF_HEAP);
        putSimpleResolve(trie, e, "test", (x, y) -> y);
        System.out.println("Trie " + trie.dump());
        assertEquals("test", trie.get(e));
        assertEquals(null, trie.get(ByteComparable.of("teste")));
    }

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
        InMemoryDTrie<String> trie = new InMemoryDTrie<>(BufferType.OFF_HEAP);
        for (String test : tests)
        {
            ByteComparable e = ByteComparable.fixedLength(ByteBufferUtil.hexToBytes(test));
            System.out.println("Adding " + asString(e) + ": " + test);
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
        InMemoryDTrie<String> trie = new InMemoryDTrie<>(BufferType.OFF_HEAP);
        for (int i = 0; i < tests.length; ++i)
        {
            String test = tests[i];
            String v = values[i];
            ByteComparable e = ByteComparable.of(test);
            System.out.println("Adding " + asString(e) + ": " + v);
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

    static class SpecStackEntry
    {
        Object[] children;
        int curChild;
        Object content;
        SpecStackEntry parent;
        Object alternateBranch;

        public SpecStackEntry(Object[] spec, Object content, Object alternateBranch, SpecStackEntry parent)
        {
            this.children = spec;
            this.content = content;
            this.parent = parent;
            this.alternateBranch = alternateBranch;
            this.curChild = -1;
        }
    }
    static SpecStackEntry makeSpecStackEntry(Object spec, SpecStackEntry parent)
    {
        if (spec instanceof Pair)
            return makeSpecStackEntry(((Pair) spec).left(), ((Pair) spec).right(), parent);
        else
            return makeSpecStackEntry(spec, null, parent);
    }
    static SpecStackEntry makeSpecStackEntry(Object spec, Object alternateBranch, SpecStackEntry parent)
    {
        assert !(spec instanceof Pair);
        if (spec instanceof Object[])
            return new SpecStackEntry((Object[]) spec, null, alternateBranch, parent);
        else
            return new SpecStackEntry(new Object[0], spec, alternateBranch, parent);

    }

    public static class CursorFromSpec<T> implements NonDeterministicTrieImpl.Cursor<T>
    {
        SpecStackEntry stack;
        int depth;
        int leadingTransition;

        CursorFromSpec(Object[] spec)
        {
            stack = makeSpecStackEntry(spec, null, null);
            depth = 0;
            leadingTransition = -1;
        }

        CursorFromSpec(SpecStackEntry stack, int depth, int leadingTransition)
        {
            this.stack = stack;
            this.depth = depth;
            this.leadingTransition = leadingTransition;
        }

        @Override
        public int advance()
        {
            Object child;
            SpecStackEntry current = stack;
            do
            {
                while (current != null && ++current.curChild >= current.children.length)
                {
                    current = current.parent;
                    --depth;
                }
                if (current == null)
                {
                    stack = null;
                    leadingTransition = -1;
                    return depth = -1;
                }

                child = current.children[current.curChild];
            }
            while (child == null);
            stack = makeSpecStackEntry(child, current);

            return ++depth;
        }

//        @Override
//        public int advanceMultiple(Trie.TransitionsReceiver receiver)
//        {
//            if (stack == null || stack.curChild + 1 >= stack.children.length)
//                return advance();
//
//            ++stack.curChild;
//            Object child = stack.children[stack.curChild];
//            while (child instanceof Object[])
//            {
//                stack = new SpecStackEntry((Object[]) child, null, null, stack);
//                if (receiver != null)
//                    receiver.addPathByte(0x30);
//                ++depth;
//                if (stack.children.length == 0)
//                    return depth;
//                child = stack.children[++stack.curChild];
//            }
//            stack = makeSpecStackEntry(child, stack);
//
//
//            return ++depth;
//        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            assert skipDepth <= depth + 1 : "skipTo descends more than one level";

            while (stack != null && skipDepth <= depth)
            {
                --depth;
                stack = stack.parent;
            }
            if (stack == null)
            {
                leadingTransition = -1;
                return depth = -1;
            }

            int index = skipTransition - 0x30;
            stack.curChild = Math.max(stack.curChild, index - 1);
            return advance();
        }

        @Override
        public int depth()
        {
            return depth;
        }

        @Override
        public T content()
        {
            return (T) stack.content;
        }

        @Override
        public int incomingTransition()
        {
            SpecStackEntry parent = stack != null ? stack.parent : null;
            return parent != null ? parent.curChild + 0x30 : leadingTransition;
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> alternateBranch()
        {
            if (stack.alternateBranch == null)
                return null;
            return new CursorFromSpec<>(makeSpecStackEntry(stack.alternateBranch, null), depth, incomingTransition());
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> duplicate()
        {
            return new CursorFromSpec<>(copyStack(stack), depth, leadingTransition);
        }

        static SpecStackEntry copyStack(SpecStackEntry stack)
        {
            if (stack == null)
                return null;
            return new SpecStackEntry(stack.children, stack.content, stack.alternateBranch, copyStack(stack.parent));
        }

        @Override
        public String toString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(incomingTransition())
                         .append("@")
                         .append(depth);
            if (stack.alternateBranch != null)
                stringBuilder.append(" alt");
            if (stack.content != null)
                stringBuilder.append(" content ")
                             .append(stack.content);
            stringBuilder.append(" children ");
            stringBuilder.append(IntStream.range(0, stack.children.length)
                                          .filter(i -> stack.children[i] != null)
                                          .mapToObj(i -> (i + 1 == stack.curChild ? "*" : "") + (char) (i + 0x30))
                                          .reduce("", (x, y) -> x + y));
            return stringBuilder.toString();
        }
    }

    static <T> TrieWithImpl<T> specifiedTrie(Object[] nodeDef)
    {
        return () -> new CursorFromSpec<>(nodeDef);
    }

    static <T> NonDeterministicTrieWithImpl<T> specifiedNonDeterministicTrie(Object[] nodeDef)
    {
        return () -> new CursorFromSpec<>(nodeDef);
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

        SortedMap<ByteComparable, ByteBuffer> expected = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        expected.put(comparable("00"), ByteBufferUtil.bytes(1));
        expected.put(comparable("01"), ByteBufferUtil.bytes(2));
        expected.put(comparable("2"), ByteBufferUtil.bytes(3));
        expected.put(comparable("30"), ByteBufferUtil.bytes(4));
        expected.put(comparable("4"), ByteBufferUtil.bytes(5));
        expected.put(comparable("6"), ByteBufferUtil.bytes(6));

        Trie<ByteBuffer> trie = specifiedTrie(trieDef);
        System.out.println(trie.dump());
        assertSameContent(trie, expected);
    }

    static ByteComparable comparable(String s)
    {
        ByteBuffer b = ByteBufferUtil.bytes(s);
        return ByteComparable.fixedLength(b);
    }

    @Test
    public void testDirect()
    {
        ByteComparable[] src = generateKeys(rand, COUNT);
        SortedMap<ByteComparable, ByteBuffer> content = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        InMemoryDTrie<ByteBuffer> trie = makeInMemoryDTrie(src, content, usePut());
        int keysize = Arrays.stream(src)
                            .mapToInt(src1 -> ByteComparable.length(src1, VERSION))
                            .sum();
        long ts = ObjectSizes.measureDeep(content);
        long onh = ObjectSizes.measureDeep(trie.contentArrays);
        System.out.format("Trie size on heap %,d off heap %,d measured %,d keys %,d treemap %,d\n",
                          trie.sizeOnHeap(), trie.sizeOffHeap(), onh, keysize, ts);
        System.out.format("per entry on heap %.2f off heap %.2f measured %.2f keys %.2f treemap %.2f\n",
                          trie.sizeOnHeap() * 1.0 / COUNT, trie.sizeOffHeap() * 1.0 / COUNT, onh * 1.0 / COUNT, keysize * 1.0 / COUNT, ts * 1.0 / COUNT);
        if (VERBOSE)
            System.out.println("Trie " + trie.dump(ByteBufferUtil::bytesToHex));

        assertSameContent(trie, content);
        checkGet(trie, content);

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
        InMemoryDTrie<String> trie = new InMemoryDTrie<>(BufferType.OFF_HEAP);
        for (String test : tests)
        {
            ByteComparable e = mapping.apply(test);
            System.out.println("Adding " + asString(e) + ": " + test);
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
        InMemoryDTrie<ByteBuffer> trie = new InMemoryDTrie<>(BufferType.OFF_HEAP);
        addToInMemoryDTrie(src, content, trie, usePut);
        return trie;
    }

    static void addToInMemoryDTrie(ByteComparable[] src,
                                  Map<ByteComparable, ByteBuffer> content,
                                  InMemoryDTrie<ByteBuffer> trie,
                                  boolean usePut)

    {
        for (ByteComparable b : src)
        {
            // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
            // (so that all sources have the same value).
            int payload = asString(b).hashCode();
            ByteBuffer v = ByteBufferUtil.bytes(payload);
            content.put(b, v);
            if (VERBOSE)
                System.out.println("Adding " + asString(b) + ": " + ByteBufferUtil.bytesToHex(v));
            putSimpleResolve(trie, b, v, (x, y) -> y, usePut);
            if (VERBOSE)
                System.out.println(trie.dump(ByteBufferUtil::bytesToHex));
        }
    }

    static void checkGet(InMemoryDTrie<ByteBuffer> trie, Map<ByteComparable, ByteBuffer> items)
    {
        for (Map.Entry<ByteComparable, ByteBuffer> en : items.entrySet())
        {
            assertEquals(en.getValue(), trie.get(en.getKey()));
        }
    }

    static void assertSameContent(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map)
    {
        assertMapEquals(trie, map);
        assertForEachEntryEquals(trie, map);
        assertValuesEqual(trie, map);
        assertForEachValueEquals(trie, map);
        assertUnorderedValuesEqual(trie, map);
    }

    private static void assertValuesEqual(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map)
    {
        assertIterablesEqual(trie.values(), map.values());
    }

    private static void assertUnorderedValuesEqual(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map)
    {
        Multiset<ByteBuffer> unordered = HashMultiset.create();
        StringBuilder errors = new StringBuilder();
        for (ByteBuffer b : trie.valuesUnordered())
            unordered.add(b);

        for (ByteBuffer b : map.values())
            if (!unordered.remove(b))
                errors.append("\nMissing value in valuesUnordered: " + ByteBufferUtil.bytesToHex(b));

        for (ByteBuffer b : unordered)
            errors.append("\nExtra value in valuesUnordered: " + ByteBufferUtil.bytesToHex(b));

        assertEquals("", errors.toString());
    }

    private static void assertForEachEntryEquals(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map)
    {
        Iterator<Map.Entry<ByteComparable, ByteBuffer>> it = map.entrySet().iterator();
        trie.forEachEntry((key, value) -> {
            Assert.assertTrue("Map exhausted first, key " + asString(key), it.hasNext());
            Map.Entry<ByteComparable, ByteBuffer> entry = it.next();
            assertEquals(0, ByteComparable.compare(entry.getKey(), key, TrieImpl.BYTE_COMPARABLE_VERSION));
            assertEquals(entry.getValue(), value);
        });
        Assert.assertFalse("Trie exhausted first", it.hasNext());
    }

    private static void assertForEachValueEquals(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map)
    {
        Iterator<ByteBuffer> it = map.values().iterator();
        trie.forEachValue(value -> {
            Assert.assertTrue("Map exhausted first, value " + ByteBufferUtil.bytesToHex(value), it.hasNext());
            ByteBuffer entry = it.next();
            assertEquals(entry, value);
        });
        Assert.assertFalse("Trie exhausted first", it.hasNext());
    }

    static void assertMapEquals(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map)
    {
        assertMapEquals(trie.entrySet(), map.entrySet());
    }

    static void assertMapEquals(Iterable<Map.Entry<ByteComparable, ByteBuffer>> container1,
                                Iterable<Map.Entry<ByteComparable, ByteBuffer>> container2)
    {
        Iterator<Map.Entry<ByteComparable, ByteBuffer>> it1 = container1.iterator();
        Iterator<Map.Entry<ByteComparable, ByteBuffer>> it2 = container2.iterator();
        List<ByteComparable> failedAt = new ArrayList<>();
        StringBuilder b = new StringBuilder();
        while (it1.hasNext() && it2.hasNext())
        {
            Map.Entry<ByteComparable, ByteBuffer> en1 = it1.next();
            Map.Entry<ByteComparable, ByteBuffer> en2 = it2.next();
            b.append(String.format("TreeSet %s:%s\n", asString(en2.getKey()), ByteBufferUtil.bytesToHex(en2.getValue())));
            b.append(String.format("Trie    %s:%s\n", asString(en1.getKey()), ByteBufferUtil.bytesToHex(en1.getValue())));
            if (ByteComparable.compare(en1.getKey(), en2.getKey(), VERSION) != 0 || ByteBufferUtil.compareUnsigned(en1.getValue(), en2.getValue()) != 0)
                failedAt.add(en1.getKey());
        }
        while (it1.hasNext())
        {
            Map.Entry<ByteComparable, ByteBuffer> en1 = it1.next();
            b.append(String.format("Trie    %s:%s\n", asString(en1.getKey()), ByteBufferUtil.bytesToHex(en1.getValue())));
            failedAt.add(en1.getKey());
        }
        while (it2.hasNext())
        {
            Map.Entry<ByteComparable, ByteBuffer> en2 = it2.next();
            b.append(String.format("TreeSet %s:%s\n", asString(en2.getKey()), ByteBufferUtil.bytesToHex(en2.getValue())));
            failedAt.add(en2.getKey());
        }
        if (!failedAt.isEmpty())
        {
            String message = "Failed at " + Lists.transform(failedAt, InMemoryTrieTestBase::asString);
            System.err.println(message);
            System.err.println(b);
            Assert.fail(message);
        }
    }

    static <E extends Comparable<E>> void assertIterablesEqual(Iterable<E> expectedIterable, Iterable<E> actualIterable)
    {
        Iterator<E> expected = expectedIterable.iterator();
        Iterator<E> actual = actualIterable.iterator();
        while (actual.hasNext() && expected.hasNext())
        {
            Assert.assertEquals(actual.next(), expected.next());
        }
        if (expected.hasNext())
            Assert.fail("Remaing values in expected, starting with " + expected.next());
        else if (actual.hasNext())
            Assert.fail("Remaing values in actual, starting with " + actual.next());
    }

    static ByteComparable[] generateKeys(Random rand, int count)
    {
        ByteComparable[] sources = new ByteComparable[count];
        TreeSet<ByteComparable> added = new TreeSet<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        for (int i = 0; i < count; ++i)
        {
            sources[i] = generateKey(rand);
            if (!added.add(sources[i]))
                --i;
        }

        // note: not sorted!
        return sources;
    }

    static ByteComparable generateKey(Random rand)
    {
        return generateKey(rand, MIN_LENGTH, MAX_LENGTH);
    }

    static ByteComparable generateKey(Random rand, int minLength, int maxLength)
    {
        int len = rand.nextInt(maxLength - minLength + 1) + minLength;
        byte[] bytes = new byte[len];
        int p = 0;
        int length = bytes.length;
        while (p < length)
        {
            int seed = rand.nextInt(KEY_CHOICE);
            Random r2 = new Random(seed);
            int m = r2.nextInt(5) + 2 + p;
            if (m > length)
                m = length;
            while (p < m)
                bytes[p++] = (byte) r2.nextInt(256);
        }
        return ByteComparable.fixedLength(bytes);
    }

    static String asString(ByteComparable bc)
    {
        return bc != null ? bc.byteComparableAsString(VERSION) : "null";
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
        catch (InMemoryDTrie.SpaceExhaustedException e)
        {
            // Should not happen, test stays well below size limit.
            throw new AssertionError(e);
        }
    }
}
