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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.base.Objects;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import org.junit.Assert;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TrieUtil
{
    // Set this to true (in combination with smaller count) to dump the tries while debugging a problem.
    // Do not commit the code with VERBOSE = true.
    static final boolean VERBOSE = false;
    static final int COUNT = 100000;
    static final ByteComparable.Version VERSION = InMemoryDTrie.BYTE_COMPARABLE_VERSION;
    public static final Comparator<ByteComparable> REVERSE_COMPARATOR = (bytes1, bytes2) -> ByteComparable.compare(invert(bytes1), invert(bytes2), VERSION);
    public static final Comparator<ByteComparable> FORWARD_COMPARATOR = (bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION);
    private static final int KEY_CHOICE = 25;
    private static final int MIN_LENGTH = 10;
    private static final int MAX_LENGTH = 50;

    static <T> void assertTrieEquals(BaseTrie<T> trie, Map<ByteComparable, T> map)
    {
        assertMapEquals(trie.entrySet(Direction.FORWARD),
                        map.entrySet(),
                        FORWARD_COMPARATOR);
        assertMapEquals(trie.entrySet(Direction.REVERSE),
                        reorderBy(map, REVERSE_COMPARATOR).entrySet(),
                        REVERSE_COMPARATOR);
    }

    static <T> void assertMapEquals(Iterable<Map.Entry<ByteComparable, T>> container1,
                                    Iterable<Map.Entry<ByteComparable, T>> container2,
                                    Comparator<ByteComparable> comparator)
    {
        Map<String, String> values1 = collectAsStrings(container1, comparator);
        Map<String, String> values2 = collectAsStrings(container2, comparator);
        if (values1.equals(values2))
            return;

        // If the maps are not equal, we want to print out the differences in a way that is easy to read.
        final Set<String> allKeys = Sets.union(values1.keySet(), values2.keySet());
        Set<String> keyDifference = allKeys.stream()
                                           .filter(k -> !Objects.equal(values1.get(k), values2.get(k)))
                                           .collect(Collectors.toCollection(() -> new TreeSet<>()));
        System.err.println("All data");
        dumpDiff(values1, values2, allKeys);
        System.err.println("\nDifferences");
        dumpDiff(values1, values2, keyDifference);
        fail("Maps are not equal at " + keyDifference);
    }

    private static void dumpDiff(Map<String, String> values1, Map<String, String> values2, Set<String> set)
    {
        for (String key : set)
        {
            String v1 = values1.get(key);
            if (v1 != null)
                System.err.println(String.format("Trie    %s:%s", key, v1));
            String v2 = values2.get(key);
            if (v2 != null)
                System.err.println(String.format("TreeSet %s:%s", key, v2));
        }
    }

    private static <T> Map<String, String> collectAsStrings(Iterable<Map.Entry<ByteComparable, T>> container,
                                                            Comparator<ByteComparable> comparator)
    {
        var map = new LinkedHashMap<String, String>();
        ByteComparable prevKey = null;
        for (var e : container)
        {
            var key = e.getKey();
            if (prevKey != null && comparator.compare(prevKey, key) >= 0)
                fail("Keys are not sorted: " + asString(prevKey) + " >= " + asString(key));
            prevKey = key;
            map.put(asString(key), e.getValue().toString());
        }
        return map;
    }

    static ByteComparable invert(ByteComparable b)
    {
        return version -> invert(b.asComparableBytes(version));
    }

    static ByteSource invert(ByteSource src)
    {
        return () ->
        {
            int v = src.next();
            if (v == ByteSource.END_OF_STREAM)
                return v;
            return v ^ 0xFF;
        };
    }

    static SpecStackEntry makeSpecStackEntry(Direction direction, Object spec, SpecStackEntry parent)
    {
        if (spec instanceof Pair)
            return makeSpecStackEntry(direction, ((Pair) spec).left(), ((Pair) spec).right(), parent);
        else
            return makeSpecStackEntry(direction, spec, null, parent);
    }

    static SpecStackEntry makeSpecStackEntry(Direction direction, Object spec, Object alternateBranch, SpecStackEntry parent)
    {
        assert !(spec instanceof Pair);
        if (spec instanceof Object[])
        {
            final Object[] specArray = (Object[]) spec;
            return new SpecStackEntry(specArray, null, alternateBranch, parent, direction.select(-1, specArray.length));
        }
        else
            return new SpecStackEntry(new Object[0], spec, alternateBranch, parent, direction.select(-1, 1));

    }

    static <T> TrieWithImpl<T> specifiedTrie(Object[] nodeDef)
    {
        return dir -> new CursorFromSpec<>(nodeDef, dir);
    }

    static <T extends NonDeterministicTrie.Mergeable<T>> NonDeterministicTrieWithImpl<T> specifiedNonDeterministicTrie(Object[] nodeDef)
    {
        return dir -> new NDCursorFromSpec<>(nodeDef, dir);
    }

    static ByteComparable comparable(String s)
    {
        ByteBuffer b = ByteBufferUtil.bytes(s);
        return ByteComparable.fixedLength(b);
    }

    static void assertSameContent(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map)
    {
        for (Direction dir : Direction.values())
        {
            assertMapEquals(trie, map, dir);
            assertForEachEntryEquals(trie, map, dir);
            assertValuesEqual(trie, map, dir);
            assertForEachValueEquals(trie, map, dir);
        }
        assertUnorderedValuesEqual(trie, map);
        checkGet(trie, map);
    }

    static void checkGet(Trie<ByteBuffer> trie, Map<ByteComparable, ByteBuffer> items)
    {
        for (Map.Entry<ByteComparable, ByteBuffer> en : items.entrySet())
        {
            assertEquals(en.getValue(), trie.get(en.getKey()));
        }
    }

    private static void assertValuesEqual(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map, Direction direction)
    {
        assertIterablesEqual(trie.values(direction), maybeReversed(direction, map).values());
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

    static Collection<ByteComparable> maybeReversed(Direction direction, Collection<ByteComparable> data)
    {
        return direction.isForward() ? data : reorderBy(data, REVERSE_COMPARATOR);
    }

    static <V> Map<ByteComparable, V> maybeReversed(Direction direction, Map<ByteComparable, V> data)
    {
        return direction.isForward() ? data : reorderBy(data, REVERSE_COMPARATOR);
    }

    private static <V> Map<ByteComparable, V> reorderBy(Map<ByteComparable, V> data, Comparator<ByteComparable> comparator)
    {
        Map<ByteComparable, V> newMap = new TreeMap<>(comparator);
        newMap.putAll(data);
        return newMap;
    }

    private static void assertForEachEntryEquals(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map, Direction direction)
    {
        Iterator<Map.Entry<ByteComparable, ByteBuffer>> it = maybeReversed(direction, map).entrySet().iterator();
        trie.forEachEntry((key, value) -> {
            Assert.assertTrue("Map exhausted first, key " + asString(key), it.hasNext());
            Map.Entry<ByteComparable, ByteBuffer> entry = it.next();
            assertEquals(0, ByteComparable.compare(entry.getKey(), key, TrieImpl.BYTE_COMPARABLE_VERSION));
            assertEquals(entry.getValue(), value);
        }, direction);
        Assert.assertFalse("Trie exhausted first", it.hasNext());
    }

    private static void assertForEachValueEquals(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map, Direction direction)
    {
        Iterator<ByteBuffer> it = maybeReversed(direction, map).values().iterator();
        trie.forEachValue(value -> {
            Assert.assertTrue("Map exhausted first, value " + ByteBufferUtil.bytesToHex(value), it.hasNext());
            ByteBuffer entry = it.next();
            assertEquals("Map " + ByteBufferUtil.bytesToHex(entry) + " vs trie " + ByteBufferUtil.bytesToHex(value), entry, value);
        }, direction);
        Assert.assertFalse("Trie exhausted first", it.hasNext());
    }

    static void assertMapEquals(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map, Direction direction)
    {
        assertMapEquals(trie.entryIterator(direction), maybeReversed(direction, map).entrySet().iterator());
    }

    static <E> Collection<E> reorderBy(Collection<E> original, Comparator<E> comparator)
    {
        List<E> list = original.stream().collect(Collectors.toList());
        list.sort(comparator);
        return list;
    }

    static void assertMapEquals(Iterator<Map.Entry<ByteComparable, ByteBuffer>> it1,
                                Iterator<Map.Entry<ByteComparable, ByteBuffer>> it2)
    {
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
            String message = "Failed at " + Lists.transform(failedAt, TrieUtil::asString);
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
        TreeSet<ByteComparable> added = new TreeSet<>(FORWARD_COMPARATOR);
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
        return generateKey(rand, MIN_LENGTH, MAX_LENGTH, ByteSource.TERMINATOR);
    }

    static ByteComparable generateKeyBound(Random rand)
    {
        return generateKey(rand, MIN_LENGTH, MAX_LENGTH, ByteSource.LT_NEXT_COMPONENT);
    }

    static ByteComparable generateKey(Random rand, int minLength, int maxLength, int terminator)
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
        return v -> ByteSource.withTerminator(terminator, ByteSource.of(bytes, v));
    }

    static class SwappedLastByte implements ByteSource
    {
        final ByteSource source;
        final int newLastByte;
        int next;

        SwappedLastByte(ByteSource source, int newLastByte)
        {
            this.source = source;
            this.newLastByte = newLastByte;
            next = source.next();
        }

        @Override
        public int next()
        {
            int toReturn = next;
            if (toReturn == END_OF_STREAM)
                return toReturn;
            next = source.next();
            return (next != END_OF_STREAM) ? toReturn : newLastByte;
        }
    }

    static ByteComparable toBound(ByteComparable bc)
    {
        return toBound(bc, false);
    }

    static ByteComparable toBound(ByteComparable bc, boolean greater)
    {
        if (bc == null)
            return null;

        ByteComparable res = v -> new SwappedLastByte(bc.asComparableBytes(v), greater ? ByteSource.GT_NEXT_COMPONENT : ByteSource.LT_NEXT_COMPONENT);
//        System.out.println(bc.byteComparableAsString(VERSION) + " -> " + res.byteComparableAsString(VERSION));
        return res;
    }

    static String asString(ByteComparable bc)
    {
        return bc != null ? bc.byteComparableAsString(VERSION) : "null";
    }

    static class SpecStackEntry
    {
        Object[] children;
        int curChild;
        Object content;
        SpecStackEntry parent;
        Object alternateBranch;

        public SpecStackEntry(Object[] spec, Object content, Object alternateBranch, SpecStackEntry parent, int curChild)
        {
            this.children = spec;
            this.content = content;
            this.parent = parent;
            this.alternateBranch = alternateBranch;
            this.curChild = curChild;
        }
    }

    public static class CursorFromSpec<T> implements TrieImpl.Cursor<T>
    {
        SpecStackEntry stack;
        int depth;
        int leadingTransition;
        Direction direction;

        CursorFromSpec(Object[] spec, Direction direction)
        {
            this.direction = direction;
            stack = makeSpecStackEntry(direction, spec, null, null);
            depth = 0;
            leadingTransition = -1;
        }

        CursorFromSpec(SpecStackEntry stack, int depth, int leadingTransition, Direction direction)
        {
            this.direction = direction;
            this.stack = stack;
            this.depth = depth;
            this.leadingTransition = leadingTransition;
        }

        @Override
        public int advance()
        {
            SpecStackEntry current = stack;
            Object child;
            do
            {
                while (current != null
                       && (current.children.length == 0
                           || !direction.inLoop(current.curChild += direction.increase, 0, current.children.length - 1)))
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
            stack = makeSpecStackEntry(direction, child, current);

            return ++depth;
        }

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
            stack.curChild = direction.max(stack.curChild, index - direction.increase);
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
        public TrieImpl.Cursor<T> duplicate()
        {
            return new CursorFromSpec<>(copyStack(stack), depth, leadingTransition, direction);
        }

        static SpecStackEntry copyStack(SpecStackEntry stack)
        {
            if (stack == null)
                return null;
            return new SpecStackEntry(stack.children, stack.content, stack.alternateBranch, copyStack(stack.parent), stack.curChild);
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

    public static class NDCursorFromSpec<T extends NonDeterministicTrie.Mergeable<T>>
    extends CursorFromSpec<T>
    implements NonDeterministicTrieImpl.Cursor<T>
    {
        NDCursorFromSpec(Object[] spec, Direction direction)
        {
            super(spec, direction);
        }

        NDCursorFromSpec(SpecStackEntry stack, int depth, int leadingTransition, Direction direction)
        {
            super(stack, depth, leadingTransition, direction);
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> alternateBranch()
        {
            if (stack.alternateBranch == null)
                return null;
            return new NDCursorFromSpec<>(makeSpecStackEntry(direction, stack.alternateBranch, null),
                                          depth,
                                          incomingTransition(),
                                          direction);
        }

        @Override
        public NonDeterministicTrieImpl.Cursor<T> duplicate()
        {
            return new NDCursorFromSpec<>(copyStack(stack), depth, leadingTransition, direction);
        }
    }
}
