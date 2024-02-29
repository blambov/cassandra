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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSourceTestBase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class TrieDuplicationTest
{
    private static final int TRIES = 10;
    private static final int TESTS = 20;
    private static final double TARGET_DUPES_PER_TRY = 5;
    Random rand = new Random(1);

    @Test
    public void testDuplicationSingleton()
    {
        // Use non-duplicatable byte sources to ensure duplicate logic is executed correctly.
        for (BigInteger v : ByteSourceTestBase.testBigInts)
        {
            ByteComparable comparable = typeToComparable(IntegerType.instance, v);
            testDuplication(Trie.singleton(comparable, v), "Type bigint value " + v);
        }
    }

    interface RangeTrieMaker
    {
        BaseTrie<?> makeTrie(ByteComparable left, Object leftValue, ByteComparable right, Object rightValue);
    }

    @Test
    public void testDuplicationAlternateRange()
    {
        testDuplicationRange(AlternateRangeTrie::new);
    }

    @Test
    public void testDuplicationRangeMerge()
    {
        testDuplicationRange((left, leftValue, right, rightValue) -> Trie.singleton(left, leftValue)
                                                                         .mergeWith(Trie.singleton(right, rightValue),
                                                                                    (x, y) -> x));
    }

    @Test
    public void testDuplicationInMemTrieRange()
    {
        testDuplicationRange((left, leftValue, right, rightValue) -> {
            InMemoryDTrie<Object> trie = new InMemoryDTrie<>(BufferType.ON_HEAP);
            try
            {
                trie.putRecursive(left, leftValue, (x, y) -> y);
                trie.putRecursive(right, rightValue, (x, y) -> y);
            }
            catch (InMemoryDTrie.SpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
            return trie;
        });
    }

    @Test
    public void testDuplicationInMemTrieAlternateRange()
    {
        testDuplicationRange((left, leftValue, right, rightValue) -> {
            InMemoryNDTrie<Object> trie = new InMemoryNDTrie<>(BufferType.ON_HEAP);
            try
            {
                trie.putAlternativeRangeRecursive(left, leftValue, right, rightValue, (x, y) -> y);
            }
            catch (InMemoryNDTrie.SpaceExhaustedException e)
            {
                throw new AssertionError(e);
            }
            return trie;
        });
    }

    public void testDuplicationRange(RangeTrieMaker trieMaker)
    {
        // Use non-duplicatable byte sources to ensure duplicate logic is executed correctly.
        for (int i = 0; i < TESTS; ++i)
        {
            BigInteger v1 = randomBigInt();
            BigInteger v2 = randomBigInt();
            testForRange(v1, v2, IntegerType.instance, trieMaker);
        }

        // try some prefix repetition
        for (int i : new int[] {0, 256, 65536, 16777216, 268435456, 2147483647})
        {
            testForRange(i, i + 1, Int32Type.instance, trieMaker);
            testForRange(i + 0L, i + 1L, LongType.instance, trieMaker);
            for (int j = 0; j < TESTS; ++j)
            {
                testForRange((i + 0L << 32) | rand.nextInt(),
                             (i + 0L << 32) | rand.nextInt(),
                             LongType.instance,
                             trieMaker);
            }
        }
    }

    private BigInteger randomBigInt()
    {
        return ByteSourceTestBase.testBigInts[rand.nextInt(ByteSourceTestBase.testBigInts.length)];
    }

    private <T extends Comparable<T>> void testForRange(T v1, T v2, AbstractType<T> type, RangeTrieMaker trieMaker)
    {
        if (v1.compareTo(v2) > 0)
        {
            T tmp = v1; v1 = v2; v2 = tmp;
        }
        final BaseTrie<?> trie = trieMaker.makeTrie(typeToComparable(type, v1), v1, typeToComparable(type, v2), v2);
        testDuplication(trie,
                        "Range " + v1 + "-" + v2);
    }

    @Test
    public void testDuplicationInMemTrie() throws InMemoryDTrie.SpaceExhaustedException
    {
        InMemoryDTrie<BigInteger> t = new InMemoryDTrie<>(BufferType.ON_HEAP);
        for (BigInteger v : ByteSourceTestBase.testBigInts)
        {
            ByteComparable comparable = typeToComparable(IntegerType.instance, v);
            t.putRecursive(comparable, v, (x, y) -> y);
        }
        testDuplication(t, "InMemoryDTrie");
    }

    @Test
    public void testDuplicationBigMerge()
    {
        List<Trie<BigInteger>> tries = new ArrayList<>();
        for (BigInteger v : ByteSourceTestBase.testBigInts)
        {
            ByteComparable comparable = typeToComparable(IntegerType.instance, v);
            tries.add(Trie.singleton(comparable, v));
        }
        testDuplication(Trie.merge(tries, c -> c.iterator().next()), "InMemoryDTrie");
    }

    @Test
    public void testDuplicationRanges() throws InMemoryDTrie.SpaceExhaustedException
    {
        InMemoryDTrie<BigInteger> t = new InMemoryDTrie<>(BufferType.ON_HEAP);
        for (BigInteger v : ByteSourceTestBase.testBigInts)
        {
            ByteComparable comparable = typeToComparable(IntegerType.instance, v);
            t.putRecursive(comparable, v, (x, y) -> y);
        }
        ByteComparable[] ranges = new ByteComparable[ByteSourceTestBase.testBigInts.length / 4];
        for (int i = 0; i < ranges.length; ++i)
            ranges[i] = typeToComparable(IntegerType.instance, ByteSourceTestBase.testBigInts[rand.nextInt(ByteSourceTestBase.testBigInts.length)]);
        Arrays.sort(ranges, (x, y) -> ByteComparable.compare(x, y, TrieImpl.BYTE_COMPARABLE_VERSION));
        Trie<BigInteger> ix = t.intersect(TrieSet.ranges(ranges));

        testDuplication(ix, "InMemoryDTrie");
    }

    public ByteComparable typeToComparable(AbstractType t, Object o)
    {
        return v -> t.asComparableBytes(t.decompose(o), v);
    }

    static class WalkState<T>
    {
        int depth;
        int incomingTransition;
        T content;

        WalkState(TrieImpl.Cursor<T> cursor)
        {
            depth = cursor.depth();
            incomingTransition = cursor.incomingTransition();
            content = cursor.content();
        }

        void verify(TrieImpl.Cursor<T> cursor, String msg)
        {
            assertEquals(msg + " Depth", depth, cursor.depth());
            assertEquals(msg + " Incoming transition", incomingTransition, cursor.incomingTransition());
            assertSame(msg + " Content", content, cursor.content());
        }
    }

    private <T> void testDuplication(BaseTrie<T> trie, String msg)
    {
        testDuplicationVersions(trie, msg);
        ByteComparable left = typeToComparable(IntegerType.instance, randomBigInt());
        ByteComparable right = typeToComparable(IntegerType.instance, randomBigInt());
        if (ByteComparable.compare(left, right, TrieImpl.BYTE_COMPARABLE_VERSION) > 0)
        {
            ByteComparable tmp = left;
            left = right;
            right = tmp;
        }
        msg += " subtrie " +
               left.byteComparableAsString(TrieImpl.BYTE_COMPARABLE_VERSION) + ":" +
               right.byteComparableAsString(TrieImpl.BYTE_COMPARABLE_VERSION);
        testDuplicationVersions(trie.subtrie(left,
                                            right),
                               msg);
    }

    private <T> void testDuplicationVersions(BaseTrie<T> trie, String msg)
    {
        if (trie instanceof Trie)
        {
            testDuplicationVersion((Trie<T>) trie, msg);
        }
        else if (trie instanceof NonDeterministicTrie)
        {
            NonDeterministicTrie<T> ndt = (NonDeterministicTrie<T>) trie;
            testDuplicationVersion((TrieWithImpl<T>) NonDeterministicTrieImpl.impl(ndt)::cursor, msg);  // main path only
            testDuplicationVersion(ndt.deterministic(c -> c.iterator().next()),
                                   msg + " with alternates");
        }
        else
        {
            throw new AssertionError("Unknown trie type: " + trie);
        }
    }

    private <T> void testDuplicationVersion(Trie<T> trie, String msg)
    {
        List<WalkState<T>> walk = new ArrayList<>();
        TrieImpl.Cursor<T> cursor = TrieImpl.impl(trie).cursor();
        walk.add(new WalkState<>(cursor));
        while (cursor.advance() >= 0)
            walk.add(new WalkState<>(cursor));

        double dupeProbability = Math.min(0.6, TARGET_DUPES_PER_TRY / walk.size()); // A few duplications per entry on average

        IntArrayList positions = new IntArrayList();
        List<TrieImpl.Cursor<T>> sources = new ArrayList<>();
        for (int test = 0; test < Math.max(1, TRIES / walk.size()); ++test)
        {
            sources.add(TrieImpl.impl(trie).cursor());
            positions.add(0);

            while (!sources.isEmpty())
            {
                int index = rand.nextInt(sources.size());
                int pos = positions.getInt(index);
                TrieImpl.Cursor<T> source = sources.get(index);
                walk.get(pos).verify(source, msg);
                if (rand.nextDouble() <= dupeProbability)
                {
                    TrieImpl.Cursor<T> duplicate = source.duplicate();
                    sources.add(duplicate);
                    positions.add(pos);
                }
                ++pos;
                if (source.advance() >= 0)
                {
                    positions.setInt(index, pos);
                }
                else
                {
                    assertEquals(msg, walk.size(), pos);
                    sources.remove(index);
                    positions.remove(index);
                }
            }
        }
    }
}
