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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.VERSION;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.asString;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.comparable;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.specifiedTrie;

public class AlternativeBranchesTest
{
    static final int COUNT = 10;
    static final Random rand = new Random(1);

    @Test
    public void testSpecifiedSimple()
    {
        Trie<Integer> specified = specifiedTrie(new Object[] {
            Pair.create(
                new Object[] { // 0
                    new Object[] { 0, 1, 2 }, // 00
                    null, // 01
                    new Object[] { 2, null, 4 }, // 02
                },
                new Object[] { // 0
                    null,
                    new Object[] { 11, 12, 13 }, // 01
                    new Object[] { null, 13, 14 }, // 02
                }
            )
        });

        assertMapEquals(specified.entrySet(),
                        ImmutableMap.of(comparable("000"), 0,
                                        comparable("001"), 1,
                                        comparable("002"), 2,
                                        comparable("020"), 2,
                                        comparable("022"), 4)
                                    .entrySet());
        assertMapEquals(specified.mergeAlternativeBranches(c -> c.stream().reduce(Integer::sum).get()).entrySet(),
                        ImmutableMap.of(comparable("000"), 0,
                                        comparable("001"), 1,
                                        comparable("002"), 2,
                                        comparable("010"), 11,
                                        comparable("011"), 12,
                                        comparable("012"), 13,
                                        comparable("020"), 2,
                                        comparable("021"), 13,
                                        comparable("022"), 18)
                                    .entrySet());
    }

    final Trie.CollectionMergeResolver<Integer> RESOLVER_FIRST = c -> c.iterator().next();

    @Test
    public void testRandomSingletons()
    {
        List<Trie<Integer>> tries = new ArrayList<>();
        SortedMap<ByteComparable, Integer> normals = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        SortedMap<ByteComparable, Integer> alternates = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        for (int i = 0; i < COUNT; ++i)
        {
            String key = makeSpecKey(rand);
            int value = key.hashCode() & 0xFF; // to make sure value is the same on clash
            if (rand.nextDouble() <= 0.3)
            {
                tries.add(specifiedTrie((Object[]) makeSpec(key, -1, value)));
                normals.put(comparable(key), value);
            }
            else
            {
                tries.add(specifiedTrie((Object[]) makeSpec(key, rand.nextInt(key.length()), -value)));
                alternates.put(comparable(key), -value);
            }
        }
        Trie<Integer> union = Trie.merge(tries, RESOLVER_FIRST);
        verifyAlternates(union, normals, alternates, false);

        union = Trie.merge(tries.subList(0, COUNT/2), RESOLVER_FIRST)
                    .mergeWith(Trie.merge(tries.subList(COUNT/2, COUNT), RESOLVER_FIRST),
                               RESOLVER_FIRST);
        verifyAlternates(union, normals, alternates, false);
    }

    @Test
    public void testPutAlternativeRecursive() throws InMemoryTrie.SpaceExhaustedException
    {
        InMemoryTrie<Integer> trie = new InMemoryTrie<>(BufferType.ON_HEAP);
        SortedMap<ByteComparable, Integer> normals = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        SortedMap<ByteComparable, Integer> alternates = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        for (int i = 0; i < COUNT; ++i)
        {
            String key = makeSpecKey(rand);
            int value = key.hashCode() & 0xFF; // to make sure value is the same on clash
            if (rand.nextDouble() <= 0.3)
            {
                trie.putRecursive(comparable(key), value, (x, y) -> y);
                normals.put(comparable(key), value);
                System.out.println("Adding " + asString(comparable(key)) + ": " + value);
            }
            else
            {
                trie.putAlternativeRecursive(comparable(key), -value, (x, y) -> y);
                alternates.put(comparable(key), -value);
                System.out.println("Adding " + asString(comparable(key)) + ": " + -value);
            }
        }
        verifyAlternates(trie, normals, alternates, true);

        // Now try adding normals first, followed by alternates to have the alternates branch lower.
        trie = new InMemoryTrie<>(BufferType.ON_HEAP);
        putNth(trie, normals, 1, 0);    // puts all
        putNth(trie, alternates, 1, 0);    // puts all
        verifyAlternates(trie, normals, alternates, false);

        // Now try a more complex mixture.
        trie = new InMemoryTrie<>(BufferType.ON_HEAP);
        putNth(trie, normals, 3, 2);
        putNth(trie, alternates, 2, 1);
        putNth(trie, normals, 3, 1);
        putNth(trie, alternates, 2, 0);
        putNth(trie, normals, 3, 0);
        verifyAlternates(trie, normals, alternates, false);
    }

    static void putNth(InMemoryTrie<Integer> trie, Map<ByteComparable, Integer> data, int divisor, int remainder) throws InMemoryTrie.SpaceExhaustedException
    {
        int i = 0;
        for (var e : data.entrySet())
        {
            if (i % divisor == remainder)
            {
                System.out.println("Adding " + asString(e.getKey()) + ": " + e.getValue());
                if (e.getValue() < 0)
                    trie.putAlternativeRecursive(e.getKey(), e.getValue(), (x, y) -> y);
                else
                    trie.putRecursive(e.getKey(), e.getValue(), (x, y) -> y);
            }
            ++i;
        }
    }

    private void verifyAlternates(Trie<Integer> trie, SortedMap<ByteComparable, Integer> normals, SortedMap<ByteComparable, Integer> alternates, boolean checkAlternatesView)
    {
        SortedMap<ByteComparable, Integer> both = new TreeMap<>(alternates);
        both.putAll(normals);
        ByteComparable left = comparable("3");
        ByteComparable right = comparable("7");

        System.out.println(trie.dump());

        assertMapEquals(trie.entrySet(), normals.entrySet());
        if (checkAlternatesView)
            assertMapEquals(trie.alternateView().entrySet(), alternates.entrySet());
        assertMapEquals(trie.mergeAlternativeBranches(RESOLVER_FIRST).entrySet(), both.entrySet());
        Trie<Integer> ix = trie.subtrie(left, right);
        assertMapEquals(ix.entrySet(), normals.subMap(left, right).entrySet());
        if (checkAlternatesView)
            assertMapEquals(ix.alternateView().entrySet(), alternates.subMap(left, right).entrySet());
        assertMapEquals(ix.mergeAlternativeBranches(RESOLVER_FIRST).entrySet(), both.subMap(left, right).entrySet());

        if (checkAlternatesView)
        {
            assertMapEquals(trie.alternateView()
                                .subtrie(left, right)
                                .entrySet(),
                            alternates.subMap(left, right)
                                      .entrySet());
        }
        assertMapEquals(trie.mergeAlternativeBranches(RESOLVER_FIRST)
                             .subtrie(left, right)
                             .entrySet(),
                        both.subMap(left, right)
                            .entrySet());
    }

    String makeSpecKey(Random rand)
    {
        int len = rand.nextInt(10) + 10;
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; ++i)
            b.append(rand.nextInt(10) + '0');
        return b.toString();
    }

    public Object makeSpec(String s, int alternateAt, int value)
    {
        if (s.isEmpty())
            return value;
        Object child = makeSpec(s.substring(1), alternateAt - 1, value);
        if (alternateAt == 0)
            child = Pair.create(null, child);
        Object[] arr = new Object[10];
        arr[s.charAt(0) - '0'] = child;
        return arr;
    }

    static <T> void assertMapEquals(Iterable<Map.Entry<ByteComparable, T>> container1,
                                    Iterable<Map.Entry<ByteComparable, T>> container2)
    {
        Iterator<Map.Entry<ByteComparable, T>> it1 = container1.iterator();
        Iterator<Map.Entry<ByteComparable, T>> it2 = container2.iterator();
        List<ByteComparable> failedAt = new ArrayList<>();
        StringBuilder b = new StringBuilder();
        while (it1.hasNext() && it2.hasNext())
        {
            Map.Entry<ByteComparable, T> en1 = it1.next();
            Map.Entry<ByteComparable, T> en2 = it2.next();
            b.append(String.format("TreeSet %s:%s\n", asString(en2.getKey()), (en2.getValue())));
            b.append(String.format("Trie    %s:%s\n", asString(en1.getKey()), (en1.getValue())));
            if (ByteComparable.compare(en1.getKey(), en2.getKey(), VERSION) != 0 || !Objects.equal(en1.getValue(), en2.getValue()))
                failedAt.add(en1.getKey());
        }
        while (it1.hasNext())
        {
            Map.Entry<ByteComparable, T> en1 = it1.next();
            b.append(String.format("Trie    %s:%s\n", asString(en1.getKey()), (en1.getValue())));
            failedAt.add(en1.getKey());
        }
        while (it2.hasNext())
        {
            Map.Entry<ByteComparable, T> en2 = it2.next();
            b.append(String.format("TreeSet %s:%s\n", asString(en2.getKey()), (en2.getValue())));
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
}
