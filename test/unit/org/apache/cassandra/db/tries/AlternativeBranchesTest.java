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
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.IntPredicate;
import java.util.stream.Collectors;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.VERSION;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.asString;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.comparable;
import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.specifiedNonDeterministicTrie;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AlternativeBranchesTest
{
    static final int COUNT = 10000;
    static final Random rand = new Random(111);

    static class MergeableInteger implements NonDeterministicTrie.Mergeable<MergeableInteger>
    {
        final int value;

        MergeableInteger(int value)
        {
            this.value = value;
        }

        @Override
        public MergeableInteger mergeWith(MergeableInteger other)
        {
            return of(value + other.value);
        }

        @Override
        public String toString()
        {
            return Integer.toString(value);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MergeableInteger that = (MergeableInteger) o;
            return value == that.value;
        }
    }

    static MergeableInteger of(int v)
    {
        return new MergeableInteger(v);
    }

    @Test
    public void testSpecifiedSimple()
    {
        NonDeterministicTrie<MergeableInteger> specified = specifiedNonDeterministicTrie(new Object[] {
            Pair.create(
                new Object[] { // 0
                    new Object[] { of(0), of(1), of(2) }, // 00
                    null, // 01
                    new Object[] { of(2), null, of(4) }, // 02
                },
                new Object[] { // 0
                    null,
                    new Object[] { of(11), of(12), of(13) }, // 01
                    new Object[] { null, of(13), of(14) }, // 02
                }
            )
        });

        assertMapEquals(specified.entrySet(),
                        ImmutableMap.of(comparable("000"), of(0),
                                        comparable("001"), of(1),
                                        comparable("002"), of(2),
                                        comparable("010"), of(11),
                                        comparable("011"), of(12),
                                        comparable("012"), of(13),
                                        comparable("020"), of(2),
                                        comparable("021"), of(13),
                                        comparable("022"), of(18))
                                    .entrySet());
        assertMapEquals(specified.mainPathOnly().entrySet(),
                        ImmutableMap.of(comparable("000"), of(0),
                                        comparable("001"), of(1),
                                        comparable("002"), of(2),
                                        comparable("020"), of(2),
                                        comparable("022"), of(4))
                                    .entrySet());
        assertMapEquals(specified.deterministic().entrySet(),
                        ImmutableMap.of(comparable("000"), of(0),
                                        comparable("001"), of(1),
                                        comparable("002"), of(2),
                                        comparable("010"), of(11),
                                        comparable("011"), of(12),
                                        comparable("012"), of(13),
                                        comparable("020"), of(2),
                                        comparable("021"), of(13),
                                        comparable("022"), of(18))
                                    .entrySet());
    }

    static <T extends NonDeterministicTrie.Mergeable<T>> TrieWithImpl<T> mainBranch(NonDeterministicTrie<T> ndtrie)
    {
        return NonDeterministicTrieImpl.impl(ndtrie)::cursor;
    }

    static <T extends NonDeterministicTrie.Mergeable<T>> TrieWithImpl<T> alternatesOnly(NonDeterministicTrie<T> ndtrie)
    {
        return new MergeAlternativeBranchesTrie<>(NonDeterministicTrieImpl.impl(ndtrie), true);
    }

    @Test
    public void testRandomSingletons()
    {
        List<NonDeterministicTrie<MergeableInteger>> tries = new ArrayList<>();
        SortedMap<ByteComparable, MergeableInteger> normals = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        SortedMap<ByteComparable, MergeableInteger> alternates = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        for (int i = 0; i < COUNT; ++i)
        {
            String key = makeSpecKey(rand);
            int value = key.hashCode() & 0xFF; // to make sure value is the same on clash
            if (rand.nextDouble() <= 0.3)
            {
                tries.add(specifiedNonDeterministicTrie((Object[]) makeSpec(key, -1, value)));
                normals.put(comparable(key), of(value));
            }
            else
            {
                tries.add(specifiedNonDeterministicTrie((Object[]) makeSpec(key, rand.nextInt(key.length()), ~value)));
                alternates.put(comparable(key), of(~value));
            }
        }
        NonDeterministicTrie<MergeableInteger> union = NonDeterministicTrie.merge(tries);
        verifyAlternates(union, normals, alternates);

        union = NonDeterministicTrie.merge(tries.subList(0, COUNT/2))
                    .mergeWith(NonDeterministicTrie.merge(tries.subList(COUNT/2, COUNT)));
        verifyAlternates(union, normals, alternates);
    }

    @Test
    public void testPutAlternativeRecursive() throws InMemoryNDTrie.SpaceExhaustedException
    {
        InMemoryNDTrie<MergeableInteger> trie = new InMemoryNDTrie<>(BufferType.ON_HEAP);
        SortedMap<ByteComparable, MergeableInteger> normals = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        SortedMap<ByteComparable, MergeableInteger> alternates = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        for (int i = 0; i < COUNT; ++i)
        {
            String key = makeSpecKey(rand);
            int value = key.hashCode() & 0xFF; // to make sure value is the same on clash
            if (rand.nextDouble() <= 0.3)
            {
                trie.putRecursive(comparable(key), of(value), (x, y) -> y);
                normals.put(comparable(key), of(value));
//                System.out.println("Adding " + asString(comparable(key)) + ": " + value);
            }
            else
            {
                trie.putAlternativeRecursive(comparable(key), of(~value), (x, y) -> y);
                alternates.put(comparable(key), of(~value));
//                System.out.println("Adding " + asString(comparable(key)) + ": " + ~value);
            }
        }
        verifyAlternates(trie, normals, alternates);

        // Now try adding normals first, followed by alternates to have the alternates branch lower.
        trie = new InMemoryNDTrie<>(BufferType.ON_HEAP);
        putNth(trie, normals, 1, 0);    // puts all
        putNth(trie, alternates, 1, 0);    // puts all
        verifyAlternates(trie, normals, alternates);

        // Now try a more complex mixture.
        trie = new InMemoryNDTrie<>(BufferType.ON_HEAP);
        putNth(trie, normals, 3, 2);
        putNth(trie, alternates, 2, 1);
        putNth(trie, normals, 3, 1);
        putNth(trie, alternates, 2, 0);
        putNth(trie, normals, 3, 0);
        verifyAlternates(trie, normals, alternates);
    }

    static void putNth(InMemoryNDTrie<MergeableInteger> trie, Map<ByteComparable, MergeableInteger> data, int divisor, int remainder) throws InMemoryNDTrie.SpaceExhaustedException
    {
//        System.out.println();
        int i = 0;
        for (var e : data.entrySet())
        {
            if (i % divisor == remainder)
            {
//                System.out.println("Adding " + asString(e.getKey()) + ": " + e.getValue());
                if (e.getValue().value < 0)
                    trie.putAlternativeRecursive(e.getKey(), e.getValue(), (x, y) -> y);
                else
                    trie.putRecursive(e.getKey(), e.getValue(), (x, y) -> y);
            }
            ++i;
        }
    }

    @Test
    public void testPutAlternativeRangeRecursive() throws InMemoryNDTrie.SpaceExhaustedException
    {
        testPutAlternativeRangeRecursive(i -> rand.nextDouble() < 0.7);
        testPutAlternativeRangeRecursive(i -> rand.nextDouble() < 0.5);
        testPutAlternativeRangeRecursive(i -> rand.nextDouble() < 0.3);

        testPutAlternativeRangeRecursive(i -> i >= COUNT / 2);  // normal first
        testPutAlternativeRangeRecursive(i -> i >= COUNT / 5 && rand.nextDouble() < 0.4);
    }

    public void testPutAlternativeRangeRecursive(IntPredicate alternateChooser) throws InMemoryNDTrie.SpaceExhaustedException
    {
        InMemoryNDTrie<MergeableInteger> trie = new InMemoryNDTrie<>(BufferType.ON_HEAP);
        SortedMap<ByteComparable, MergeableInteger> normals = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        SortedMap<ByteComparable, MergeableInteger> alternates = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        for (int i = 0; i < COUNT; ++i)
        {
            String skey = makeSpecKey(rand);
            int svalue = skey.hashCode() & 0xFF; // to make sure value is the same on clash
            String ekey = makeSpecKey(rand);
            int evalue = ekey.hashCode() & 0xFF; // to make sure value is the same on clash
            if (!alternateChooser.test(i))
            {
                trie.putRangeRecursive(comparable(skey), of(svalue), comparable(ekey), of(evalue), (x, y) -> y);
                normals.put(comparable(skey), of(svalue));
                normals.put(comparable(ekey), of(evalue));
//                System.out.println("Adding " + asString(comparable(key)) + ": " + value);
            }
            else
            {
                trie.putAlternativeRangeRecursive(comparable(skey), of(~svalue), comparable(ekey), of(~evalue), (x, y) -> y);
                alternates.put(comparable(skey), of(~svalue));
                alternates.put(comparable(ekey), of(~evalue));
//                System.out.println("Adding " + asString(comparable(key)) + ": " + ~value);
            }
        }
        verifyAlternates(trie, normals, alternates);
    }


    @Test
    public void testApplyAlternativeRange() throws InMemoryNDTrie.SpaceExhaustedException
    {
        testApplyAlternativeRange(i -> rand.nextDouble() < 0.7);
        testApplyAlternativeRange(i -> rand.nextDouble() < 0.5);
        testApplyAlternativeRange(i -> rand.nextDouble() < 0.3);

        testApplyAlternativeRange(i -> i >= COUNT / 2);  // normal first
        testApplyAlternativeRange(i -> i >= COUNT / 5 && rand.nextDouble() < 0.4);
    }

    public void testApplyAlternativeRange(IntPredicate alternateChooser) throws InMemoryNDTrie.SpaceExhaustedException
    {
        InMemoryNDTrie<MergeableInteger> trie = new InMemoryNDTrie<>(BufferType.ON_HEAP);
        SortedMap<ByteComparable, MergeableInteger> normals = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        SortedMap<ByteComparable, MergeableInteger> alternates = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        for (int i = 0; i < COUNT; ++i)
        {
            String skey = makeSpecKey(rand);
            int svalue = skey.hashCode() & 0xFF; // to make sure value is the same on clash
            String ekey = makeSpecKey(rand);
            int evalue = ekey.hashCode() & 0xFF; // to make sure value is the same on clash
            if (!alternateChooser.test(i))
            {
                trie.apply(Trie.singleton(comparable(skey), of(svalue))
                               .mergeWith(Trie.singleton(comparable(ekey), of(evalue)), (x, y) -> y),
                           (x, y) -> y);
                normals.put(comparable(skey), of(svalue));
                normals.put(comparable(ekey), of(evalue));
//                System.out.println("Adding " + asString(comparable(key)) + ": " + value);
            }
            else
            {
                trie.apply(alternateRange(comparable(skey), of(~svalue), comparable(ekey), of(~evalue)), (x, y) -> y);
                alternates.put(comparable(skey), of(~svalue));
                alternates.put(comparable(ekey), of(~evalue));
//                System.out.println("Adding " + asString(comparable(key)) + ": " + ~value);
            }
        }
        verifyAlternates(trie, normals, alternates);
    }

    private <T extends NonDeterministicTrie.Mergeable<T>>
    NonDeterministicTrieWithImpl<T> alternateRange(ByteComparable sComparable, T svalue, ByteComparable eComparable, T evalue)
    {
        if (ByteComparable.compare(sComparable, eComparable, TrieImpl.BYTE_COMPARABLE_VERSION) > 0)
            return () -> new AlternateRangeCursor<>(eComparable, evalue, sComparable, svalue);
        else
            return () -> new AlternateRangeCursor<>(sComparable, svalue, eComparable, evalue);
    }

    @Test
    public void testCoveredVisitsRangePut() throws InMemoryNDTrie.SpaceExhaustedException
    {
        testCoveredVisitsRange((trie, sComparable, svalue, eComparable, evalue) -> {
            trie.putAlternativeRangeRecursive(sComparable, svalue, eComparable, evalue, (x, y) -> of(y));
            return trie;
        });
    }

    @Test
    public void testCoveredVisitsRangeTrie() throws InMemoryNDTrie.SpaceExhaustedException
    {
        testCoveredVisitsRange((trie, sComparable, svalue, eComparable, evalue) ->
                               alternateRange(sComparable, of(svalue), eComparable, of(evalue)));
    }

    @Test
    public void testCoveredVisitsRangeApply() throws InMemoryNDTrie.SpaceExhaustedException
    {
        testCoveredVisitsRange((trie, sComparable, svalue, eComparable, evalue) -> {
            trie.apply(alternateRange(sComparable, of(svalue), eComparable, of(evalue)), (x, y) -> y);
            return trie;
        });
    }

    interface CoveredRangeAdder
    {
        NonDeterministicTrie<MergeableInteger> addAndReturnTrie(InMemoryNDTrie<MergeableInteger> trie, ByteComparable sComparable, int svalue, ByteComparable eComparable, int evalue) throws InMemoryNDTrie.SpaceExhaustedException;
    }

    void testCoveredVisitsRange(CoveredRangeAdder adder) throws InMemoryNDTrie.SpaceExhaustedException
    {
        InMemoryNDTrie<MergeableInteger> trie = new InMemoryNDTrie<>(BufferType.ON_HEAP);
        NavigableMap<ByteComparable, MergeableInteger> normals = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        for (int i = 0; i < COUNT; ++i)
        {
            String key = makeSpecKey(rand);
            int value = key.hashCode() & 0xFF; // to make sure value is the same on clash
            normals.put(comparable(key), of(value));
            if (i % 10 != 1)    // leave some out of the trie to also test non-present covered keys
                trie.putRecursive(comparable(key), of(value), (x, y) -> y);
        }

        for (int i = 0; i < Math.max(10, COUNT / 4); ++i)
        {
            String skey = makeSpecKey(rand);
            String ekey = skey.substring(0, rand.nextInt(9)) + makeSpecKey(rand);  // make sure keys share a prefix
            if (skey.compareTo(ekey) > 0)
            {
                String tmp = skey;
                skey = ekey;
                ekey = tmp;
            }
            int svalue = ~(skey.hashCode() & 0xFF);
            int evalue = ~(ekey.hashCode() & 0xFF);
            final ByteComparable sComparable = comparable(skey);
            final ByteComparable eComparable = comparable(ekey);
            NonDeterministicTrie<MergeableInteger> testTrie = adder.addAndReturnTrie(trie, sComparable, svalue, eComparable, evalue);
            checkAlternateCoverage(normals, sComparable, eComparable, testTrie, svalue, evalue);
        }
    }

    private void checkAlternateCoverage(NavigableMap<ByteComparable, MergeableInteger> normals, ByteComparable sComparable, ByteComparable eComparable, NonDeterministicTrie<MergeableInteger> trie, int svalue, int evalue)
    {
        Map<ByteComparable, MergeableInteger> covered = Maps.newHashMap();
        covered.putAll(normals.subMap(sComparable, true, eComparable, true));
        // also test start and end but make sure they are not found in the non-alternate path
        covered.putIfAbsent(sComparable, null);
        covered.putIfAbsent(eComparable, null);
        for (var entry : covered.entrySet())
        {
            NonDeterministicTrieImpl.Cursor<MergeableInteger> c = NonDeterministicTrieImpl.impl(trie).cursor();
            var key = entry.getKey().asComparableBytes(TrieImpl.BYTE_COMPARABLE_VERSION);
            var start = ByteSource.duplicatable(sComparable.asComparableBytes(TrieImpl.BYTE_COMPARABLE_VERSION));
            var end = ByteSource.duplicatable(eComparable.asComparableBytes(TrieImpl.BYTE_COMPARABLE_VERSION));
            boolean foundStart = false;
            boolean foundEnd = false;
            int next;
            int depth = c.depth();
            while (true)
            {
                NonDeterministicTrieImpl.Cursor<MergeableInteger> alt = c.alternateBranch();
                if (alt != null)
                {
                    foundStart = foundStart || start != null && checkMatch(alt.duplicate(), start.duplicate(), svalue);
                    foundEnd = foundEnd || end != null && checkMatch(alt.duplicate(), end.duplicate(), evalue);
                }
                next = key.next();
                int snext = start != null ? start.next() : ByteSource.END_OF_STREAM;
                int enext = end != null ? end.next() : ByteSource.END_OF_STREAM;
                if (snext != next)
                    start = null;
                if (enext != next)
                    end = null;
                if (next == ByteSource.END_OF_STREAM)
                {
                    if (c.content() != null)
                        assertEquals(entry.getValue(), c.content());
                    break;
                }
                c.skipTo(++depth, next);
                if (c.depth() != depth || c.incomingTransition() != next)
                    break;  // key isn't in the trie
            }
            if (!foundStart || !foundEnd)
            {
                System.err.println("Failed to find " + (foundStart ? "" : "start ") + (foundEnd ? "" : "end ") + "for " + asString(entry.getKey()) + " in " + asString(sComparable) + ":" + asString(eComparable));
                System.err.println("Trie section:\n" + trie.subtrie(sComparable, true, eComparable, true).deterministic().dump());
                assertTrue(foundStart);
                assertTrue(foundEnd);
            }
        }
    }

    boolean checkMatch(NonDeterministicTrieImpl.Cursor<MergeableInteger> c, ByteSource key, int value)
    {
        int next = key.next();
        int depth = c.depth();
        while (next != ByteSource.END_OF_STREAM)
        {
            c.skipTo(++depth, next);
            if (c.depth() != depth || c.incomingTransition() != next)
                return false;
            next = key.next();
        }
        return (c.content().value == value);
    }

    private void verifyAlternates(NonDeterministicTrie<MergeableInteger> trie, SortedMap<ByteComparable, MergeableInteger> normals, SortedMap<ByteComparable, MergeableInteger> alternates)
    {
        SortedMap<ByteComparable, MergeableInteger> both = new TreeMap<>(alternates);
        both.putAll(normals);
        ByteComparable left = comparable("3" + makeSpecKey(rand));
        ByteComparable right = comparable("7" + makeSpecKey(rand));

//        System.out.println("Normal:\n" + trie.dump());
//        System.out.println("Merged:\n" + trie.deterministic()(RESOLVER_FIRST).dump());
//        System.out.println("Alt   :\n" + trie.alternateView(RESOLVER_FIRST).dump());

        assertMapEquals(mainBranch(trie).entrySet(), normals.entrySet());
        assertMapEquals(alternatesOnly(trie).entrySet(), alternates.entrySet());
        assertMapEquals(trie.entrySet(), both.entrySet());

        NonDeterministicTrie<MergeableInteger> ix = trie.subtrie(left, right);
        assertMapEquals(mainBranch(ix).entrySet(), normals.subMap(left, right).entrySet());
        assertMapEquals(alternatesOnly(ix).entrySet(), alternates.subMap(left, right).entrySet());
        assertMapEquals(ix.entrySet(), both.subMap(left, right).entrySet());

//        System.out.println("Bounds " + asString(left) + " to " + asString(right));
//        System.out.println("IX Normal:\n" + ix.dump());
//        System.out.println("IX Merged:\n" + ix.deterministic()(RESOLVER_FIRST).dump());
//        System.out.println("IX Alt   :\n" + alternatesOnly(ix).dump());

        assertMapEquals(alternatesOnly(trie)
                            .subtrie(left, right)
                            .entrySet(),
                        alternates.subMap(left, right)
                                  .entrySet());
        assertMapEquals(trie.subtrie(left, right)
                            .entrySet(),
                        both.subMap(left, right)
                            .entrySet());
    }

    String makeSpecKey(Random rand)
    {
        int len = rand.nextInt(10) + 10;
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; ++i)
            b.append((char) (rand.nextInt(10) + '0'));
        return b.toString();
    }

    public Object makeSpec(String s, int alternateAt, int value)
    {
        if (s.isEmpty())
            return of(value);
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
        NavigableMap<String, String> values1 = collectAsStrings(container1);
        NavigableMap<String, String> values2 = collectAsStrings(container2);
        if (values1.equals(values2))
            return;

        // If the maps are not equal, we want to print out the differences in a way that is easy to read.
        final Set<String> allKeys = Sets.union(values1.keySet(), values2.keySet());
        Set<String> keyDifference = allKeys.stream()
                                           .filter(k -> !Objects.equal(values1.get(k), values2.get(k)))
                                           .collect(Collectors.toSet());
        System.err.println("All data");
        dumpDiff(values1, values2, allKeys);
        System.err.println("\nDifferences");
        dumpDiff(values1, values2, keyDifference);
        fail("Maps are not equal at " + keyDifference);
    }

    private static void dumpDiff(NavigableMap<String, String> values1, NavigableMap<String, String> values2, Set<String> set)
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

    private static <T> TreeMap<String, String> collectAsStrings(Iterable<Map.Entry<ByteComparable, T>> container1)
    {
        var map = new TreeMap<String, String>();
        ByteComparable prevKey = null;
        for (var e : container1)
        {
            var key = e.getKey();
            if (prevKey != null && ByteComparable.compare(prevKey, key, VERSION) >= 0)
                fail("Keys are not sorted: " + asString(prevKey) + " >= " + asString(key));
            prevKey = key;
            map.put(asString(key), e.getValue().toString());
        }
        return map;
    }
}
