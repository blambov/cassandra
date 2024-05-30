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
import java.util.TreeMap;
import java.util.function.IntPredicate;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

import static org.apache.cassandra.db.tries.TrieUtil.FORWARD_COMPARATOR;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.asString;
import static org.apache.cassandra.db.tries.TrieUtil.comparable;
import static org.apache.cassandra.db.tries.TrieUtil.specifiedNonDeterministicTrie;
import static org.apache.cassandra.db.tries.TrieUtil.toBound;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

        TrieUtil.assertTrieEquals(specified,
                                  ImmutableMap.of(comparable("000"), of(0),
                                                  comparable("001"), of(1),
                                                  comparable("002"), of(2),
                                                  comparable("010"), of(11),
                                                  comparable("011"), of(12),
                                                  comparable("012"), of(13),
                                                  comparable("020"), of(2),
                                                  comparable("021"), of(13),
                                                  comparable("022"), of(18)));
        TrieUtil.assertTrieEquals(specified.mainPathOnly(),
                                  ImmutableMap.of(comparable("000"), of(0),
                                                  comparable("001"), of(1),
                                                  comparable("002"), of(2),
                                                  comparable("020"), of(2),
                                                  comparable("022"), of(4)));
        TrieUtil.assertTrieEquals(specified.deterministic(),
                                  ImmutableMap.of(comparable("000"), of(0),
                                                  comparable("001"), of(1),
                                                  comparable("002"), of(2),
                                                  comparable("010"), of(11),
                                                  comparable("011"), of(12),
                                                  comparable("012"), of(13),
                                                  comparable("020"), of(2),
                                                  comparable("021"), of(13),
                                                  comparable("022"), of(18)));
    }

    @Test
    public void testRandomSingletons()
    {
        List<NonDeterministicTrie<MergeableInteger>> tries = new ArrayList<>();
        NavigableMap<ByteComparable, MergeableInteger> normals = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        NavigableMap<ByteComparable, MergeableInteger> alternates = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
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
        NavigableMap<ByteComparable, MergeableInteger> normals = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        NavigableMap<ByteComparable, MergeableInteger> alternates = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
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
        NavigableMap<ByteComparable, MergeableInteger> normals = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        NavigableMap<ByteComparable, MergeableInteger> alternates = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
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
        NavigableMap<ByteComparable, MergeableInteger> normals = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
        NavigableMap<ByteComparable, MergeableInteger> alternates = new TreeMap<>((x, y) -> ByteComparable.compare(x, y, VERSION));
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
                           (x, y) -> y,
                           Predicates.alwaysFalse()); // no concurrency here
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
            return dir -> new AlternateRangeCursor<>(dir, eComparable, evalue, sComparable, svalue);
        else
            return dir -> new AlternateRangeCursor<>(dir, sComparable, svalue, eComparable, evalue);
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
            // TODO: Test reverse iteration
            NonDeterministicTrieImpl.Cursor<MergeableInteger> c = NonDeterministicTrieImpl.impl(trie).cursor(Direction.FORWARD);
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
                System.err.println("Trie section:\n" + trie.subtrie(toBound(sComparable, false), toBound(eComparable, true)).deterministic().dump());
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

    private void verifyAlternates(NonDeterministicTrie<MergeableInteger> trie,
                                  NavigableMap<ByteComparable, MergeableInteger> normals,
                                  NavigableMap<ByteComparable, MergeableInteger> alternates)
    {
        NavigableMap<ByteComparable, MergeableInteger> both = new TreeMap<>(alternates);
        both.putAll(normals);
        ByteComparable left =  comparable("3" + makeSpecBound(rand));
        ByteComparable right = comparable("7" + makeSpecBound(rand));

//        System.out.println("Normal:\n" + trie.dump());
//        System.out.println("Merged:\n" + trie.deterministic()(RESOLVER_FIRST).dump());
//        System.out.println("Alt   :\n" + trie.alternateView(RESOLVER_FIRST).dump());

        TrieUtil.assertTrieEquals(trie.mainPathOnly(), normals);
        TrieUtil.assertTrieEquals(trie.alternatesOnly(), alternates);
        TrieUtil.assertTrieEquals(trie, both);

        NonDeterministicTrie<MergeableInteger> ix = trie.subtrie(left, right);
        TrieUtil.assertTrieEquals(ix.mainPathOnly(), normals.subMap(left, right));
        TrieUtil.assertTrieEquals(ix.alternatesOnly(), alternates.subMap(left, right));
        TrieUtil.assertTrieEquals(ix, both.subMap(left, right));

//        System.out.println("Bounds " + asString(left) + " to " + asString(right));
//        System.out.println("IX Normal:\n" + ix.dump());
//        System.out.println("IX Merged:\n" + ix.deterministic()(RESOLVER_FIRST).dump());
//        System.out.println("IX Alt   :\n" + alternatesOnly(ix).dump());
    }

    String makeSpecKey(Random rand)
    {
        return makeSpecKey(rand, '1');
    }

    String makeSpecBound(Random rand)
    {
        return makeSpecKey(rand, '0');
    }

    String makeSpecKey(Random rand, char trail)
    {
        int len = rand.nextInt(10) + 10;
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < len; ++i)
            b.append((char) (rand.nextInt(8) + '2'));
        // Add a trailing 1 to ensure prefix-freedom, and to leave room for '0' as the bound position.
        b.append(trail);
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
}
