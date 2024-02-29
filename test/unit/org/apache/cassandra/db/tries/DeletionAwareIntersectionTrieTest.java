///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.cassandra.db.tries;
//
//import java.util.Arrays;
//import java.util.List;
//import java.util.Objects;
//import java.util.stream.Collectors;
//
//import com.google.common.collect.Streams;
//import org.junit.Test;
//
//import org.apache.cassandra.io.compress.BufferType;
//import org.apache.cassandra.utils.bytecomparable.ByteComparable;
//
//import static java.util.Arrays.asList;
//import static org.junit.Assert.assertEquals;
//
//public class DeletionAwareIntersectionTrieTest
//{
//    static final int bitsNeeded = 4;
//    int bits = bitsNeeded;
//
//    class DeletionMarker
//    {
//        final ByteComparable position;
//        final int leftSide;
//        final int rightSide;
//
//        DeletionMarker(int position, int leftSide, int rightSide)
//        {
//            this(of(position), leftSide, rightSide);
//        }
//
//        DeletionMarker(ByteComparable position, int leftSide, int rightSide)
//        {
//            this.position = position;
//            this.leftSide = leftSide;
//            this.rightSide = rightSide;
//        }
//
//
//        @Override
//        public boolean equals(Object o)
//        {
//            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
//            DeletionMarker that = (DeletionMarker) o;
//            return ByteComparable.compare(this.position, that.position, TrieImpl.BYTE_COMPARABLE_VERSION) == 0
//                   && leftSide == that.leftSide
//                   && rightSide == that.rightSide;
//        }
//
//        @Override
//        public int hashCode()
//        {
//            return Objects.hash(position, leftSide, rightSide);
//        }
//
//        @Override
//        public String toString()
//        {
//            return (leftSide >= 0 ? leftSide + "<\"" : "\"") +
//                   position.byteComparableAsString(TrieImpl.BYTE_COMPARABLE_VERSION) +
//                   (rightSide >= 0 ? "\"<" + rightSide : "\"");
//        }
//    }
//
//    DeletionAwareTrie.DeletionHandler<DeletionMarker, DeletionMarker> DELETION_HANDLER =
//    new DeletionAwareTrie.DeletionHandler<>()
//    {
//        @Override
//        public boolean has(DeletionMarker deletionMarker, DeletionAwareTrie.BoundSide side)
//        {
//            switch (side)
//            {
//                case BEFORE:
//                    return deletionMarker.leftSide >= 0;
//                case AFTER:
//                    return deletionMarker.rightSide >= 0;
//                default:
//                    throw new AssertionError();
//            }
//        }
//
//        @Override
//        public DeletionMarker delete(DeletionMarker content, DeletionMarker deletionMarker, DeletionAwareTrie.BoundSide contentRelativeToDeletion)
//        {
//            throw new AssertionError();
//        }
//
//        @Override
//        public DeletionMarker asBound(DeletionMarker deletionMarker, DeletionAwareTrie.BoundSide deletionSide, DeletionAwareTrie.BoundSide targetSide)
//        {
//            int value = deletionSide == DeletionAwareTrie.BoundSide.BEFORE ? deletionMarker.leftSide : deletionMarker.rightSide;
//            if (value < 0)
//                return null;
//            return targetSide == DeletionAwareTrie.BoundSide.BEFORE ? new DeletionMarker(deletionMarker.position, value, -1)
//                                                                    : new DeletionMarker(deletionMarker.position, -1, value);
//        }
//
//        @Override
//        public boolean closes(DeletionMarker deletionMarker, DeletionMarker activeMarker)
//        {
//            if (activeMarker == null)
//                return deletionMarker.leftSide < 0;
//            return deletionMarker.leftSide == activeMarker.rightSide;
//        }
//    };
//
//    class DeletionMarkerTrie implements DeletionAwareTrieImpl<DeletionMarker, DeletionMarker>
//    {
//        final TrieImpl<DeletionMarker> trie;
//
//        DeletionMarkerTrie(TrieImpl<DeletionMarker> trie)
//        {
//            this.trie = trie;
//        }
//
//        @Override
//        public Cursor<DeletionMarker> cursor()
//        {
//            return trie.cursor();
//        }
//
//        @Override
//        public DeletionHandler<DeletionMarker, DeletionMarker> deletionHandler()
//        {
//            return DELETION_HANDLER;
//        }
//    }
//
//    /**
//     * Extract the values of the provided trie into a list.
//     */
//    private List<DeletionMarker> toList(DeletionAwareTrie<DeletionMarker, DeletionMarker> trie)
//    {
//        return Streams.stream(trie.withDeletions().entryIterator())
//                      .map(en -> remap(en.getValue(), en.getKey()))
//                      .collect(Collectors.toList());
//    }
//
//    DeletionMarker remap(DeletionMarker dm, ByteComparable newKey)
//    {
//        return new DeletionMarker(newKey, dm.leftSide, dm.rightSide);
//    }
//
//    private DeletionMarkerTrie fromList(DeletionMarker... list) throws InMemoryDTrie.SpaceExhaustedException
//    {
//        InMemoryDTrie<DeletionMarker> trie = new InMemoryDTrie<>(BufferType.ON_HEAP);
//        for (DeletionMarker i : list)
//        {
//            trie.putAlternativeRecursive(keyOf(i), i, (ex, n) -> n);
//        }
//        return new DeletionMarkerTrie(trie);
//    }
//
//    /** Creates a {@link ByteComparable} for the provided value by splitting the integer in sequences of "bits" bits. */
//    private ByteComparable of(int value)
//    {
//        assert value >= 0 && value <= Byte.MAX_VALUE;
//
//        byte[] splitBytes = new byte[(bitsNeeded + bits - 1) / bits];
//        int pos = 0;
//        int mask = (1 << bits) - 1;
//        for (int i = bitsNeeded - bits; i > 0; i -= bits)
//            splitBytes[pos++] = (byte) ((value >> i) & mask);
//
//        splitBytes[pos] = (byte) (value & mask);
//        return ByteComparable.fixedLength(splitBytes);
//    }
//
//    private ByteComparable keyOf(DeletionMarker marker)
//    {
//        return marker.position;
//    }
//
//    private DeletionMarker from(int where, int value)
//    {
//        return new DeletionMarker(where, -1, value);
//    }
//
//    private DeletionMarker to(int where, int value)
//    {
//        return new DeletionMarker(where, value, -1);
//    }
//
//    private DeletionMarker change(int where, int from, int to)
//    {
//        return new DeletionMarker(where, from, to);
//    }
//
//    @Test
//    public void testSubtrie() throws InMemoryDTrie.SpaceExhaustedException
//    {
//        for (bits = bitsNeeded; bits > 0; --bits)
//        {
//            DeletionMarkerTrie trie = fromList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12));
//
//            testIntersection("all",
//                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)),
//                             trie,
//                             TrieSet.range(null, null));
//            testIntersection("fully covered range",
//                             asList(from(1, 10), to(4, 10)),
//                             trie,
//                             TrieSet.range(of(0), of(5)));
//            testIntersection("fully covered range",
//                             asList(from(6, 11), change(8, 11, 12), to(10, 12)),
//                             trie,
//                             TrieSet.range(of(5), of(13)));
//            testIntersection("matching range",
//                             asList(from(1, 10), to(4, 10)),
//                             trie,
//                             TrieSet.range(of(1), of(4)));
//            testIntersection("touching empty",
//                             asList(),
//                             trie,
//                             TrieSet.range(of(4), of(6)));
//
//            testIntersection("partial left",
//                             asList(from(2, 10), to(4, 10)),
//                             trie,
//                             TrieSet.range(of(2), of(5)));
//            testIntersection("partial left on change",
//                             asList(from(8, 12), to(10, 12)),
//                             trie,
//                             TrieSet.range(of(8), of(12)));
//            testIntersection("partial left with null",
//                             asList(from(9, 12), to(10, 12)),
//                             trie,
//                             TrieSet.range(of(9), null));
//
//
//            testIntersection("partial right",
//                             asList(from(6, 11), to(7, 11)),
//                             trie,
//                             TrieSet.range(of(5), of(7)));
//            testIntersection("partial right on change",
//                             asList(from(6, 11), to(8, 11)),
//                             trie,
//                             TrieSet.range(of(5), of(8)));
//            testIntersection("partial right with null",
//                             asList(from(1, 10), to(2, 10)),
//                             trie,
//                             TrieSet.range(null, of(2)));
//
//            testIntersection("inside range",
//                             asList(from(2, 10), to(3, 10)),
//                             trie,
//                             TrieSet.range(of(2), of(3)));
//            testIntersection("inside with change",
//                             asList(from(7, 11), change(8, 11, 12), to(9, 12)),
//                             trie,
//                             TrieSet.range(of(7), of(9)));
//
//            testIntersection("empty range inside",
//                             asList(),
//                             trie,
//                             TrieSet.range(of(7), of(7)));
//        }
//    }
//
//    @Test
//    public void testRanges() throws InMemoryDTrie.SpaceExhaustedException
//    {
//        for (bits = bitsNeeded; bits > 0; --bits)
//        {
//            DeletionMarkerTrie trie = fromList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12));
//
//            testIntersection("fully covered ranges",
//                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)),
//                             trie,
//                             TrieSet.ranges(of(0), of(5), of(5), of(13)));
//            testIntersection("matching ranges",
//                             asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12)),
//                             trie,
//                             TrieSet.ranges(of(1), of(4), of(6), of(11)));
//            testIntersection("touching empty",
//                             asList(),
//                             trie,
//                             TrieSet.ranges(of(0), of(1), of(4), of(6), of(12), of(15)));
//            testIntersection("partial left",
//                             asList(from(2, 10), to(4, 10), from(9, 12), to(10, 12)),
//                             trie,
//                             TrieSet.ranges(of(2), of(5), of(9), null));
//
//            testIntersection("partial right",
//                             asList(from(1, 10), to(2, 10), from(6, 11), to(7, 11)),
//                             trie,
//                             TrieSet.ranges(null, of(2), of(5), of(7)));
//
//            testIntersection("inside ranges",
//                             asList(from(2, 10), to(3, 10), from(7, 11), change(8, 11, 12), to(9, 12)),
//                             trie,
//                             TrieSet.ranges(of(2), of(3), of(7), of(9)));
//
//            testIntersection("jumping inside",
//                             asList(from(1, 10), to(2, 10), from(3, 10), to(4, 10), from(7, 11), to(8, 11), from(9, 12), to(10, 12)),
//                             trie,
//                             TrieSet.ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10)));
//        }
//    }
//
//    @Test
//    public void testRangeOnSubtrie() throws InMemoryDTrie.SpaceExhaustedException
//    {
//        for (bits = bitsNeeded; bits > 0; --bits)
//        {
//            DeletionMarkerTrie trie = fromList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12), from(13, 13), to(14, 13));
//
//            // non-overlapping
////            testIntersection("", asList(), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(4), of(7)));
//            // touching, i.e. still non-overlapping
//            testIntersection("", asList(), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(3), of(7)));
//            // overlapping 1
//            testIntersection("", asList(from(2, 10), to(3, 10)), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(2), of(7)));
//            // overlapping 2
//            testIntersection("", asList(from(1, 10), to(3, 10)), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(1), of(7)));
//            // covered
//            testIntersection("", asList(from(1, 10), to(3, 10)), trie, TrieSet.range(of(0), of(3)), TrieSet.range(of(0), of(7)));
//            // covered
//            testIntersection("", asList(from(3, 10), to(4, 10), from(6, 11), to(7, 11)), trie, TrieSet.range(of(3), of(7)), TrieSet.range(of(0), of(7)));
//            // covered 2
//            testIntersection("", asList(from(1, 10), to(3, 10)), trie, TrieSet.range(of(1), of(3)), TrieSet.range(of(0), of(7)));
//        }
//    }
//
//    @Test
//    public void testRangesOnRanges() throws InMemoryDTrie.SpaceExhaustedException
//    {
//        for (bits = bitsNeeded; bits > 0; --bits)
//            testIntersections(fromList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12), from(13, 13), to(14, 13)));
//    }
//
//    private void testIntersections(DeletionAwareTrie<DeletionMarker, DeletionMarker> trie)
//    {
//        testIntersection("", asList(from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12), from(13, 13), to(14, 13)), trie);
//
//        TrieSet set1 = TrieSet.ranges(null, of(4), of(5), of(9), of(12), null);
//        TrieSet set2 = TrieSet.ranges(of(2), of(7), of(8), of(10), of(12), of(14));
//        TrieSet set3 = TrieSet.ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10));
//
//        testIntersections(trie, set1, set2, set3);
//
//        testSetAlgebraIntersection(trie);
//    }
//
//    private void testSetAlgebraIntersection(DeletionAwareTrie<DeletionMarker, DeletionMarker> trie)
//    {
//        TrieSet set1 = TrieSet.range(null, of(3))
//                              .union(TrieSet.range(of(2), of(4)))
//                              .union(TrieSet.range(of(5), of(7)))
//                              .union(TrieSet.range(of(7), of(9)))
//                              .union(TrieSet.range(of(14), of(16)))
//                              .union(TrieSet.range(of(12), null));
//        TrieSet set2 = TrieSet.range(of(2), of(7))
//                              .union(TrieSet.ranges(null, of(8), of(10), null).negation())
//                              .union(TrieSet.ranges(of(8), of(10), of(12), of(14)));
//        TrieSet set3 = TrieSet.range(of(1), of(2))
//                              .union(TrieSet.range(of(3), of(4)))
//                              .union(TrieSet.range(of(5), of(6)))
//                              .union(TrieSet.range(of(7), of(8)))
//                              .union(TrieSet.range(of(9), of(10)));
//
//        testIntersections(trie, set1, set2, set3);
//    }
//
//    private void testIntersections(DeletionAwareTrie<DeletionMarker, DeletionMarker> trie, TrieSet set1, TrieSet set2, TrieSet set3)
//    {
//        // set1 = TrieSet.ranges(null, of(4), of(5), of(9), of(12), null);
//        // set2 = TrieSet.ranges(of(2), of(7), of(8), of(10), of(12), of(14));
//        // set3 = TrieSet.ranges(of(1), of(2), of(3), of(4), of(5), of(6), of(7), of(8), of(9), of(10));
//        // from(1, 10), to(4, 10), from(6, 11), change(8, 11, 12), to(10, 12), from(13, 13), to(14, 13)
//        testIntersection("1", asList(from(1, 10), to(4, 10),
//                                     from(6, 11), change(8, 11, 12), to(9, 12),
//                                     from(13, 13), to(14,13)), trie, set1);
//
//        testIntersection("2", asList(from(2, 10), to(4, 10),
//                                     from(6, 11), to(7, 11),
//                                     from(8, 12), to(10, 12),
//                                     from(13, 13), to(14, 13)), trie, set2);
//
//        testIntersection("3", asList(from(1, 10), to(2, 10),
//                                     from(3, 10), to(4, 10),
//                                     from(7, 11), to(8, 11),
//                                     from(9, 12), to(10, 12)), trie, set3);
//
//        testIntersection("12", asList(from(2, 10), to(4, 10),
//                                      from(6, 11), to(7, 11),
//                                      from(8, 12), to(9, 12),
//                                      from(13, 13), to(14, 13)), trie, set1, set2);
//
//        testIntersection("13", asList(from(1, 10), to(2, 10),
//                                      from(3, 10), to(4, 10),
//                                      from(7, 11), to(8, 11)), trie, set1, set3);
//
//        testIntersection("23", asList(from(3, 10), to(4, 10),
//                                      from(9, 12), to(10, 12)), trie, set2, set3);
//
//        testIntersection("123", asList(from(3, 10), to(4, 10)), trie, set1, set2, set3);
//    }
//
//    public void testIntersection(String message, List<DeletionMarker> expected, DeletionAwareTrie<DeletionMarker, DeletionMarker> trie, TrieSet... sets)
//    {
//        // Test that intersecting the given trie with the given sets, in any order, results in the expected list.
//        // Checks both forward and reverse iteration direction.
//        if (sets.length == 0)
//        {
//            assertEquals(message + " forward b" + bits, expected, toList(trie));
//            return;
//        }
//        else
//        {
//            for (int toRemove = 0; toRemove < sets.length; ++toRemove)
//            {
//                TrieSet set = sets[toRemove];
//                testIntersection(message + " " + toRemove, expected,
//                                 trie.intersect(set),
//                                 Arrays.stream(sets)
//                                       .filter(x -> x != set)
//                                       .toArray(TrieSet[]::new)
//                );
//            }
//        }
//    }
//}
