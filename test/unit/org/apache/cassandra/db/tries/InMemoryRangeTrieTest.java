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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InMemoryRangeTrieTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    int delTime;

    TestRangeMarker toMarker(String string)
    {
        return toMarker(string, -1); // Use delTime of -1 to put non-ranged content.
    }

    TestRangeMarker toMarker(String string, int delTime)
    {
        return new TestRangeMarker(TrieUtil.directComparable(string), -1, delTime, -1, true);
    }

    String fromMarker(TestRangeMarker marker)
    {
        if (marker == null)
            return null;
        return new String(marker.position.asByteComparableArray(VERSION), StandardCharsets.UTF_8);
    }

    @Test
    public void testSingle()
    {
        ByteComparable e = TrieUtil.directComparable("test");
        InMemoryRangeTrie<TestRangeMarker> trie = InMemoryRangeTrie.shortLived(VERSION);
        putSimpleResolve(trie, e, toMarker("test"), (x, y) -> y);
        System.out.println("Trie " + trie.dump());
        assertEquals("test", fromMarker(trie.get(e)));
        assertEquals(null, fromMarker(trie.get(TrieUtil.directComparable("teste"))));
    }

    @Test
    public void testSplitMulti()
    {
        testEntries("testing", "tests", "trials", "trial", "aaaa", "aaaab", "abdddd", "abeeee");
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
        InMemoryRangeTrie<TestRangeMarker> trie = InMemoryRangeTrie.shortLived(VERSION);
        for (String test : tests)
        {
            ByteComparable e = ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(test));
            System.out.println("Adding " + asString(e) + ": " + test);
            putSimpleResolve(trie, e, toMarker(test), (x, y) -> y);
        }

        System.out.println(trie.dump());

        for (String test : tests)
            assertEquals(test, fromMarker(trie.get(ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(test)))));

        Arrays.sort(tests);

        int idx = 0;
        for (TestRangeMarker s : trie.values())
        {
            if (!fromMarker(s).equals(tests[idx]))
                throw new AssertionError("" + fromMarker(s) + "!=" + tests[idx]);
            ++idx;
        }
        assertEquals(tests.length, idx);
    }

    @Test
    public void testUpdateContent()
    {
        String[] tests = new String[] {"testing", "tests", "trials", "trial", "testing", "trial", "trial"};
        String[] values = new String[] {"testing", "tests", "trials", "trial", "t2", "x2", "y2"};
        InMemoryRangeTrie<TestRangeMarker> trie = InMemoryRangeTrie.shortLived(VERSION);
        for (int i = 0; i < tests.length; ++i)
        {
            String test = tests[i];
            String v = values[i];
            ByteComparable e = TrieUtil.directComparable(test);
            System.out.println("Adding " + asString(e) + ": " + v);
            putSimpleResolve(trie, e, toMarker(v), (x, y) -> toMarker("" + fromMarker(x) + fromMarker(y)));
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
                         fromMarker(trie.get(TrieUtil.directComparable(test))));
        }
    }

    @Test
    public void testPrefixEvolution()
    {
        testEntries("testing",
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
                    "split");
    }

    @Test
    public void testPrefixUnsafeMulti()
    {
        // Make sure prefixes on inside a multi aren't overwritten by embedded metadata node.

        testEntries("test89012345678901234567890",
                    "test8",
                    "test89",
                    "test890",
                    "test8901",
                    "test89012",
                    "test890123",
                    "test8901234");
    }

    private void testEntries(String... tests)
    {
        for (Function<String, ByteComparable> mapping :
        ImmutableList.<Function<String, ByteComparable>>of(TrieUtil::comparable,
                                                           s -> ByteComparable.preencoded(VERSION, s.getBytes())))
        {
            testEntries(tests, mapping);
        }
    }

    private void testEntriesHex(String[] tests)
    {
        testEntries(tests, s -> ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(s)));
        // Run the other translations just in case.
        testEntries(tests);
    }

    private void testEntries(String[] tests, Function<String, ByteComparable> mapping)

    {
        InMemoryRangeTrie<TestRangeMarker> trie = InMemoryRangeTrie.shortLived(VERSION);
        for (String test : tests)
        {
            ByteComparable e = mapping.apply(test);
            System.out.println("Adding " + asString(e) + ": " + test);
            putSimpleResolve(trie, e, toMarker(test), (x, y) -> y);
            System.out.println("Trie\n" + trie.dump());
        }

        for (String test : tests)
            assertEquals(test, fromMarker(trie.get(mapping.apply(test))));
    }

    static String asString(ByteComparable bc)
    {
        return bc != null ? bc.byteComparableAsString(VERSION) : "null";
    }

    // Tests of nested deletions (delete a, delete aaabc-aaacd, delete aaac-aaad, intersperse walk sitting at aa
    // between the second and third deletion). +simpler version without "delete a".

    // Tests of deletion over a cursor's state (have aabc-aabd, have cursor at aab, delete aa, see if cursor continues
    // ok). Include skip versions. Requires nested deletions.

    @Test
    public void testCursorDeletionBeforeNearest() throws TrieSpaceExhaustedException
    {
        testCursorsWithInterveningDeletions(strings("aaebc", "aaecd"),
                                            "aa", "aaec",
                                            strings("aabc", "aacd"));
    }

    @Test
    public void testCursorDeletionBeforeNearestWithParentDeletion() throws TrieSpaceExhaustedException
    {
        testCursorsWithInterveningDeletions(strings("a", "a", "aaebc", "aaecd"),
                                            "aa", "aaec",
                                            strings("aabc", "aacd"));
    }

    @Test
    public void testCursorDeletionBeforeNearestWithNestedParentDeletion() throws TrieSpaceExhaustedException
    {
        testCursorsWithInterveningDeletions(strings("a", "a", "aaa", "aaa", "aaaaebc", "aaaaecd"),
                                            "aaaa", "aaaaec",
                                            strings("aaaabc", "aaaacd"));
    }

    @Test
    public void testCursorRangeDeletionCoversPosition() throws TrieSpaceExhaustedException
    {
        testCursorsWithInterveningDeletions(strings("aaabc", "aaacd", "bcd", "cde"),
                                            "aaa", "aaacd",
                                            strings("aa", "dd"));
    }

    @Test
    public void testCursorBranchDeletionCoversPosition() throws TrieSpaceExhaustedException
    {
        testCursorsWithInterveningDeletions(strings("aaabc", "aaacd", "bcd", "cde"),
                                            "aaa", "aaacd",
                                            strings("aa", "aa"));
    }

    private String[] strings(String... strings)
    {
        return strings;
    }

    private void testCursorsWithInterveningDeletions(String[] preparations,
                                                     String leftPos,
                                                     String rightPos,
                                                     String[] insertions)
    throws TrieSpaceExhaustedException
    {
        // New deletions supercede old
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, false, 1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, true, 1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, false, 1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, true, 1);

        // New deletions addition to old
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, false, -1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, true, -1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, false, -1);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, true, -1);

        // New deletions group with old
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, false, 0);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.FORWARD, true, 0);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, false, 0);
        testCursorsWithInterveningDeletions(preparations, leftPos, rightPos, insertions, Direction.REVERSE, true, 0);
    }

    private void testCursorsWithInterveningDeletions(String[] preparations,
                                                     String leftPos,
                                                     String rightPos,
                                                     String[] insertions,
                                                     Direction dir,
                                                     boolean useSkip,
                                                     int delTimeIncrease)
    throws TrieSpaceExhaustedException
    {
        // Note: ranges are inserted one pair at a time, with changing delTime.
        delTime = 100;
        if (!dir.isForward() && rightPos.startsWith(leftPos))
        {
            String t = leftPos; leftPos = rightPos; rightPos = t; // swap left and right as prefixes are always before
        }

        InMemoryRangeTrie<TestRangeMarker> trie = InMemoryRangeTrie.shortLived(VERSION);
        insertRanges(trie, preparations, delTimeIncrease);

        final String current = dir.select(leftPos, rightPos);
        RangeCursor<TestRangeMarker> c = trie.cursor(dir);
        TriePathReconstructor paths = new TriePathReconstructor();
        boolean found;
        if (useSkip)
            found = c.descendAlong(TrieUtil.directComparable(current).asPeekableBytes(VERSION));
        else
            found = advanceTo(c, TrieUtil.directComparable(current), paths);

        if (delTimeIncrease > 0)
            assertTrue(found);

        insertRanges(trie, insertions, delTimeIncrease);

        String target = dir.select(rightPos, leftPos);
        if (found)
        {
            if (useSkip)
                found = skipByDifference(c, TrieUtil.directComparable(current), TrieUtil.directComparable(target));
            else
                found = advanceTo(c, TrieUtil.directComparable(target), paths);
        }
        else
        {
            // nested entries may be gone if deleted by parent. If so, just try to skip to target for a new cursor.
            c = trie.cursor(dir);
            paths = new TriePathReconstructor();
            if (useSkip)
                found = c.descendAlong(TrieUtil.directComparable(current).asPeekableBytes(VERSION));
            else
                found = advanceTo(c, TrieUtil.directComparable(current), paths);
        }

        if (delTimeIncrease > 0)
            assertTrue(found);

        if (found)
            while (c.advanceMultiple(null) != -1) {}    // let the verification cursor check the correctness of the iteration
    }

    ByteComparable maybeInvert(ByteComparable bc, Direction dir)
    {
        return dir.isForward() ? bc : InMemoryTriePutTest.invert(bc);
    }

    private boolean advanceTo(RangeCursor<TestRangeMarker> c, ByteComparable target, TriePathReconstructor paths)
    {
        int cmp;
        Direction dir = c.direction();
        while (true)
        {
            cmp = ByteComparable.compare(maybeInvert(target, dir), maybeInvert(ByteComparable.preencoded(VERSION, paths.keyBytes, 0, paths.keyPos), dir), VERSION);
            if (cmp == 0)
                return true;
            if (cmp < 0)
                return false;
            if (c.advance() < 0)
                return false; // exhausted

            paths.resetPathLength(c.depth() - 1);
            paths.addPathByte(c.incomingTransition());
        }
    }

    private boolean skipByDifference(Cursor<?> cursor, ByteComparable a, ByteComparable b)
    {
        ByteSource.Peekable sa = a.asPeekableBytes(VERSION);
        ByteSource.Peekable sb = b.asPeekableBytes(VERSION);
        int depth = 0;
        while (sa.peek() == sb.peek())
        {
            sa.next();
            sb.next();
            ++depth;
        }
        final int nextByte = sb.next();
        int skippedDepth = cursor.skipTo(depth + 1, nextByte);
        if (skippedDepth != depth + 1 || cursor.incomingTransition() != nextByte)
            return false;
        return cursor.descendAlong(sb);
    }

    private void insertRanges(InMemoryRangeTrie<TestRangeMarker> trie, String[] insertions, int delTimeIncrease) throws TrieSpaceExhaustedException
    {
        for (int i = 0; i < insertions.length; i += 2)
        {
            trie.apply(RangeTrie.range(TrieUtil.directComparable(insertions[i]),
                                       TrieUtil.directComparable(insertions[i + 1]),
                                       VERSION,
                                       toMarker(insertions[i], delTime)),
                       (existing, update) -> existing == null ? update : TestRangeMarker.combine(existing, update),
                       delTimeIncrease >= 0 ? Predicates.alwaysFalse() : Predicates.alwaysTrue()); // if we delete covered branches, we should be okay with no force copying
//                       Predicates.alwaysTrue());
            delTime += delTimeIncrease;
        }
        System.out.println("After inserting " + Arrays.toString(insertions) + ":\n" + trie.dump());
    }

    static <M extends RangeMarker<M>> void putSimpleResolve(InMemoryRangeTrie<M> trie,
                                                            ByteComparable key,
                                                            M value,
                                                            Trie.MergeResolver<M> resolver)
    {
        try
        {
            trie.apply(RangeTrie.singleton(key, VERSION, value),
                       (existing, update) -> existing != null ? resolver.resolve(existing, update) : update,
                       Predicates.alwaysFalse());
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw Throwables.propagate(e);
        }
    }
}
