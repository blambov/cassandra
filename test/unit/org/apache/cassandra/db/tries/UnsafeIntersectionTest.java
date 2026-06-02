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
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.utils.bytecomparable.ByteComparable.Preencoded;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UnsafeIntersectionTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    @Test
    public void testRangeForUnsafeIntersection()
    {
        Preencoded[] boundaries = RangesTrieSetTest.toByteComparables("", "", "abc", "abc", "abcde", "abcfg", "abchi", "abchi", "abcj", "abcjk", "abcjk", "abcj", "abc", "abc", "", "");
        BitSet expicitPlaceAfter = BitSet.valueOf(new long[] {0b1111111010100000}); // lsb for boundaries[0], msb for boundaries[size - 1]

        TrieSet set = TrieSet.ranges(VERSION, expicitPlaceAfter, boundaries);
        assertTrue(set.strictlyContains(TrieUtil.directComparable("abcde")));
        assertTrue(set.strictlyContains(TrieUtil.directComparable("abcee")));
        assertTrue(set.strictlyContains(TrieUtil.directComparable("abcf")));
        assertTrue(set.strictlyContains(TrieUtil.directComparable("abcfg")));
        assertTrue(set.strictlyContains(TrieUtil.directComparable("abchi")));
        assertTrue(set.strictlyContains(TrieUtil.directComparable("abcj")));
        assertTrue(set.strictlyContains(TrieUtil.directComparable("abcjk")));
        assertEquals(TrieSet.ContainsResult.PREFIX, set.contains(TrieUtil.directComparable("abch")));
        assertEquals(TrieSet.ContainsResult.PREFIX, set.contains(TrieUtil.directComparable("abcd")));
        assertEquals(TrieSet.ContainsResult.PREFIX, set.contains(TrieUtil.directComparable("abc")));
        assertEquals(TrieSet.ContainsResult.PREFIX, set.contains(TrieUtil.directComparable("ab")));
        assertEquals(TrieSet.ContainsResult.PREFIX, set.contains(TrieUtil.directComparable("a")));

        // Cursor has to visit "" and "abc" on both the descent and ascent paths
        // "[" sorts before letters, "}" after
        List<String> expectedPositions = List.of("[", "a", "ab", "abc[", "abcd", "abcde[", "abcf", "abcfg}",
                                                 "abch", "abchi[", "abchi}", "abcj[", "abcjk}", "abcj}", "abc}", "}");

        List<String> expectedReversePositions = reverseExpectedPositions(expectedPositions);
        verifyVisitedPositions(set.cursor(Direction.FORWARD), expectedPositions, expectedReversePositions, true, "");
        verifyVisitedPositions(set.cursor(Direction.REVERSE), expectedPositions, expectedReversePositions, true, "");
    }

    void verifyVisitedPositions(TrieSetCursor c, List<String> expectedForwardPositions, List<String> expectedReversePositions, boolean takeTailOfRoot, String prefix)
    {
        char[] bytes = new char[100];
        long pos = c.encodedPosition();
        int expectationPos = 0;
        boolean advanced = false;
        List<String> expectedPositions = Cursor.direction(pos).select(expectedForwardPositions, expectedReversePositions);
        expectedPositions = expectedPositions.stream().filter(s -> s.startsWith(prefix)).collect(Collectors.toList());
        String first = expectedPositions.get(0);
        if (first.endsWith("}"))
            expectedPositions.add(0, first.replaceFirst("\\}", ""));
        while (!Cursor.isExhausted(pos))
        {
            String expected;
            if (expectationPos < expectedPositions.size())
                expected = expectedPositions.get(expectationPos);
            else
            {
                // We must stop on return path if interval is open.
                assertTrue(c.precedingIncluded() && Cursor.isOnReturnPath(pos));
                expected = prefix + "}";
            }
            String current = prefix + String.valueOf(bytes, 0, Cursor.depth(pos));

            if (!Cursor.isOnReturnPath(pos))
            {
                if (!expected.endsWith("["))
                    expected += "[";
                assertEquals("prefix: " + prefix, expected, current + '[');

                if (advanced || takeTailOfRoot)
                {
                    verifyVisitedPositions(c.tailCursor(Direction.FORWARD), expectedForwardPositions, expectedReversePositions, advanced, current);
                    verifyVisitedPositions(c.tailCursor(Direction.REVERSE), expectedForwardPositions, expectedReversePositions, advanced, current);
                }
            }
            else
            {
                assertEquals("prefix: " + prefix, expected, current + '}');
            }
            pos = c.advance();
            advanced = true;
            ++expectationPos;
            if (Cursor.depth(pos) > 0)
                bytes[Cursor.depth(pos) - 1] = (char) Cursor.incomingTransition(pos);
        }
        if (expectationPos < expectedPositions.size())
        {
            Assert.fail("Not returned: " + expectedPositions.subList(expectationPos, expectedPositions.size()) + "\nprefix: " + prefix);
        }
    }

    List<String> reverseExpectedPositions(List<String> expectedPositions)
    {
        List<String> res = new ArrayList<>(expectedPositions);

        res.sort((x, y) -> (y.startsWith(x) ? -1 : x.startsWith(y) ? 1 : -x.compareTo(y)));

        res = res.stream()
                 .map(x -> x.replaceFirst("\\}", ".")
                            .replaceFirst("\\[", "}")
                            .replaceFirst("\\.", "["))
                 .collect(Collectors.toList());
        return res;
    }
}
