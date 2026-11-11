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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Test;

import com.googlecode.concurrenttrees.common.Iterables;
import org.apache.cassandra.config.CassandraRelevantProperties;

import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.*;

public class PatternCursorTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    /**
     * Extract the values of the provided trie into a list.
     */
    private static <T> List<T> toList(Trie<T> trie, Direction direction)
    {
        return Iterables.toList(trie.values(direction));
    }

    private static Trie<String> createTestTrie(String... values) throws TrieSpaceExhaustedException
    {
        return TrieUtil.directOrderedTrie(values);
    }

    private void checkPatternMatch(String message, List<String> expected, Trie<String> trie, String pattern)
    {
        // Test all three algorithm choices
        for (int algoChoice = 0; algoChoice < 3; algoChoice++)
        {
            String algoMessage = message + " [algo=" + algoChoice + "]";
            Trie<String> matched = trie.patternMatched(pattern, algoChoice);
            List<String> forwardResult = toList(matched, Direction.FORWARD);
            List<String> reverseResult = toList(matched, Direction.REVERSE);
            List<String> expectedForward = expected.stream()
                                                   .sorted()
                                                   .collect(Collectors.toList());
            List<String> expectedReverse = expected.stream()
                                                   .sorted((a, b) -> b.compareTo(a))
                                                   .collect(Collectors.toList());

            assertEquals(algoMessage + " forward", expectedForward, forwardResult);
            assertEquals(algoMessage + " reverse", expectedReverse, reverseResult);
        }
    }

    @Test
    public void testExactMatch() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "banana", "cherry", "date");

        // Exact match should return only that entry
        checkPatternMatch("exact apple", ImmutableList.of("apple"), trie, "apple");
        checkPatternMatch("exact banana", ImmutableList.of("banana"), trie, "banana");
        checkPatternMatch("exact cherry", ImmutableList.of("cherry"), trie, "cherry");
        checkPatternMatch("exact date", ImmutableList.of("date"), trie, "date");
    }

    @Test
    public void testNoMatch() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "banana", "cherry");

        // Pattern that doesn't match anything
        checkPatternMatch("no match", ImmutableList.of(), trie, "orange");
        checkPatternMatch("no match prefix", ImmutableList.of(), trie, "app");
        checkPatternMatch("no match suffix", ImmutableList.of(), trie, "pple");
    }

    @Test
    public void testSingleCharacterWildcard() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("cat", "cot", "cut", "bat", "hat");

        // ? matches any single character
        checkPatternMatch("c?t", ImmutableList.of("cat", "cot", "cut"), trie, "c?t");
        checkPatternMatch("?at", ImmutableList.of("bat", "cat", "hat"), trie, "?at");
        checkPatternMatch("ca?", ImmutableList.of("cat"), trie, "ca?");
    }

    @Test
    public void testMultipleQuestionMarks() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "adc", "aec", "axc", "abcd");

        checkPatternMatch("a?c", ImmutableList.of("abc", "adc", "aec", "axc"), trie, "a?c");
        checkPatternMatch("??c", ImmutableList.of("abc", "adc", "aec", "axc"), trie, "??c");
        checkPatternMatch("a??", ImmutableList.of("abc", "adc", "aec", "axc"), trie, "a??");
        checkPatternMatch("????", ImmutableList.of("abcd"), trie, "????");
    }

    @Test
    public void testStarWildcard() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "application", "apply", "banana", "band");

        // * matches any sequence of characters (including empty)
        checkPatternMatch("app*", ImmutableList.of("apple", "application", "apply"), trie, "app*");
        checkPatternMatch("*ana", ImmutableList.of("banana"), trie, "*ana");
        checkPatternMatch("ban*", ImmutableList.of("banana", "band"), trie, "ban*");
    }

    @Test
    public void testStarAtStart() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "pineapple", "grapple", "banana");

        checkPatternMatch("*apple", ImmutableList.of("apple", "grapple", "pineapple"), trie, "*apple");
        checkPatternMatch("*ana", ImmutableList.of("banana"), trie, "*ana");
    }

    @Test
    public void testStarAtEnd() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("test", "testing", "tester", "tested", "best");

        checkPatternMatch("test*", ImmutableList.of("test", "tested", "tester", "testing"), trie, "test*");
        checkPatternMatch("tes*", ImmutableList.of("test", "tested", "tester", "testing"), trie, "tes*");
    }

    @Test
    public void testStarInMiddle() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "adc", "abbc", "abbbc", "ac", "axyzc");

        checkPatternMatch("a*c", ImmutableList.of("abbbc", "abbc", "abc", "ac", "adc", "axyzc"), trie, "a*c");
        checkPatternMatch("ab*c", ImmutableList.of("abbbc", "abbc", "abc"), trie, "ab*c");
    }

    @Test
    public void testMultipleStars() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "adc", "abxc", "abyc", "abxyc");

        // Multiple stars should be collapsed to one
        checkPatternMatch("a*c", ImmutableList.of("abc", "abxc", "abxyc", "abyc", "adc"), trie, "a**c");
        checkPatternMatch("a*c", ImmutableList.of("abc", "abxc", "abxyc", "abyc", "adc"), trie, "a***c");
    }

    @Test
    public void testCombinedWildcards() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "axc", "axyc", "abcd", "axyzc");

        checkPatternMatch("a?c", ImmutableList.of("abc", "axc"), trie, "a?c");
        checkPatternMatch("a?*c", ImmutableList.of("abc", "axc", "axyc", "axyzc"), trie, "a?*c");
        checkPatternMatch("a*?c", ImmutableList.of("abc", "axc", "axyc", "axyzc"), trie, "a*?c");
    }

    @Test
    public void testQuestionBeforeStar() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "axc", "axyc", "abcd");

        // Pattern simplification should move ? before *
        checkPatternMatch("a?*", ImmutableList.of("abc", "abcd", "axc", "axyc"), trie, "a?*");
        checkPatternMatch("a*?", ImmutableList.of("abc", "abcd", "axc", "axyc"), trie, "a*?");
    }

    @Test
    public void testMatchAll() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a", "ab", "abc", "xyz");

        // * alone should match everything
        checkPatternMatch("*", ImmutableList.of("a", "ab", "abc", "xyz"), trie, "*");
    }

    @Test
    public void testEmptyPattern() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("", "a", "ab");

        // Empty pattern should match only empty string
        checkPatternMatch("empty", ImmutableList.of(""), trie, "");
    }

    @Test
    public void testEmptyTrie() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie();

        checkPatternMatch("empty trie", ImmutableList.of(), trie, "abc");
        checkPatternMatch("empty trie wildcard", ImmutableList.of(), trie, "a*");
        checkPatternMatch("empty trie star", ImmutableList.of(), trie, "*");
    }

    @Test
    public void testPrefixMatch() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("prefix", "prefixA", "prefixAB", "prefixABC", "other");

        checkPatternMatch("prefix*", ImmutableList.of("prefix", "prefixA", "prefixAB", "prefixABC"), trie, "prefix*");
    }

    @Test
    public void testSuffixMatch() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("suffix", "Asuffix", "ABsuffix", "ABCsuffix", "other");

        checkPatternMatch("*suffix", ImmutableList.of("ABCsuffix", "ABsuffix", "Asuffix", "suffix"), trie, "*suffix");
    }

    @Test
    public void testComplexPattern() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie(
        "test", "testing", "tester", "tested",
        "best", "besting", "resting",
        "contest", "attest"
        );

        checkPatternMatch("?est*",
                          ImmutableList.of("best", "besting", "resting", "test", "tested", "tester", "testing"),
                          trie, "?est*");

        checkPatternMatch("*test*",
                          ImmutableList.of("attest", "contest", "test", "tested", "tester", "testing"),
                          trie, "*test*");

        checkPatternMatch("*est",
                          ImmutableList.of("attest", "best", "contest", "test"),
                          trie, "*est");
    }

    @Test
    public void testSingleCharacterEntries() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a", "b", "c", "d");

        checkPatternMatch("?", ImmutableList.of("a", "b", "c", "d"), trie, "?");
        checkPatternMatch("a", ImmutableList.of("a"), trie, "a");
        checkPatternMatch("*", ImmutableList.of("a", "b", "c", "d"), trie, "*");
    }

    @Test
    public void testLongPatterns() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie(
        "verylongstring",
        "verylongstringwithmore",
        "verylongstringwithevenmorecharacters"
        );

        checkPatternMatch("verylong*",
                          ImmutableList.of("verylongstring", "verylongstringwithevenmorecharacters", "verylongstringwithmore"),
                          trie, "verylong*");

        checkPatternMatch("verylongstring*",
                          ImmutableList.of("verylongstring", "verylongstringwithevenmorecharacters", "verylongstringwithmore"),
                          trie, "verylongstring*");
    }

    @Test
    public void testSpecialCharacters() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a-b", "a_b", "a.b", "a b");

        checkPatternMatch("a?b", ImmutableList.of("a b", "a-b", "a.b", "a_b"), trie, "a?b");
        checkPatternMatch("a*b", ImmutableList.of("a b", "a-b", "a.b", "a_b"), trie, "a*b");
    }

    @Test
    public void testNumericStrings() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("100", "101", "102", "200", "201");

        checkPatternMatch("10?", ImmutableList.of("100", "101", "102"), trie, "10?");
        checkPatternMatch("?0?", ImmutableList.of("100", "101", "102", "200", "201"), trie, "?0?");
        checkPatternMatch("1*", ImmutableList.of("100", "101", "102"), trie, "1*");
    }

    @Test
    public void testMixedLengthStrings() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a", "aa", "aaa", "aaaa", "b", "bb");

        checkPatternMatch("a*", ImmutableList.of("a", "aa", "aaa", "aaaa"), trie, "a*");
        checkPatternMatch("a?", ImmutableList.of("aa"), trie, "a?");
        checkPatternMatch("a??", ImmutableList.of("aaa"), trie, "a??");
        checkPatternMatch("?", ImmutableList.of("a", "b"), trie, "?");
        checkPatternMatch("??", ImmutableList.of("aa", "bb"), trie, "??");
    }

    @Test
    public void testTrailingStarBehavior() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("test", "test/a", "test/a/b", "test/a/b/c", "other");

        // Trailing star should match the prefix and all descendants
        checkPatternMatch("test*",
                          ImmutableList.of("test", "test/a", "test/a/b", "test/a/b/c"),
                          trie, "test*");

        checkPatternMatch("test/*",
                          ImmutableList.of("test/a", "test/a/b", "test/a/b/c"),
                          trie, "test/*");
    }

    @Test
    public void testPatternLongerThanEntries() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a", "ab", "abc");

        // Pattern longer than any entry should match nothing
        checkPatternMatch("abcdef", ImmutableList.of(), trie, "abcdef");
        checkPatternMatch("abc?", ImmutableList.of(), trie, "abc?");
    }

    @Test
    public void testConsecutiveWildcards() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("abc", "axc", "axyc", "axyzc");

        checkPatternMatch("a??c", ImmutableList.of("axyc"), trie, "a??c");
        checkPatternMatch("a???c", ImmutableList.of("axyzc"), trie, "a???c");
        checkPatternMatch("a?*c", ImmutableList.of("abc", "axc", "axyc", "axyzc"), trie, "a?*c");
    }

    @Test
    public void testSkipToWithExactMatch() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "application", "apply", "banana", "band", "cherry");

        // Test subtrie operations on pattern-matched trie with trailing star
        // Note: Patterns with trailing * currently don't properly restrict subtrie bounds
        // This test documents the current behavior
        checkPatternMatch("subtrie app* [application:cherry)",
                          ImmutableList.of("application", "apply"),
                          trie, "app*",
                          TrieUtil.directComparable("application"),
                          TrieUtil.directComparable("cherry"));
    }

    @Test
    public void testSkipToWithWildcards() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("a", "aa", "aaa", "aaaa", "ab", "abc", "abcd", "b", "ba", "baa");

        // Pattern match with trailing star then slice
        // Note: Trailing star patterns currently match beyond subtrie bounds
        checkPatternMatch("a* [aa:ab)",
                          ImmutableList.of("aa", "aaa", "aaaa"),
                          trie, "a*",
                          TrieUtil.directComparable("aa"),
                          TrieUtil.directComparable("ab"));

        checkPatternMatch("a?* [aa:ab)",
                          ImmutableList.of("aa", "aaa", "aaaa"),
                          trie, "a?*",
                          TrieUtil.directComparable("aa"),
                          TrieUtil.directComparable("ab"));
    }

    @Test
    public void testSkipToWithQuestionMark() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("cat", "bat", "hat", "mat", "rat");

        // Pattern ?at then slice from 'c' to 'd' - should only include cat
        checkPatternMatch("?at [c:d)",
                          ImmutableList.of("cat"),
                          trie, "?at",
                          TrieUtil.directComparable("c"),
                          TrieUtil.directComparable("d"));

        // Pattern ?at then slice from 'h' onwards
        checkPatternMatch("?at [h:null)",
                          ImmutableList.of("hat", "mat", "rat"),
                          trie, "?at",
                          TrieUtil.directComparable("h"),
                          null);
    }

    @Test
    public void testSkipToWithComplexPattern() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie(
        "test", "testing", "tester", "tested",
        "best", "besting", "resting",
        "contest", "attest", "fastest"
        );

        // Pattern *test* then slice [contest:fastest) - specific range
        checkPatternMatch("*test* [contest:fastest)",
                          ImmutableList.of("contest"),
                          trie, "*test*",
                          TrieUtil.directComparable("contest"),
                          TrieUtil.directComparable("fastest"));

        checkPatternMatch("?est* [best:fastest)",
                          ImmutableList.of("best", "besting"),
                          trie, "?est*",
                          TrieUtil.directComparable("best"),
                          TrieUtil.directComparable("fastest"));
    }

    @Test
    public void testSkipToEmptyRange() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "application", "apply", "banana");

        // Pattern match then slice to empty range
        checkPatternMatch("app* [z:zz)",
                          ImmutableList.of(),
                          trie, "app*",
                          TrieUtil.directComparable("z"),
                          TrieUtil.directComparable("zz"));
    }

    @Test
    public void testSkipToWithNullBounds() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("apple", "application", "apply", "banana", "band");

        // Pattern match then slice with null bounds (should return all matches)
        checkPatternMatch("app* [null:null)",
                          ImmutableList.of("apple", "application", "apply"),
                          trie, "app*",
                          null,
                          null);

        // Pattern match then slice with null start - [null:b) means < "b", but "banana" and "band" start with "b"
        checkPatternMatch("*an* [null:ban)",
                          ImmutableList.of(),
                          trie, "*an*",
                          null,
                          TrieUtil.directComparable("ban"));

        // Pattern match then slice with null end
        checkPatternMatch("*an* [ban:null)",
                          ImmutableList.of("banana", "band"),
                          trie, "*an*",
                          TrieUtil.directComparable("ban"),
                          null);
    }

    @Test
    public void testSkipToMultipleRanges() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie(
        "a1", "a2", "a3", "b1", "b2", "b3", "c1", "c2", "c3"
        );

        // Pattern a* then slice [a1:a3) - includes a1, a2 but excludes a3
        checkPatternMatch("a* [a1:a3)",
                          ImmutableList.of("a1", "a2"),
                          trie, "a*",
                          TrieUtil.directComparable("a1"),
                          TrieUtil.directComparable("a3"));

        // Pattern ?1 then slice [a:c) - includes a1, b1 but excludes c1
        checkPatternMatch("?1 [a:c)",
                          ImmutableList.of("a1", "b1"),
                          trie, "?1",
                          TrieUtil.directComparable("a"),
                          TrieUtil.directComparable("c"));
    }

    @Test
    public void testSkipToWithTrailingStar() throws TrieSpaceExhaustedException
    {
        Trie<String> trie = createTestTrie("test", "test/a", "test/a/b", "test/a/b/c", "other");

        // Trailing star with slice - test specific range within pattern matches
        checkPatternMatch("test* [test/a:test/a/b)",
                          ImmutableList.of("test/a"),
                          trie, "test*",
                          TrieUtil.directComparable("test/a"),
                          TrieUtil.directComparable("test/a/b"));
    }

    private void checkPatternMatch(String message, List<String> expected, Trie<String> trie, String pattern,
                                   org.apache.cassandra.utils.bytecomparable.ByteComparable left,
                                   org.apache.cassandra.utils.bytecomparable.ByteComparable right)
    {
        // Test all three algorithm choices
        for (int algoChoice = 1; algoChoice <= 3; algoChoice++)
        {
            String algoMessage = message + " [algo=" + algoChoice + "]";
            Trie<String> matched = trie.patternMatched(pattern, algoChoice);
            // Use slice with inclusive left, exclusive right to match subtrie semantics
            Trie<String> sliced = matched.slice(left, true, right, false);

            List<String> forwardResult = toList(sliced, Direction.FORWARD);
            List<String> reverseResult = toList(sliced, Direction.REVERSE);
            List<String> expectedReverse = expected.stream()
                                                   .sorted((a, b) -> b.compareTo(a))
                                                   .collect(Collectors.toList());

            assertEquals(algoMessage + " forward", expected, forwardResult);
            assertEquals(algoMessage + " reverse", expectedReverse, reverseResult);
        }
    }

    @Test
    public void testPattern64BytesExact() throws TrieSpaceExhaustedException
    {
        // Create strings that are exactly 64 bytes long
        String base64 = "a".repeat(64);
        String match64_1 = "a".repeat(63) + "b";
        String match64_2 = "a".repeat(63) + "c";
        String noMatch64 = "b".repeat(64);

        Trie<String> trie = createTestTrie(base64, match64_1, match64_2, noMatch64);

        // Pattern exactly 64 bytes - tests boundary of Pattern64Cursor
        String pattern64 = "a".repeat(63) + "?";
        checkPatternMatch("64 bytes exact",
                          ImmutableList.of(base64, match64_1, match64_2),
                          trie, pattern64);

        // Pattern 64 bytes with trailing star
        String pattern64Star = "a".repeat(63) + "*";
        checkPatternMatch("64 bytes with trailing star",
                          ImmutableList.of(base64, match64_1, match64_2),
                          trie, pattern64Star);
    }

    @Test
    public void testPattern65BytesAndOver() throws TrieSpaceExhaustedException
    {
        // Create strings over 64 bytes - forces use of PatternLongCursor
        String base64 = "a".repeat(64);
        String base65 = "a".repeat(65);
        String match65_1 = "a".repeat(64) + "b";
        String match65_2 = "a".repeat(64) + "c";
        String match70 = "a".repeat(70);
        String noMatch = "b".repeat(65);

        Trie<String> trie = createTestTrie(base64, base65, match65_1, match65_2, match70, noMatch);

        // Pattern 65 bytes - first byte in second long (only PatternLongCursor and PatternCursor support this)
        String pattern65 = "a".repeat(64) + "?";
        checkPatternMatch("65 bytes pattern", ImmutableList.of(base65, match65_1, match65_2), trie, pattern65);

        // Pattern with wildcard at 64th position (boundary)
        String patternBoundary = "a".repeat(63) + "?" + "a";
        checkPatternMatch("wildcard at 64th byte", ImmutableList.of(base65), trie, patternBoundary);

        // Pattern 70 bytes
        String pattern70 = "a".repeat(70);
        checkPatternMatch("70 bytes exact", ImmutableList.of(match70), trie, pattern70);

        // Pattern 65 bytes with trailing star
        String pattern65Star = "a".repeat(65) + "*";
        checkPatternMatch("65 bytes with star", ImmutableList.of(base65, match70), trie, pattern65Star);

        // Pattern 64 bytes with trailing star
        String pattern64Star = "a".repeat(64) + "*";
        checkPatternMatch("64 bytes with star", ImmutableList.of(base64, base65, match65_1, match65_2, match70), trie, pattern64Star);

        // Pattern with wildcard at 64th position (boundary)
        String patternBoundaryStar = "a".repeat(63) + "?" + "*";
        checkPatternMatch("wildcard at 64th byte plus star", ImmutableList.of(base64, base65, match65_1, match65_2, match70), trie, patternBoundaryStar);

        // Pattern with star at 65th position + char
        String patternBoundaryStarA = "a".repeat(63) + "?" + "*" + "a";
        checkPatternMatch("wildcard at 64th byte plus star then A", ImmutableList.of(base65, match70), trie, patternBoundaryStarA);
    }

    @Test
    public void testPatternCarryLogic() throws TrieSpaceExhaustedException
    {
        // Test the carry logic in PatternLongCursor.setNextState
        // Carry happens when a match at position 63 needs to set bit 64 (in next long)

        String match63 = "a".repeat(63);
        String match64 = "a".repeat(64);
        String match65 = "a".repeat(65);
        String match128 = "a".repeat(128);
        String noMatch = "b".repeat(65);

        Trie<String> trie = createTestTrie(match63, match64, match65, match128, noMatch);

        // Pattern that causes carry from first long to second long
        String patternCarry64 = "a".repeat(64);
        checkPatternMatch("carry at 64 bytes",
                          ImmutableList.of(match64),
                          trie, patternCarry64);

        // Pattern with trailing star to match 64+ bytes
        String patternCarry64Star = "a".repeat(64) + "*";
        checkPatternMatch("carry at 64 bytes with star", ImmutableList.of(match64, match65, match128), trie, patternCarry64Star);

        // Pattern with star at position 63 - tests carry with star
        String patternStarCarry = "a".repeat(63) + "*";
        checkPatternMatch("star at 63rd byte with carry",
                          ImmutableList.of(match63, match64, match65, match128),
                          trie, patternStarCarry);

        // Pattern with ? at position 63 followed by more chars
        // This tests carry logic: pattern is 65 bytes, should match only 65-byte strings
        String patternQuestionCarry = "a".repeat(63) + "?" + "a";
        checkPatternMatch("? at 63rd byte with carry", ImmutableList.of(match65), trie, patternQuestionCarry);

        // Pattern with ? at position 64 (tests carry into second long)
        String patternQuestion64 = "a".repeat(64) + "?";
        checkPatternMatch("? at 64th byte", ImmutableList.of(match65), trie, patternQuestion64);
    }

    @Test
    public void testPattern128BytesBoundary() throws TrieSpaceExhaustedException
    {
        // Test patterns at 128 bytes boundary (second long boundary)
        String match127 = "a".repeat(127);
        String match128 = "a".repeat(128);
        String match129 = "a".repeat(129);
        String match192 = "a".repeat(192);

        Trie<String> trie = createTestTrie(match127, match128, match129, match192);

        // Pattern exactly 128 bytes
        String pattern128 = "a".repeat(128);
        checkPatternMatch("128 bytes exact", ImmutableList.of(match128), trie, pattern128);

        // Pattern 128 bytes with trailing star
        String pattern128Star = "a".repeat(128) + "*";
        checkPatternMatch("128 bytes with star", ImmutableList.of(match128, match129, match192), trie, pattern128Star);

        // Pattern with wildcard at 127th position
        String patternBoundary127 = "a".repeat(127) + "?";
        checkPatternMatch("wildcard at 127th byte", ImmutableList.of(match128), trie, patternBoundary127);

        // Pattern with star at 127th position
        String patternStar127 = "a".repeat(127) + "*";
        checkPatternMatch("star at 127th byte", ImmutableList.of(match127, match128, match129, match192), trie, patternStar127);
    }

    @Test
    public void testLongPatternWithWildcardsAtBoundaries() throws TrieSpaceExhaustedException
    {
        // Test wildcards specifically at 64-byte boundaries
        String base = "a".repeat(62) + "xy" + "b".repeat(62) + "zw" + "c".repeat(60);
        String match1 = "a".repeat(62) + "ab" + "b".repeat(62) + "cd" + "c".repeat(60);
        String match2 = "a".repeat(62) + "xy" + "b".repeat(62) + "ab" + "c".repeat(60);

        Trie<String> trie = createTestTrie(base, match1, match2);

        // Wildcard at position 63 (last of first long)
        String pattern63 = "a".repeat(62) + "?" + "?" + "b".repeat(62) + "??" + "c".repeat(60);
        checkPatternMatch("wildcards at boundaries 63-64, 127-128",
                          ImmutableList.of(base, match1, match2),
                          trie, pattern63);

        // Star spanning boundary
        String patternStarBoundary = "a".repeat(62) + "*" + "c".repeat(60);
        checkPatternMatch("star spanning 64-byte boundary",
                          ImmutableList.of(base, match1, match2),
                          trie, patternStarBoundary);
    }

    @Test
    public void testVeryLongPatterns() throws TrieSpaceExhaustedException
    {
        // Test patterns over 192 bytes (3 longs)
        String match200 = "a".repeat(200);
        String match250 = "a".repeat(250);
        String match255 = "a".repeat(254) + "b";
        String noMatch = "b".repeat(200);

        Trie<String> trie = createTestTrie(match200, match250, match255, noMatch);

        // Pattern 200 bytes
        String pattern200 = "a".repeat(200);
        checkPatternMatch("200 bytes exact",
                          ImmutableList.of(match200),
                          trie, pattern200);

        // Pattern 200 bytes with trailing star
        String pattern200Star = "a".repeat(200) + "*";
        checkPatternMatch("200 bytes with star",
                          ImmutableList.of(match200, match250, match255),
                          trie, pattern200Star);

        // Pattern with wildcards in third long
        String pattern195 = "a".repeat(190) + "?????";
        checkPatternMatch("wildcards in third long",
                          ImmutableList.of(),
                          trie, pattern195);

        // Pattern with star at 192nd byte (third long boundary)
        String patternStar192 = "a".repeat(192) + "*";
        checkPatternMatch("star at 192nd byte",
                          ImmutableList.of(match200, match250, match255),
                          trie, patternStar192);

        // Pattern with star at 192nd byte (third long boundary)
        String patternStar192a = "a".repeat(192) + "*a";
        checkPatternMatch("star at 192nd byte plus a",
                          ImmutableList.of(match200, match250),
                          trie, patternStar192a);
    }

    @Test
    public void testCarryThenStar() throws TrieSpaceExhaustedException
    {
        // Test carry logic when star is at boundary and next position is also star
        String match64 = "a".repeat(64);
        String match65 = "a".repeat(65);
        String match100 = "a".repeat(100);
        String matchab64 = "a".repeat(63) + "b".repeat(63) + "a";
        String matchab65 = "a".repeat(64) + "b".repeat(62) + "a";
        String nomatch63 = "a".repeat(63) + "b".repeat(64);
        String nomatch64 = "a".repeat(64) + "b".repeat(63);
        String nomatch65 = "a".repeat(65) + "b".repeat(63);

        Trie<String> trie = createTestTrie(match64, match65, match100, matchab65, matchab64, nomatch64, nomatch63, nomatch65);

        // Multiple stars around boundary
        String patternMultiStar = "a".repeat(62) + "*a*a";
        checkPatternMatch("multiple stars at boundary",
                          ImmutableList.of(match64, matchab65, match65, matchab64, match100),
                          trie, patternMultiStar);

        // Multiple stars around boundary
        String patternMultiStar2 = "a".repeat(63) + "*a*a";
        checkPatternMatch("multiple stars at boundary",
                          ImmutableList.of(matchab65, match65, match100),
                          trie, patternMultiStar2);
    }

    @Test
    public void testMixedWildcardsAcrossBoundaries() throws TrieSpaceExhaustedException
    {
        // Test complex patterns with mixed wildcards across boundaries
        String match = "a".repeat(62) + "bc" + "d".repeat(62) + "ef" + "g".repeat(60);
        String match2 = "a".repeat(62) + "xy" + "d".repeat(62) + "zw" + "g".repeat(60);

        Trie<String> trie = createTestTrie(match, match2);

        // Pattern: 62 a's, then ??, then 62 d's, then ??, then 60 g's
        String pattern = "a".repeat(62) + "??" + "d".repeat(62) + "??" + "g".repeat(60);
        checkPatternMatch("mixed wildcards across boundaries",
            ImmutableList.of(match, match2),
            trie, pattern);

        // Pattern with star before boundary and ? after
        String patternStarQuestion = "a".repeat(62) + "*" + "d".repeat(62) + "?" + "?" + "g".repeat(60);
        checkPatternMatch("star before boundary, ? after",
            ImmutableList.of(match, match2),
            trie, patternStarQuestion);
    }

    @Test
    public void testRandomPatternsWithQuickTheories()
    {
        qt()
            .forAll(
                strings().allPossible().ofLengthBetween(1, 10),
                lists().of(strings().allPossible().ofLengthBetween(0, 100)).ofSizeBetween(5, 20)
            )
            .checkAssert((pattern, testStrings) -> {
                try {
                    // Convert pattern to regex for Java Pattern matching
                    String regex = pattern.replace("?", ".").replace("*", ".*");
                    java.util.regex.Pattern javaPattern = java.util.regex.Pattern.compile("^" + regex + "$");

                    // Create trie with test strings
                    Trie<String> trie = createTestTrie(testStrings.toArray(new String[0]));

                    // Get expected matches using Java Pattern
                    List<String> expected = testStrings.stream()
                                                      .filter(s -> javaPattern.matcher(s).matches())
                                                      .sorted()
                                                      .collect(Collectors.toList());

                    // Test all three algorithms
                    for (int algoChoice = 1; algoChoice <= 3; algoChoice++)
                    {
                        Trie<String> matched = trie.patternMatched(pattern, algoChoice);
                        List<String> actual = toList(matched, Direction.FORWARD);

                        assertEquals("Pattern: " + pattern + " [algo=" + algoChoice + "]",
                                   expected, actual);
                    }
                } catch (TrieSpaceExhaustedException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testRandomLongPatternsWithQuickTheories()
    {
        qt()
            .forAll(
                integers().between(50, 150),
                lists().of(integers().between(50, 150)).ofSizeBetween(5, 15)
            )
            .checkAssert((patternLength, stringLengths) -> {
                try {
                    // Create pattern with 'a's and wildcards at boundaries
                    StringBuilder patternBuilder = new StringBuilder();
                    for (int i = 0; i < patternLength; i++) {
                        if (i == 63 || i == 64 || i == 127 || i == 128) {
                            patternBuilder.append('?');
                        } else {
                            patternBuilder.append('a');
                        }
                    }
                    String pattern = patternBuilder.toString();

                    // Create test strings of various lengths
                    List<String> testStrings = new java.util.ArrayList<>();
                    for (int length : stringLengths) {
                        testStrings.add("a".repeat(length));
                    }

                    // Convert pattern to regex
                    String regex = pattern.replace("?", ".").replace("*", ".*");
                    java.util.regex.Pattern javaPattern = java.util.regex.Pattern.compile("^" + regex + "$");

                    // Create trie
                    Trie<String> trie = createTestTrie(testStrings.toArray(new String[0]));

                    // Get expected matches
                    List<String> expected = testStrings.stream()
                                                      .filter(s -> javaPattern.matcher(s).matches())
                                                      .sorted()
                                                      .distinct()
                                                      .collect(Collectors.toList());

                    // Test algorithm 1 (PatternLongCursor) for long patterns
                    Trie<String> matched = trie.patternMatched(pattern, 1);
                    List<String> actual = toList(matched, Direction.FORWARD);

                    assertEquals("Pattern length: " + patternLength + " [algo=1]",
                               expected, actual);
                } catch (TrieSpaceExhaustedException e) {
                    throw new RuntimeException(e);
                }
            });
    }

    @Test
    public void testRandomBoundaryPatternsWithQuickTheories()
    {
        qt()
            .forAll(
                integers().between(60, 68),
                booleans().all(),
                booleans().all()
            )
            .checkAssert((baseLength, addWildcard, addStar) -> {
                try {
                    // Create pattern around 64-byte boundary
                    StringBuilder patternBuilder = new StringBuilder("a".repeat(baseLength));
                    if (addWildcard) {
                        patternBuilder.append('?');
                    }
                    if (addStar) {
                        patternBuilder.append('*');
                    }
                    String pattern = patternBuilder.toString();

                    // Create test strings
                    List<String> testStrings = Arrays.asList(
                        "a".repeat(baseLength),
                        "a".repeat(baseLength + 1),
                        "a".repeat(baseLength + 2),
                        "a".repeat(baseLength + 10),
                        "a".repeat(baseLength - 1),
                        "a".repeat(64),
                        "a".repeat(65),
                        "a".repeat(128)
                    );

                    // Convert pattern to regex
                    String regex = pattern.replace("?", ".").replace("*", ".*");
                    java.util.regex.Pattern javaPattern = java.util.regex.Pattern.compile("^" + regex + "$");

                    // Create trie
                    Trie<String> trie = createTestTrie(testStrings.toArray(new String[0]));

                    // Get expected matches
                    List<String> expected = testStrings.stream()
                                                      .filter(s -> javaPattern.matcher(s).matches())
                                                      .sorted()
                                                      .distinct()
                                                      .collect(Collectors.toList());

                    // Test appropriate algorithm based on pattern length
                    for (int algoChoice = 0; algoChoice < 3; ++algoChoice)
                    {
                        Trie<String> matched = trie.patternMatched(pattern, algoChoice);
                        List<String> actual = toList(matched, Direction.FORWARD);
                        assertEquals("Pattern: " + pattern + " [algo=" + algoChoice + "]",
                                     expected, actual);
                    }
                } catch (TrieSpaceExhaustedException e) {
                    throw new RuntimeException(e);
                }
            });
    }
}