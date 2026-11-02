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

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;

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
        Trie<String> matched = trie.patternMatched(pattern);
        List<String> forwardResult = toList(matched, Direction.FORWARD);
        List<String> reverseResult = toList(matched, Direction.REVERSE);
        List<String> expectedForward = expected.stream()
                                               .sorted()
                                               .collect(Collectors.toList());
        List<String> expectedReverse = expected.stream()
                                               .sorted((a, b) -> b.compareTo(a))
                                               .collect(Collectors.toList());

        assertEquals(message + " forward", expectedForward, forwardResult);
        assertEquals(message + " reverse", expectedReverse, reverseResult);
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
        Trie<String> matched = trie.patternMatched(pattern);
        // Use slice with inclusive left, exclusive right to match subtrie semantics
        Trie<String> sliced = matched.slice(left, true, right, false);
        
        List<String> forwardResult = toList(sliced, Direction.FORWARD);
        List<String> reverseResult = toList(sliced, Direction.REVERSE);
        List<String> expectedReverse = expected.stream()
                                               .sorted((a, b) -> b.compareTo(a))
                                               .collect(Collectors.toList());

        assertEquals(message + " forward", expected, forwardResult);
        assertEquals(message + " reverse", expectedReverse, reverseResult);
    }

}
