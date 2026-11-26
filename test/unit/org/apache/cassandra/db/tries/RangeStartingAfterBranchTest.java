/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;

/// A range that excludes its left bound starts after the branch of the bound, on the return path of the bound's node.
/// Applying it to an in-memory trie must not affect the existing content of that branch.
public class RangeStartingAfterBranchTest
{
    private static final String[] KEYS = { "10", "20", "2055", "2077", "30", "50" };
    /// What remains of the keys after deleting (20, 40].
    private static final List<String> EXPECTED = Arrays.asList("10", "20", "2055", "2077", "50");

    private static ByteComparable hex(String s)
    {
        return ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(s));
    }

    private static InMemoryTrie<String> plainTrie() throws TrieSpaceExhaustedException
    {
        InMemoryTrie<String> trie = InMemoryTrie.shortLived(VERSION);
        for (String key : KEYS)
            trie.putRecursive(hex(key), key, (existing, update) -> update);
        return trie;
    }

    /// Deletes the content if the given deletion applies to its position, i.e. is in effect on the position's right.
    private static String delete(String existing, TestRangeState deletion)
    {
        return deletion.rightSide >= 0 ? null : existing;
    }

    private static List<String> values(BaseTrie<String, ?, ?> trie)
    {
        List<String> values = new ArrayList<>();
        trie.forEachValue(values::add);
        return values;
    }

    @Test
    public void testDeleteSet() throws TrieSpaceExhaustedException
    {
        InMemoryTrie<String> trie = plainTrie();
        trie.delete(TrieSet.range(VERSION, hex("20"), false, hex("40"), true));
        assertEquals(EXPECTED, values(trie));
    }

    @Test
    public void testApplyRangeToPlainTrie() throws TrieSpaceExhaustedException
    {
        InMemoryTrie<String> trie = plainTrie();
        trie.rangeMutator(RangeStartingAfterBranchTest::delete, v -> false)
            .apply(RangeTrie.range(hex("20"), false, hex("40"), true, VERSION, TestRangeState.covering(1)));
        assertEquals(EXPECTED, values(trie));
    }

    /// A range that is interrupted for the branch of 20: it stops before the branch and resumes after it.
    @Test
    public void testApplyRangeWithExcludedBranch() throws TrieSpaceExhaustedException
    {
        InMemoryTrie<String> trie = plainTrie();
        RangeTrie<TestRangeState> range = RangeTrie.range(hex("15"), true, hex("20"), false, VERSION, TestRangeState.covering(1))
                                                   .mergeWith(RangeTrie.range(hex("20"), false, hex("40"), true, VERSION, TestRangeState.covering(1)),
                                                              TestRangeState::combine);
        trie.rangeMutator(RangeStartingAfterBranchTest::delete, v -> false).apply(range);
        assertEquals(EXPECTED, values(trie));
    }

    @Test
    public void testApplyToDeletionAwareTrie() throws TrieSpaceExhaustedException
    {
        for (boolean deletionsAtFixedPoints : new boolean[]{ true, false })
        {
            InMemoryDeletionAwareTrie<String, TestRangeState> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
            var mutator = trie.mutator((String existing, String update) -> update,
                                       (TestRangeState existing, TestRangeState update) -> existing == null ? update : TestRangeState.combine(existing, update),
                                       RangeStartingAfterBranchTest::delete,
                                       (TestRangeState deletion, String update) -> update,
                                       deletionsAtFixedPoints,
                                       v -> false);
            for (String key : KEYS)
                mutator.apply(DeletionAwareTrie.singleton(hex(key), VERSION, key));
            mutator.apply(DeletionAwareTrie.deletedRange(ByteComparable.EMPTY, hex("20"), false, hex("40"), true, VERSION, TestRangeState.covering(1)));
            assertEquals(EXPECTED, values(trie));
        }
    }
}
