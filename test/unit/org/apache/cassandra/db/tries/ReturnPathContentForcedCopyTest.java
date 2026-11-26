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

import java.util.List;

import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TestRangeState.toList;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/// A mutation applied with forced copying must not change anything that a reader which started before it can reach.
/// This includes the content on the return path, e.g. the closing boundary of a deletion that covers a branch: a
/// reader that sees the new one together with the old opening boundary is given a range that does not close.
public class ReturnPathContentForcedCopyTest
{
    /// A deletion with the given value that covers the branch of one key: it opens at the key's node and closes on the
    /// return path of that node.
    private static RangeTrie<TestRangeState> branchDeletion(int value)
    {
        return RangeTrie.branch(ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes("2055")),
                                VERSION,
                                TestRangeState.covering(value));
    }

    @Test
    public void testRangeTrie() throws TrieSpaceExhaustedException
    {
        InMemoryRangeTrie<TestRangeState> trie = InMemoryRangeTrie.shortLived(VERSION);
        trie.apply(branchDeletion(1), TestRangeState::upsert, v -> true);
        List<TestRangeState> before = toList(trie, Direction.FORWARD);

        RangeCursor<TestRangeState> reader = trie.cursor(Direction.FORWARD);
        trie.apply(branchDeletion(2), TestRangeState::upsert, v -> true);

        assertNotEquals(before, toList(trie, Direction.FORWARD));
        assertEquals(before, toList(direction -> reader, Direction.FORWARD));
    }

    @Test
    public void testDeletionBranch() throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        applyDeletion(trie, 1);
        List<TestRangeState> before = toList(trie.deletionBranchAtRoot(), Direction.FORWARD);

        RangeCursor<TestRangeState> reader = trie.deletionBranchAtRoot().cursor(Direction.FORWARD);
        applyDeletion(trie, 2);

        assertNotEquals(before, toList(trie.deletionBranchAtRoot(), Direction.FORWARD));
        assertEquals(before, toList(direction -> reader, Direction.FORWARD));
    }

    private static void applyDeletion(InMemoryDeletionAwareTrie<String, TestRangeState> trie, int value) throws TrieSpaceExhaustedException
    {
        trie.apply(DeletionAwareTrie.<String, TestRangeState>deletionBranch(ByteComparable.EMPTY, VERSION, branchDeletion(value)),
                   (existing, update) -> update,
                   TestRangeState::upsert,
                   (existing, deletion) -> existing,
                   (deletion, update) -> update,
                   true,
                   v -> true);
    }
}
