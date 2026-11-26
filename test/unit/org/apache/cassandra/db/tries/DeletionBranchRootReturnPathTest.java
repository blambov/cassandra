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
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;

/// A deletion branch that covers the whole branch of its node has a boundary on the return path of its root. Views
/// that walk the branch together with the live trie must present that boundary on the return path of the node, i.e.
/// with the node's path, after the node's descendants and before its next sibling.
public class DeletionBranchRootReturnPathTest
{
    private static final String[] LIVE_KEYS = { "10", "1055", "30", "3055" };

    /// Live content on both sides of the given prefix, whose whole branch is deleted.
    private static DeletionAwareTrie<String, TestRangeState> trie(String deletedPrefix)
    {
        return live().mergeWith(deletion(deletedPrefix),
                                (x, y) -> { throw new AssertionError(); },
                                TestRangeState::combine,
                                (marker, content) -> { throw new AssertionError(); },
                                true);
    }

    private static DeletionAwareTrie<String, TestRangeState> live()
    {
        List<DeletionAwareTrie<String, TestRangeState>> tries = new ArrayList<>();
        for (String key : LIVE_KEYS)
            tries.add(DeletionAwareTrie.singleton(hex(key), VERSION, key));
        return DeletionAwareTrie.mergeDistinct(tries);
    }

    private static DeletionAwareTrie<String, TestRangeState> deletion(String deletedPrefix)
    {
        return DeletionAwareTrie.deletionBranch(hex(deletedPrefix),
                                                VERSION,
                                                RangeTrie.branch(ByteComparable.EMPTY, VERSION, TestRangeState.covering(1)));
    }

    private static ByteComparable hex(String s)
    {
        return ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(s));
    }

    private static List<String> mergedEntries(DeletionAwareTrie<String, TestRangeState> trie, Direction direction)
    {
        List<String> entries = new ArrayList<>();
        trie.mergedTrie((live, deletion) -> (live != null ? live : "") + (deletion != null ? "D" + deletion.leftSide + "/" + deletion.rightSide : ""))
            .forEachEntry(direction, (key, value) -> entries.add(key.byteComparableAsString(VERSION) + "=" + value));
        return entries;
    }

    @Test
    public void testMergedView()
    {
        assertEquals(Arrays.asList("10=10", "1055=1055", "20=D-1/1", "20=D1/-1", "30=30", "3055=3055"),
                     mergedEntries(trie("20"), Direction.FORWARD));
        assertEquals(Arrays.asList("30=30", "3055=3055", "20=D1/-1", "20=D-1/1", "10=10", "1055=1055"),
                     mergedEntries(trie("20"), Direction.REVERSE));
    }

    /// The same below a node with live content, where the walk of the branch starts at the node of the live trie.
    @Test
    public void testMergedViewBelowLiveContent()
    {
        assertEquals(Arrays.asList("10=10", "1055=1055", "30=30", "3040=D-1/1", "3040=D1/-1", "3055=3055"),
                     mergedEntries(trie("3040"), Direction.FORWARD));
        assertEquals(Arrays.asList("30=30", "3055=3055", "3040=D1/-1", "3040=D-1/1", "10=10", "1055=1055"),
                     mergedEntries(trie("3040"), Direction.REVERSE));
    }

    /// The basic cursor of the trie presents the same, where the boundaries are at the branch root and its return path.
    @Test
    public void testBasicCursor()
    {
        assertEquals(Arrays.asList("10=10", "1055=1055", "20=D-1/1", "20↑=D1/-1", "30=30", "3055=3055"),
                     basicEntries(trie("20"), Direction.FORWARD));
        assertEquals(Arrays.asList("30=30", "3055=3055", "20=D1/-1", "20↑=D-1/1", "10=10", "1055=1055"),
                     basicEntries(trie("20"), Direction.REVERSE));
    }

    private static List<String> basicEntries(DeletionAwareTrie<String, TestRangeState> trie, Direction direction)
    {
        List<String> entries = new ArrayList<>();
        for (BasicCursorWalks.Step step : BasicCursorWalks.walk(trie.basicCursor(direction)))
        {
            TestRangeState deletion = (TestRangeState) step.deletion;
            assertEquals(deletion != null && !step.onReturnPath, step.isDeletionBranchRoot);
            entries.add(Hex.bytesToHex(step.path) + (step.onReturnPath ? "↑" : "") + "=" +
                        (step.content != null ? step.content : "") +
                        (deletion != null ? "D" + deletion.leftSide + "/" + deletion.rightSide : ""));
        }
        return entries;
    }

    /// A merge applies the deletions of each source to the live content of the other, which must only be done inside
    /// the deletion's branch.
    @Test
    public void testMergeAppliesDeletionOnlyInBranch()
    {
        List<DeletionAwareTrie<String, TestRangeState>> tries = new ArrayList<>();
        tries.add(live());
        for (String key : new String[]{ "20", "2055" })
            tries.add(DeletionAwareTrie.singleton(hex(key), VERSION, key));
        DeletionAwareTrie<String, TestRangeState> merged =
            DeletionAwareTrie.mergeDistinct(tries)
                             .mergeWith(deletion("20"),
                                        (x, y) -> { throw new AssertionError(); },
                                        TestRangeState::combine,
                                        (marker, content) -> null,
                                        true);
        for (Direction direction : Direction.values())
        {
            List<String> entries = new ArrayList<>();
            merged.forEachEntry(direction, (key, value) -> entries.add(key.byteComparableAsString(VERSION) + "=" + value));
            assertEquals(direction.toString(),
                         direction.isForward() ? Arrays.asList("10=10", "1055=1055", "30=30", "3055=3055")
                                               : Arrays.asList("30=30", "3055=3055", "10=10", "1055=1055"),
                         entries);
        }
    }
}
