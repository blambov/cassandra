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
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.DataPoint.toList;
import static org.apache.cassandra.db.tries.DataPoint.verify;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/// Tests for moving deletion branches of an [InMemoryDeletionAwareTrie] up when a deletion is applied at a higher
/// level and deletions are not at fixed points.
public class InMemoryDeletionAwareTrieHoistingTest
{
    @BeforeClass
    public static void enableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(true);
    }

    @Test(timeout = 10000)
    public void testHoistingPastLastTransitionOfSplitNode() throws TrieSpaceExhaustedException
    {
        // Node 40 is split (7 children) and its last child is 0xFF. Each child has live data and a deletion branch
        // rooted at it, and there is more data after 40. A deletion applied at the root must hoist all deletion
        // branches to the root, walking past 40's 0xFF child without wrapping around to its first child.
        String[] children = new String[]{ "10", "20", "30", "40", "50", "60", "ff" };
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = InMemoryDeletionAwareTrie.shortLived(VERSION);
        var mutator = trie.mutator(DataPoint::combineLive,
                                   DataPoint::combineDeletion,
                                   DataPoint::deleteLive,
                                   DataPoint::deleteLive,
                                   false,
                                   v -> false);

        List<DataPoint> expected = new ArrayList<>();
        for (String child : children)
        {
            String prefix = "40" + child;
            mutator.apply(DeletionAwareTrie.singleton(hex(prefix + "15"), VERSION, new LivePoint(hex(prefix + "15"), 10)));
            mutator.apply(DeletionAwareTrie.singleton(hex(prefix + "30"), VERSION, new LivePoint(hex(prefix + "30"), 1)));
            mutator.apply(DeletionAwareTrie.deletedRange(hex(prefix), hex("10"), hex("20"), VERSION, new DeletionMarker(hex(prefix + "10"), 5, 5)));

            expected.add(new DeletionMarker(hex(prefix + "10"), -1, 5));
            expected.add(new LivePoint(hex(prefix + "15"), 10));
            expected.add(new DeletionMarker(hex(prefix + "20"), 5, -1));
            expected.add(new LivePoint(hex(prefix + "30"), 1));
        }
        mutator.apply(DeletionAwareTrie.singleton(hex("4110"), VERSION, new LivePoint(hex("4110"), 1)));
        mutator.apply(DeletionAwareTrie.singleton(hex("4130"), VERSION, new LivePoint(hex("4130"), 1)));

        assertNull(trie.cursor(Direction.FORWARD).deletionBranchCursor(Direction.FORWARD));
        // Deletion at the root, which is after node 40 so that only the hoisting walk goes over it.
        mutator.apply(DeletionAwareTrie.deletedRange(ByteComparable.EMPTY, hex("4100"), hex("4120"), VERSION, new DeletionMarker(hex("4100"), 7, 7)));

        // All boundaries, including the ones previously under 40's children, must now be in the root branch.
        RangeTrie<DeletionMarker> rootBranch = dir -> trie.cursor(dir).deletionBranchCursor(dir);
        assertEquals(children.length * 2 + 2, Iterables.size(rootBranch.values()));

        expected.add(new DeletionMarker(hex("4100"), -1, 7));
        expected.add(new DeletionMarker(hex("4120"), 7, -1));
        expected.add(new LivePoint(hex("4130"), 1));
        verify(expected);

        List<DataPoint> actual = toList(trie);
        assertEquals(expected, actual);
        assertEquals(expected.toString(), actual.toString()); // marker equality does not check positions
        DeletionAwareTestBase.assertDeletionAwareEqual("hoisted", expected, trie);
    }

    /// Hoisting moves the deletion branches below a node to the node itself, so the node's child loses them and its
    /// deletion branch gains them. Concurrent readers must see them all the time, i.e. both changes at once.
    /// The node has live content and a sparse child, which makes it a full prefix node that a mutation could change in
    /// place. Done at the root and at an inner node, which are attached differently.
    @Test
    public void testHoistingIsAtomicForReaders() throws TrieSpaceExhaustedException
    {
        for (String at : new String[]{ "", "40" })
        {
            WalkingOnWriteTrie trie = new WalkingOnWriteTrie();
            var mutator = trie.mutator(DataPoint::combineLive,
                                       DataPoint::combineDeletion,
                                       DataPoint::deleteLive,
                                       DataPoint::deleteLive,
                                       false,
                                       v -> false);
            mutator.apply(DeletionAwareTrie.singleton(hex(at), VERSION, new LivePoint(hex(at), 1)));
            for (String child : new String[]{ "10", "20", "30" })
            {
                String prefix = at + child;
                mutator.apply(DeletionAwareTrie.singleton(hex(prefix + "15"), VERSION, new LivePoint(hex(prefix + "15"), 10)));
                mutator.apply(DeletionAwareTrie.deletedRange(hex(prefix), hex("10"), hex("20"), VERSION, new DeletionMarker(hex(prefix + "10"), 5, 5)));
            }
            List<DataPoint> before = toList(trie);

            // A deletion rooted at the node and after its children, so that it does not change what is there.
            trie.mustStayVisible = before.stream().map(Object::toString).collect(Collectors.toList());
            mutator.apply(DeletionAwareTrie.deletedRange(hex(at), hex("50"), hex("60"), VERSION, new DeletionMarker(hex(at + "50"), 7, 7)));
            trie.mustStayVisible = null;
            assertNull("hoisting to '" + at + "': " + trie.failure, trie.failure);

            List<DataPoint> expected = new ArrayList<>(before);
            expected.add(new DeletionMarker(hex(at + "50"), -1, 7));
            expected.add(new DeletionMarker(hex(at + "60"), 7, -1));
            verify(expected);
            List<DataPoint> actual = toList(trie);
            assertEquals(expected, actual);
            assertEquals(expected.toString(), actual.toString()); // marker equality does not check positions
            DeletionAwareTestBase.assertDeletionAwareEqual("hoisting to '" + at + "'", expected, trie);
        }
    }

    /// A trie that walks itself after every volatile write, i.e. whenever a concurrent reader may first see a new
    /// state, and records the first time a point that must stay visible is missing.
    private static class WalkingOnWriteTrie extends InMemoryDeletionAwareTrie<LivePoint, DeletionMarker>
    {
        List<String> mustStayVisible = null;
        String failure = null;

        WalkingOnWriteTrie()
        {
            super(VERSION, null, BufferType.ON_HEAP, ExpectedLifetime.SHORT, null);
        }

        @Override
        protected void putIntVolatile(int pos, int value)
        {
            super.putIntVolatile(pos, value);
            if (mustStayVisible == null || failure != null)
                return;
            // Marker equality does not check positions, compare the string forms.
            List<String> visible = toList(this).stream().map(Object::toString).collect(Collectors.toList());
            for (String point : mustStayVisible)
                if (!visible.contains(point))
                {
                    failure = point + " is not visible after a write to " + pos + ", a reader sees " + visible;
                    return;
                }
        }
    }

    private static DataPoint livePointInside(ByteComparable position, int timestamp, int covering)
    {
        return new CombinedDataPoint(new LivePoint(position, timestamp), new DeletionMarker(position, covering, covering));
    }

    private static ByteComparable hex(String s)
    {
        return ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(s));
    }
}
