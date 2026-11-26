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
import java.util.stream.Collectors;

import com.google.common.collect.Streams;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.DataPoint.fromList;
import static org.apache.cassandra.db.tries.DataPoint.toList;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.booleans;
import static org.quicktheories.generators.SourceDSL.lists;

/// Tests of the basic cursor interfaces and of applying basic cursors to in-memory deletion-aware tries.
@RunWith(Parameterized.class)
public class BasicCursorTest extends DeletionAwareTestBase
{
    /// The basic cursor of a deletion-aware trie must list what the trie's merged view does, in both directions, and
    /// mark the roots of the trie's deletion branches.
    @Test
    public void testDeletionAwareTrieAsBasicCursor()
    {
        qt().forAll(dataPointListGen(), booleans().all())
            .checkAssert((dataPoints, deletionsAtRoot) -> {
                DeletionAwareTrie<LivePoint, DeletionMarker> trie = fromList(dataPoints, false, deletionsAtRoot);
                for (Direction direction : Direction.values())
                {
                    List<DataPoint> expected = Streams.stream(trie.mergedTrie(DataPoint::resolve).entryIterator(direction))
                                                      .map(en -> en.getValue().remap(en.getKey()))
                                                      .collect(Collectors.toList());
                    List<BasicCursorWalks.Step> steps = BasicCursorWalks.walk(trie.basicCursor(direction));
                    assertEquals(direction.toString(), expected, toDataPoints(steps));
                    assertEquals(direction.toString(), steps, BasicCursorWalks.walkMultiple(trie.basicCursor(direction)));
                    assertEquals(direction.toString(), deletionBranchRoots(trie, direction), deletionBranchRoots(steps));
                }
            });
    }

    /// A deletion branch at the end of a prefix has a basic cursor of its own, which must present what the trie's
    /// deletion-aware cursor does.
    @Test
    public void testDeletionBranchBasicCursor()
    {
        qt().forAll(dataPointListGen(), booleans().all())
            .checkAssert((dataPoints, withPrefix) -> {
                ByteComparable prefix = withPrefix ? at(11) : ByteComparable.EMPTY;
                DeletionAwareTrie<LivePoint, DeletionMarker> trie =
                    DeletionAwareTrie.deletionBranch(prefix, VERSION, fromList(dataPoints, false, true).deletionOnlyTrie());
                assertSameAsDefaultBasicCursor(trie);
            });
    }

    /// The same for a deletion that covers the whole branch, which has a boundary on the return path of the root.
    @Test
    public void testBranchDeletionBasicCursor()
    {
        for (ByteComparable prefix : Arrays.asList(ByteComparable.EMPTY, at(11)))
        {
            RangeTrie<DeletionMarker> deletion = RangeTrie.branch(ByteComparable.EMPTY, VERSION, new DeletionMarker(ByteComparable.EMPTY, 5, 5));
            assertSameAsDefaultBasicCursor(DeletionAwareTrie.deletionBranch(prefix, VERSION, deletion));
        }
    }

    /// The basic cursor of a merge merges the basic cursors of the sources, and must present what the merge's
    /// deletion-aware cursor does.
    @Test
    public void testMergeBasicCursor()
    {
        qt().forAll(dataPointPairGen(), booleans().all())
            .checkAssert((pair, deletionsAtRoot) -> {
                DeletionAwareTrie<LivePoint, DeletionMarker> merged =
                    fromList(pair.get(0), false, deletionsAtRoot).mergeWith(fromList(pair.get(1), false, deletionsAtRoot),
                                                                            LivePoint::combine,
                                                                            DeletionMarker::combine,
                                                                            DeletionMarker::applyTo,
                                                                            deletionsAtRoot);
                assertSameAsDefaultBasicCursor(merged);
            });
    }

    @Test
    public void testCollectionMergeBasicCursor()
    {
        qt().forAll(lists().of(dataPointListGen()).ofSizeBetween(3, 6), booleans().all())
            .checkAssert((lists, deletionsAtRoot) -> {
                List<DeletionAwareTrie<LivePoint, DeletionMarker>> tries = lists.stream()
                                                                                .map(list -> fromList(list, false, deletionsAtRoot))
                                                                                .collect(Collectors.toList());
                assertSameAsDefaultBasicCursor(DeletionAwareTrie.merge(tries,
                                                                       LivePoint::combineCollection,
                                                                       DeletionMarker::combineCollection,
                                                                       DeletionMarker::applyTo,
                                                                       deletionsAtRoot));
            });
    }

    /// Checks that the basic cursor of the given trie, which is specific to the kind of trie, presents the same as
    /// the default one, which is made from the trie's deletion-aware cursor.
    private static void assertSameAsDefaultBasicCursor(DeletionAwareTrie<LivePoint, DeletionMarker> trie)
    {
        for (Direction direction : Direction.values())
        {
            List<BasicCursorWalks.Step> expected = BasicCursorWalks.walk(new DeletionAwareCursor.AsBasic<>(trie.cursor(direction)));
            assertEquals(direction.toString(), expected, BasicCursorWalks.walk(trie.basicCursor(direction)));
            assertEquals(direction.toString(), expected, BasicCursorWalks.walkMultiple(trie.basicCursor(direction)));
        }
    }

    /// Applying a source that is not a trie must have the same effect as merging its content. Done with the deletion
    /// branch of the source rooted at the root of the trie and at the longest common prefix of its deletions, and with
    /// and without deletions at fixed points (which requires all branches to be at the root).
    @Test
    public void testApplyListSource()
    {
        qt().forAll(dataPointPairGen(), booleans().all(), booleans().all(), booleans().all())
            .checkAssert((pair, forceCopy, deletionsAtFixedPoints, rootAtRoot) -> {
                List<DataPoint> existing = pair.get(0);
                List<DataPoint> update = pair.get(1);
                InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = fromList(existing, forceCopy, deletionsAtFixedPoints);
                List<DataPoint> expected = expectedMerge(existing, update);
                var mutator = trie.mutator(DataPoint::combineLive,
                                           DataPoint::combineDeletion,
                                           DataPoint::deleteLive,
                                           DataPoint::deleteLive,
                                           deletionsAtFixedPoints,
                                           v -> forceCopy);
                ByteComparable root = rootAtRoot || deletionsAtFixedPoints ? ByteComparable.EMPTY : deletionsPrefix(update);
                try
                {
                    mutator.apply(dir -> new ListCursor(update, root));
                }
                catch (TrieSpaceExhaustedException e)
                {
                    throw new AssertionError(e);
                }
                assertEquals(expected, toList(trie));
            });
    }

    /// The order in which the transformers are called for a mutation that has deletions and live content at the same
    /// positions, and the keys that the mutator gives them. The content at the root of the deletion branch (here the
    /// root of the trie) is processed before the branch; elsewhere the deletions come first, and what an open deletion
    /// covers is visited on the way to the mutation's next position.
    @Test
    public void testTransformerCalls() throws TrieSpaceExhaustedException
    {
        ByteComparable root = ByteComparable.EMPTY;
        ByteComparable w = before(10);
        ByteComparable x = at(20);
        ByteComparable v = before(22);
        ByteComparable y = at(25);
        ByteComparable z = before(30);
        List<DataPoint> existing = Arrays.asList(new LivePoint(root, 15),
                                                 new DeletionMarker(w, -1, 2),
                                                 new LivePoint(x, 5),
                                                 new DeletionMarker(v, 2, -1),
                                                 new LivePoint(y, 5));
        List<DataPoint> update = Arrays.asList(new CombinedDataPoint(new LivePoint(root, 20), new DeletionMarker(root, -1, 3)),
                                               new CombinedDataPoint(new LivePoint(x, 20), new DeletionMarker(x, 3, 10)),
                                               new DeletionMarker(z, 10, -1));

        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker> trie = fromList(existing, false, true);
        List<String> calls = new ArrayList<>();
        InMemoryDeletionAwareTrie<LivePoint, DeletionMarker>.Mutator<LivePoint, DeletionMarker>[] mutator = new InMemoryDeletionAwareTrie.Mutator[1];
        mutator[0] = trie.mutator((LivePoint e, LivePoint u) -> {
                                      calls.add("liveTransformer " + key(mutator[0]));
                                      return DataPoint.combineLive(e, u);
                                  },
                                  (DeletionMarker e, DeletionMarker u) -> {
                                      calls.add("deletionTransformer " + key(mutator[0]));
                                      // the deletion branch is at the root
                                      assertEquals(key(mutator[0]), Hex.bytesToHex(mutator[0].getDeletionBranchKeyBytes()));
                                      return DataPoint.combineDeletion(e, u);
                                  },
                                  (LivePoint e, DeletionMarker d) -> {
                                      calls.add("existingDeleter " + key(mutator[0]));
                                      return DataPoint.deleteLive(e, d);
                                  },
                                  (DeletionMarker d, LivePoint u) -> {
                                      calls.add("insertedDeleter " + key(mutator[0]));
                                      return DataPoint.deleteLive(d, u);
                                  },
                                  true,
                                  v1 -> false);
        mutator[0].apply(dir -> new ListCursor(update, root));

        assertEquals(Arrays.asList("liveTransformer " + str(root),
                                   "existingDeleter " + str(root),
                                   "deletionTransformer " + str(root),
                                   "deletionTransformer " + str(w),
                                   "deletionTransformer " + str(x),
                                   "existingDeleter " + str(x),
                                   "insertedDeleter " + str(x),
                                   "liveTransformer " + str(x),
                                   "deletionTransformer " + str(v),
                                   "existingDeleter " + str(y),
                                   "deletionTransformer " + str(z)),
                     calls);
        assertEquals(expectedMerge(existing, update), toList(trie));
    }

    /// The result of applying the update to the existing content, taken from the merged view of the two.
    private static List<DataPoint> expectedMerge(List<DataPoint> existing, List<DataPoint> update)
    {
        return toList(fromList(existing).mergeWith(fromList(update),
                                                   LivePoint::combine,
                                                   DeletionMarker::combine,
                                                   DeletionMarker::applyTo,
                                                   false));
    }

    /// The key that the mutator presents to the transformer it is calling.
    private static String key(InMemoryBaseTrie.Mutator<?, ?, ?, ?> mutator)
    {
        byte[] key = mutator.getCurrentKeyBytes();
        assertEquals(key.length, mutator.currentDepth());
        return Hex.bytesToHex(key);
    }

    private static String str(ByteComparable position)
    {
        return Hex.bytesToHex(position.asByteComparableArray(VERSION));
    }

    /// Returns the longest common prefix of the deletions in the given list, or null if there are no deletions.
    private static ByteComparable deletionsPrefix(List<DataPoint> points)
    {
        byte[] prefix = null;
        for (DataPoint point : points)
        {
            if (point.marker() == null)
                continue;
            byte[] key = point.position().asByteComparableArray(VERSION);
            prefix = prefix == null ? key : Arrays.copyOf(prefix, commonPrefixLength(prefix, key));
        }
        return prefix != null ? ByteComparable.preencoded(VERSION, prefix) : null;
    }

    private static int commonPrefixLength(byte[] a, byte[] b)
    {
        int i = 0;
        while (i < a.length && i < b.length && a[i] == b[i])
            ++i;
        return i;
    }

    /// A basic deletion-aware cursor over a sorted list of data points, i.e. one that is not made from a trie. Marks
    /// the given prefix as the root of its deletion branch.
    static class ListCursor implements BasicDeletionAwareCursor<LivePoint, DeletionMarker>
    {
        final List<DataPoint> points;
        final byte[][] keys;
        final byte[] deletionBranchRoot;
        /// The current position is the prefix of length `depth` of the key at `index`.
        int index = 0;
        int depth = 0;
        long position;

        ListCursor(List<DataPoint> points, ByteComparable deletionBranchRoot)
        {
            this.points = points;
            this.keys = points.stream().map(p -> p.position().asByteComparableArray(VERSION)).toArray(byte[][]::new);
            this.deletionBranchRoot = deletionBranchRoot != null ? deletionBranchRoot.asByteComparableArray(VERSION) : null;
            this.position = withFlags(Cursor.rootPosition(Direction.FORWARD));
        }

        @Override
        public long advance()
        {
            if (index >= keys.length)
                return position = Cursor.exhaustedPosition(Direction.FORWARD);

            if (depth < keys[index].length)
                ++depth;
            else if (++index < keys.length)
                depth = commonPrefixLength(keys[index - 1], keys[index]) + 1;
            else
                return position = Cursor.exhaustedPosition(Direction.FORWARD);

            return position = withFlags(Cursor.encode(depth, keys[index][depth - 1] & 0xFF, Direction.FORWARD));
        }

        private long withFlags(long position)
        {
            DataPoint point = pointAtPosition();
            if (point != null && point.live() != null)
                position |= Cursor.MAY_HAVE_CONTENT_BIT;
            if (point != null && point.marker() != null)
                position |= Cursor.MAY_HAVE_DELETION_BIT;
            if (deletionBranchRoot != null
                && depth == deletionBranchRoot.length
                && (index >= keys.length || commonPrefixLength(keys[index], deletionBranchRoot) == depth))
                position |= Cursor.MAY_HAVE_DELETION_BRANCH_BIT;
            return position;
        }

        private DataPoint pointAtPosition()
        {
            return index < keys.length && depth == keys[index].length ? points.get(index) : null;
        }

        @Override
        public long encodedPosition()
        {
            return position;
        }

        @Override
        public LivePoint content()
        {
            DataPoint point = pointAtPosition();
            return point != null ? point.live() : null;
        }

        @Override
        public DeletionMarker deletion()
        {
            DataPoint point = pointAtPosition();
            return point != null ? point.marker() : null;
        }

        @Override
        public ByteComparable.Version byteComparableVersion()
        {
            return VERSION;
        }

        @Override
        public void close()
        {
        }
    }

    /// Converts the steps of a walk to the data points they present.
    static List<DataPoint> toDataPoints(List<BasicCursorWalks.Step> steps)
    {
        List<DataPoint> points = new ArrayList<>();
        for (BasicCursorWalks.Step step : steps)
        {
            DataPoint point = DataPoint.resolve((LivePoint) step.content, (DeletionMarker) step.deletion);
            if (point != null)
                points.add(point.remap(ByteComparable.preencoded(VERSION, step.path)));
        }
        return points;
    }

    private static List<String> deletionBranchRoots(List<BasicCursorWalks.Step> steps)
    {
        return steps.stream()
                    .filter(step -> step.isDeletionBranchRoot)
                    .map(step -> Hex.bytesToHex(step.path))
                    .collect(Collectors.toList());
    }

    /// The paths of the nodes of the trie that have a deletion branch.
    private static List<String> deletionBranchRoots(DeletionAwareTrie<LivePoint, DeletionMarker> trie, Direction direction)
    {
        List<String> roots = new ArrayList<>();
        trie.process(direction, new TrieEntriesWalker.DeletionAware<LivePoint, DeletionMarker, Void>()
        {
            @Override
            public boolean enterDeletionsBranch()
            {
                roots.add(Hex.bytesToHex(keyBytes, 0, keyPos));
                return false;
            }

            @Override
            protected void content(LivePoint content, byte[] bytes, int byteLength)
            {
            }

            @Override
            protected void deletionMarker(DeletionMarker marker, byte[] bytes, int byteLength)
            {
            }

            @Override
            public Void complete()
            {
                return null;
            }
        });
        return roots;
    }
}
