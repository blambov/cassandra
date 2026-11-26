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

import com.google.common.collect.Streams;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.DataPoint.fromList;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.booleans;

/// Tests of the basic cursor interfaces.
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
