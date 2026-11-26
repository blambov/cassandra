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


package org.apache.cassandra.db.partitions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.TrieTombstoneMarker;
import org.apache.cassandra.db.tries.BasicCursorWalks;
import org.apache.cassandra.db.tries.BasicCursorWalks.Step;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.OnDiskDeletionAwareTrie;
import org.apache.cassandra.db.tries.OnDiskDeletionAwareTrieWriter;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/// Tests that [PartitionUpdateTrieView] presents the same trie as the conversion of the update to trie form.
@RunWith(Parameterized.class)
public class PartitionUpdateTrieViewTest
{
    private static final long NOW = 1_000_000;
    private static final int ITERATIONS = 3000;

    @Parameterized.Parameter(0)
    public PartitionUpdate.Factory factory;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> factories()
    {
        return Arrays.asList(new Object[] { BTreePartitionUpdate.FACTORY },
                             new Object[] { TriePartitionUpdateStage2.FACTORY });
    }

    static TableMetadata metadata;

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        metadata = TableMetadata.builder("ks", "tbl")
                                .addPartitionKeyColumn("pk", Int32Type.instance)
                                .addClusteringColumn("ck1", Int32Type.instance)
                                .addClusteringColumn("ck2", Int32Type.instance)
                                .addRegularColumn("r1", Int32Type.instance)
                                .addRegularColumn("r2", UTF8Type.instance)
                                .addRegularColumn("c1", SetType.getInstance(Int32Type.instance, true))
                                .addRegularColumn("c2", SetType.getInstance(Int32Type.instance, true))
                                .addStaticColumn("s1", Int32Type.instance)
                                .addStaticColumn("ss", SetType.getInstance(Int32Type.instance, true))
                                .build();
        // Note: RandomPartitionUpdates expects this layout (see RandomPartitionUpdates.TABLE_COLUMNS).
    }

    /// Random updates where deletions may have the same deletion times. In such cases the kinds of the deletions
    /// can differ, which the comparison ignores.
    @Test
    public void testRandomUpdates()
    {
        testRandomUpdates(new Random(1), false);
    }

    /// Random updates where all deletion times are distinct. The deletion kinds must match.
    @Test
    public void testRandomUpdatesDistinctDeletions()
    {
        testRandomUpdates(new Random(2), true);
    }

    private void testRandomUpdates(Random rand, boolean distinctDeletions)
    {
        for (int i = 0; i < ITERATIONS; ++i)
        {
            RandomPartitionUpdates generator = new RandomPartitionUpdates(metadata, factory, rand, distinctDeletions);
            PartitionUpdate update = generator.update();
            for (Direction direction : Direction.values())
                compare(update, direction, distinctDeletions);
            compareWritten(update);
        }
    }

    @Test
    public void testExamples()
    {
        RandomPartitionUpdates g = new RandomPartitionUpdates(metadata, factory, new Random(3), true);
        DecoratedKey key = g.key(1);
        ColumnMetadata r1 = metadata.getColumn(ByteBufferUtil.bytes("r1"));
        ColumnMetadata c1 = metadata.getColumn(ByteBufferUtil.bytes("c1"));

        // A row with a complex deletion inside a range tombstone inside a partition deletion.
        PartitionUpdate.Builder builder = factory.builder(metadata, key, metadata.regularAndStaticColumns(), 4);
        builder.addPartitionDeletion(DeletionTime.build(10, NOW));
        builder.add(new RangeTombstone(Slice.make(ClusteringBound.inclusiveStartOf(g.clustering(1)),
                                                  ClusteringBound.inclusiveEndOf(g.clustering(5))),
                                       DeletionTime.build(20, NOW)));
        Row.Builder rb = BTreeRow.sortedBuilder();
        rb.newRow(g.clustering(3, 3));
        rb.addRowDeletion(Row.Deletion.regular(DeletionTime.build(25, NOW)));
        rb.addCell(BufferCell.live(r1, 30, ByteBufferUtil.bytes(1)));
        rb.addComplexDeletion(c1, DeletionTime.build(28, NOW));
        rb.addCell(BufferCell.live(c1, 29, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.create(ByteBufferUtil.bytes(1))));
        rb.addCell(BufferCell.live(c1, 27, ByteBufferUtil.EMPTY_BYTE_BUFFER, CellPath.create(ByteBufferUtil.bytes(2))));
        builder.add(rb.build());
        PartitionUpdate update = builder.build();
        for (Direction direction : Direction.values())
            compare(update, direction, true);
    }

    private void compare(PartitionUpdate update, Direction direction, boolean checkKinds)
    {
        List<Step> expected = BasicCursorWalks.walk(TriePartitionUpdate.asMergableTrie(update).basicCursor(direction));
        List<Step> actual = BasicCursorWalks.walk(PartitionUpdateTrieView.of(update).basicCursor(direction));
        String message = String.format("Mismatch for %s in direction %s:\nexpected\n%s\nactual\n%s",
                                       update, direction, toString(expected), toString(actual));
        assertEquals(message, expected, actual);
        if (checkKinds)
        {
            for (int i = 0; i < expected.size(); ++i)
            {
                TrieTombstoneMarker e = (TrieTombstoneMarker) expected.get(i).deletion;
                TrieTombstoneMarker a = (TrieTombstoneMarker) actual.get(i).deletion;
                if (e != null && (kind(e.leftDeletion()) != kind(a.leftDeletion()) ||
                                  kind(e.rightDeletion()) != kind(a.rightDeletion())))
                    fail("Kind mismatch at " + expected.get(i) + "\n" + message);
            }
        }

        // Taking multiple steps must give the same positions.
        assertEquals(message, actual, BasicCursorWalks.walkMultiple(PartitionUpdateTrieView.of(update).basicCursor(direction)));
    }

    /// The view written in the on-disk trie format, which walks it in reverse and has to place the deletions in a
    /// branch, must read back with the content of the conversion.
    private void compareWritten(PartitionUpdate update)
    {
        // Content is written as its index in this list.
        List<Object> written = new ArrayList<>();
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            long root = OnDiskDeletionAwareTrieWriter.<Object, TrieTombstoneMarker>write(PartitionUpdateTrieView.of(update),
                                                                                         (o, content) -> writeIndexed(o, content, written),
                                                                                         (o, marker) -> writeIndexed(o, marker, written),
                                                                                         out);
            OnDiskDeletionAwareTrie<Object, TrieTombstoneMarker> read =
                OnDiskDeletionAwareTrie.open(out.asNewBuffer(),
                                             (in, length) -> written.get(in.readInt()),
                                             (in, length) -> (TrieTombstoneMarker) written.get(in.readInt()),
                                             TrieBackedPartition.BYTE_COMPARABLE_VERSION,
                                             root);
            assertEquals(update.toString(),
                         BasicCursorWalks.walk(TriePartitionUpdate.asMergableTrie(update).basicCursor(Direction.FORWARD)),
                         BasicCursorWalks.walk(read.basicCursor(Direction.FORWARD)));
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    private static void writeIndexed(DataOutputPlus out, Object content, List<Object> written) throws IOException
    {
        out.writeInt(written.size());
        written.add(content);
    }

    private static TrieTombstoneMarker.Kind kind(TrieTombstoneMarker.Covering covering)
    {
        return covering != null ? covering.deletionKind() : null;
    }

    private static String toString(List<Step> steps)
    {
        return steps.stream().map(Step::toString).collect(Collectors.joining("\n"));
    }
}
