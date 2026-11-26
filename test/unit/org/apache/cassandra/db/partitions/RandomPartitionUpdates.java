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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.TreeSet;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.BufferCell;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

/// Generates random partition updates, with rows, static rows, simple and complex columns, expiring cells, cell
/// tombstones and deletions on all levels.
public class RandomPartitionUpdates
{
    /// The column definitions of a table that this generator can be used with.
    public static final String TABLE_COLUMNS = "pk int, ck1 int, ck2 int, r1 int, r2 text, c1 set<int>, c2 set<int>, " +
                                               "s1 int static, ss set<int> static, PRIMARY KEY (pk, ck1, ck2)";
    static final long NOW = 1_000_000;

    final TableMetadata metadata;
    final PartitionUpdate.Factory factory;
    final Random rand;
    final boolean distinctDeletions;
    long nextDeletionTimestamp = 1000;

    /// Create a generator of random updates for the given table, which must have the [#TABLE_COLUMNS] layout.
    /// If `distinctDeletions` is true, all generated deletions have different timestamps.
    public RandomPartitionUpdates(TableMetadata metadata, PartitionUpdate.Factory factory, Random rand, boolean distinctDeletions)
    {
        this.metadata = metadata;
        this.factory = factory;
        this.rand = rand;
        this.distinctDeletions = distinctDeletions;
    }

    DecoratedKey key(int value)
    {
        return metadata.partitioner.decorateKey(ByteBufferUtil.bytes(value));
    }

    Clustering<?> clustering(int... values)
    {
        ByteBuffer[] buffers = new ByteBuffer[values.length];
        for (int i = 0; i < values.length; ++i)
            buffers[i] = ByteBufferUtil.bytes(values[i]);
        return Clustering.make(buffers);
    }

    long timestamp()
    {
        return rand.nextInt(6);
    }

    DeletionTime deletion()
    {
        long timestamp = distinctDeletions ? nextDeletionTimestamp++ : timestamp();
        return DeletionTime.build(timestamp, NOW - rand.nextInt(2));
    }

    public PartitionUpdate update()
    {
        PartitionUpdate.Builder builder = factory.builder(metadata, key(rand.nextInt(5)), metadata.regularAndStaticColumns(), 4);
        if (rand.nextInt(5) == 0)
            builder.addPartitionDeletion(deletion());
        if (rand.nextInt(3) == 0)
            builder.add(row(Clustering.STATIC_CLUSTERING));

        TreeSet<Integer> clusterings = new TreeSet<>();
        int rowCount = rand.nextInt(6);
        for (int i = 0; i < rowCount; ++i)
            clusterings.add(rand.nextInt(16));
        for (int c : clusterings)
            builder.add(row(clustering(c / 4, c % 4)));

        int rangeCount = rand.nextInt(3);
        for (int i = 0; i < rangeCount; ++i)
            builder.add(new RangeTombstone(slice(), deletion()));
        return builder.build();
    }

    Slice slice()
    {
        while (true)
        {
            ClusteringBound<?> start = bound(true);
            ClusteringBound<?> end = bound(false);
            if (metadata.comparator.compare(start, end) < 0)
                return Slice.make(start, end);
        }
    }

    ClusteringBound<?> bound(boolean isStart)
    {
        int kind = rand.nextInt(6);
        if (kind == 0)
            return isStart ? ClusteringBound.BOTTOM : ClusteringBound.TOP;
        Clustering<?> prefix = kind < 3 ? clustering(rand.nextInt(4)) : clustering(rand.nextInt(4), rand.nextInt(4));
        boolean inclusive = rand.nextBoolean();
        return isStart ? (inclusive ? ClusteringBound.inclusiveStartOf(prefix) : ClusteringBound.exclusiveStartOf(prefix))
                       : (inclusive ? ClusteringBound.inclusiveEndOf(prefix) : ClusteringBound.exclusiveEndOf(prefix));
    }

    Row row(Clustering<?> clustering)
    {
        boolean isStatic = clustering == Clustering.STATIC_CLUSTERING;
        Row.Builder builder = BTreeRow.sortedBuilder();
        builder.newRow(clustering);
        boolean empty = true;
        if (!isStatic && rand.nextBoolean())
        {
            builder.addPrimaryKeyLivenessInfo(rand.nextInt(4) == 0
                                              ? LivenessInfo.expiring(timestamp(), 100, NOW)
                                              : LivenessInfo.create(timestamp(), NOW));
            empty = false;
        }
        if (!isStatic && rand.nextInt(4) == 0)
        {
            builder.addRowDeletion(Row.Deletion.regular(deletion()));
            empty = false;
        }
        for (ColumnMetadata column : isStatic ? metadata.staticColumns() : metadata.regularColumns())
        {
            if (rand.nextBoolean())
                continue;
            if (column.isSimple())
            {
                builder.addCell(cell(column, null));
                empty = false;
            }
            else
            {
                if (rand.nextInt(3) == 0)
                {
                    builder.addComplexDeletion(column, deletion());
                    empty = false;
                }
                TreeSet<Integer> paths = new TreeSet<>();
                int count = rand.nextInt(4);
                for (int i = 0; i < count; ++i)
                    paths.add(rand.nextInt(8));
                for (int path : paths)
                {
                    builder.addCell(cell(column, CellPath.create(ByteBufferUtil.bytes(path))));
                    empty = false;
                }
            }
        }
        if (empty)
            builder.addPrimaryKeyLivenessInfo(LivenessInfo.create(timestamp(), NOW));
        return builder.build();
    }

    Cell<?> cell(ColumnMetadata column, CellPath path)
    {
        ByteBuffer value = path != null ? ByteBufferUtil.EMPTY_BYTE_BUFFER
                                        : column.type.fromString(column.type == UTF8Type.instance ? "v" + rand.nextInt(10)
                                                                                                : Integer.toString(rand.nextInt(10)));
        switch (rand.nextInt(5))
        {
            case 0:
                return BufferCell.tombstone(column, timestamp(), NOW, path);
            case 1:
                return BufferCell.expiring(column, timestamp(), 100, NOW, value, path);
            default:
                return BufferCell.live(column, timestamp(), value, path);
        }
    }
}
