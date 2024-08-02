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

package org.apache.cassandra.index.sai.disk.v1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.index.sai.IndexContext;
import org.apache.cassandra.index.sai.QueryContext;
import org.apache.cassandra.index.sai.SSTableContext;
import org.apache.cassandra.index.sai.disk.SearchableIndex;
import org.apache.cassandra.index.sai.disk.format.IndexComponents;
import org.apache.cassandra.index.sai.plan.Expression;
import org.apache.cassandra.index.sai.plan.Orderer;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeyWithSortKey;
import org.apache.cassandra.index.sai.utils.RangeConcatIterator;
import org.apache.cassandra.index.sai.utils.RangeIterator;
import org.apache.cassandra.index.sai.utils.TypeUtil;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;

import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.CELL_COUNT;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.COLUMN_NAME;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.COMPONENT_METADATA;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.END_TOKEN;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.MAX_SSTABLE_ROW_ID;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.MAX_TERM;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.MIN_SSTABLE_ROW_ID;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.MIN_TERM;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.START_TOKEN;
import static org.apache.cassandra.index.sai.virtual.SegmentsSystemView.TABLE_NAME;

/**
 * A version specific implementation of the {@link SearchableIndex} where the
 * index is segmented
 */
public class V1SearchableIndex implements SearchableIndex
{
    private final IndexContext indexContext;
    private final ImmutableList<Segment> segments;
    private final List<SegmentMetadata> metadatas;
    private final DecoratedKey minKey;
    private final DecoratedKey maxKey; // in token order
    private final ByteBuffer minTerm;
    private final ByteBuffer maxTerm;
    private final long minSSTableRowId, maxSSTableRowId;
    private final long numRows;
    private PerIndexFiles indexFiles;

    public V1SearchableIndex(SSTableContext sstableContext, IndexComponents.ForRead perIndexComponents)
    {
        this.indexContext = perIndexComponents.context();
        try
        {
            this.indexFiles = new PerIndexFiles(perIndexComponents);

            ImmutableList.Builder<Segment> segmentsBuilder = ImmutableList.builder();

            final MetadataSource source = MetadataSource.loadMetadata(perIndexComponents);

            metadatas = SegmentMetadata.load(source, sstableContext.primaryKeyFactory());

            for (SegmentMetadata metadata : metadatas)
            {
                segmentsBuilder.add(new Segment(indexContext, sstableContext, indexFiles, metadata));
            }

            segments = segmentsBuilder.build();
            assert !segments.isEmpty();

            this.minKey = metadatas.get(0).minKey.partitionKey();
            this.maxKey = metadatas.get(metadatas.size() - 1).maxKey.partitionKey();

            var version = perIndexComponents.version();
            this.minTerm = metadatas.stream().map(m -> m.minTerm).min(TypeUtil.comparator(indexContext.getValidator(), version)).orElse(null);
            this.maxTerm = metadatas.stream().map(m -> m.maxTerm).max(TypeUtil.comparator(indexContext.getValidator(), version)).orElse(null);

            this.numRows = metadatas.stream().mapToLong(m -> m.numRows).sum();

            this.minSSTableRowId = metadatas.get(0).minSSTableRowId;
            this.maxSSTableRowId = metadatas.get(metadatas.size() - 1).maxSSTableRowId;
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(indexFiles);
            FileUtils.closeQuietly(sstableContext);
            throw Throwables.unchecked(t);
        }
    }

    @Override
    public long indexFileCacheSize()
    {
        return segments.stream().mapToLong(Segment::indexFileCacheSize).sum();
    }

    @Override
    public long getRowCount()
    {
        return numRows;
    }

    @Override
    public long minSSTableRowId()
    {
        return minSSTableRowId;
    }

    @Override
    public long maxSSTableRowId()
    {
        return maxSSTableRowId;
    }

    @Override
    public ByteBuffer minTerm()
    {
        return minTerm;
    }

    @Override
    public ByteBuffer maxTerm()
    {
        return maxTerm;
    }

    @Override
    public DecoratedKey minKey()
    {
        return minKey;
    }

    @Override
    public DecoratedKey maxKey()
    {
        return maxKey;
    }

    @Override
    public RangeIterator search(Expression expression,
                                AbstractBounds<PartitionPosition> keyRange,
                                QueryContext context,
                                boolean defer,
                                int limit) throws IOException
    {
        RangeConcatIterator.Builder rangeConcatIteratorBuilder = RangeConcatIterator.builder(segments.size());

        try
        {
            for (Segment segment : segments)
            {
                if (segment.intersects(keyRange))
                {
                    rangeConcatIteratorBuilder.add(segment.search(expression, keyRange, context, defer, limit));
                }
            }

            return rangeConcatIteratorBuilder.build();
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(rangeConcatIteratorBuilder.ranges());
            throw t;
        }
    }

    @Override
    public List<CloseableIterator<PrimaryKeyWithSortKey>> orderBy(Orderer orderer,
                                                                  AbstractBounds<PartitionPosition> keyRange,
                                                                  QueryContext context,
                                                                  int limit,
                                                                  long totalRows) throws IOException
    {
        var iterators = new ArrayList<CloseableIterator<PrimaryKeyWithSortKey>>(segments.size());
        try
        {
            for (Segment segment : segments)
            {
                if (segment.intersects(keyRange))
                {
                    var segmentLimit = segment.proportionalAnnLimit(limit, totalRows);
                    iterators.add(segment.orderBy(orderer, keyRange, context, segmentLimit));
                }
            }

            return iterators;
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(iterators);
            throw t;
        }
    }

    @Override
    public List<CloseableIterator<PrimaryKeyWithSortKey>> orderResultsBy(QueryContext context, List<PrimaryKey> keys, Orderer orderer, int limit, long totalRows) throws IOException
    {
        var results = new ArrayList<CloseableIterator<PrimaryKeyWithSortKey>>(segments.size());
        try
        {
            for (Segment segment : segments)
            {
                // Only pass the primary keys in a segment's range to the segment index.
                var segmentKeys = getKeysInRange(keys, segment);
                var segmentLimit = segment.proportionalAnnLimit(limit, totalRows);
                results.add(segment.orderResultsBy(context, segmentKeys, orderer, segmentLimit));
            }

            return results;
        }
        catch (Throwable t)
        {
            FileUtils.closeQuietly(results);
            throw t;
        }
    }

    @Override
    public List<Segment> getSegments()
    {
        return segments;
    }

    @Override
    public void populateSystemView(SimpleDataSet dataset, SSTableReader sstable)
    {
        Token.TokenFactory tokenFactory = sstable.metadata().partitioner.getTokenFactory();

        for (SegmentMetadata metadata : metadatas)
        {
            String minTerm = indexContext.isVector() ? "N/A" : indexContext.getValidator().getSerializer().deserialize(metadata.minTerm).toString();
            String maxTerm = indexContext.isVector() ? "N/A" : indexContext.getValidator().getSerializer().deserialize(metadata.maxTerm).toString();

            dataset.row(sstable.metadata().keyspace, indexContext.getIndexName(), sstable.getFilename(), metadata.segmentRowIdOffset)
                   .column(TABLE_NAME, sstable.descriptor.cfname)
                   .column(COLUMN_NAME, indexContext.getColumnName())
                   .column(CELL_COUNT, metadata.numRows)
                   .column(MIN_SSTABLE_ROW_ID, metadata.minSSTableRowId)
                   .column(MAX_SSTABLE_ROW_ID, metadata.maxSSTableRowId)
                   .column(START_TOKEN, tokenFactory.toString(metadata.minKey.partitionKey().getToken()))
                   .column(END_TOKEN, tokenFactory.toString(metadata.maxKey.partitionKey().getToken()))
                   .column(MIN_TERM, minTerm)
                   .column(MAX_TERM, maxTerm)
                   .column(COMPONENT_METADATA, metadata.componentMetadatas.asMap());
        }
    }

    /** Create a sublist of the keys within (inclusive) the segment's bounds */
    protected List<PrimaryKey> getKeysInRange(List<PrimaryKey> keys, Segment segment)
    {
        int minIndex = findBoundaryIndex(keys, segment, true);
        int maxIndex = findBoundaryIndex(keys, segment, false);
        return keys.subList(minIndex, maxIndex);
    }

    private int findBoundaryIndex(List<PrimaryKey> keys, Segment segment, boolean findMin)
    {
        // The minKey and maxKey are sometimes just partition keys (not primary keys), so binarySearch
        // may not return the index of the least/greatest match.
        var key = findMin ? segment.metadata.minKey : segment.metadata.maxKey;
        int index = Collections.binarySearch(keys, key);
        if (index < 0)
            return -index - 1;
        if (findMin)
        {
            while (index > 0 && keys.get(index - 1).equals(key))
                index--;
        }
        else
        {
            while (index < keys.size() - 1 && keys.get(index + 1).equals(key))
                index++;
            // We must include the PrimaryKey at the boundary
            index++;
        }
        return index;
    }

    @Override
    public void close() throws IOException
    {
        FileUtils.closeQuietly(indexFiles);
        FileUtils.closeQuietly(segments);
    }
}
