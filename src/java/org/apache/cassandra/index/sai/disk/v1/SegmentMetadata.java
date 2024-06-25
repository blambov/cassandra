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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.index.sai.disk.PostingList;
import org.apache.cassandra.index.sai.disk.format.IndexComponentType;
import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Multiple {@link SegmentMetadata} are stored in {@link IndexComponentType#META} file, each corresponds to an on-disk
 * index segment.
 */
public class SegmentMetadata implements Comparable<SegmentMetadata>
{
    private static final String NAME = "SegmentMetadata";

    /**
     * Used to retrieve sstableRowId which equals to offset plus segmentRowId.
     */
    public final long segmentRowIdOffset;

    /**
     * Min and max sstable rowId in current segment.
     *
     * For index generated by compaction, minSSTableRowId is the same as segmentRowIdOffset.
     * But for flush, segmentRowIdOffset is taken from previous segment's maxSSTableRowId.
     */
    public final long minSSTableRowId;
    public final long maxSSTableRowId;

    /**
     * number of indexed rows (aka. pair of term and segmentRowId) in current segment
     */
    public final long numRows;

    /**
     * Ordered by their token position in current segment
     */
    public final PrimaryKey minKey;
    public final PrimaryKey maxKey;

    /**
     * Minimum and maximum indexed column value ordered by its {@link org.apache.cassandra.db.marshal.AbstractType}.
     */
    public final ByteBuffer minTerm;
    public final ByteBuffer maxTerm;

    /**
     * Root, offset, length for each index structure in the segment.
     *
     * Note: postings block offsets are stored in terms dictionary, no need to worry about its root.
     */
    public final ComponentMetadataMap componentMetadatas;

    public SegmentMetadata(long segmentRowIdOffset,
                           long numRows,
                           long minSSTableRowId,
                           long maxSSTableRowId,
                           PrimaryKey minKey,
                           PrimaryKey maxKey,
                           ByteBuffer minTerm,
                           ByteBuffer maxTerm,
                           ComponentMetadataMap componentMetadatas)
    {
        assert numRows < Integer.MAX_VALUE;
        Objects.requireNonNull(minKey);
        Objects.requireNonNull(maxKey);
        Objects.requireNonNull(minTerm);
        Objects.requireNonNull(maxTerm);

        this.segmentRowIdOffset = segmentRowIdOffset;
        this.minSSTableRowId = minSSTableRowId;
        this.maxSSTableRowId = maxSSTableRowId;
        this.numRows = numRows;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.minTerm = minTerm;
        this.maxTerm = maxTerm;
        this.componentMetadatas = componentMetadatas;
    }

    private static final Logger logger = LoggerFactory.getLogger(SegmentMetadata.class);

    @SuppressWarnings("resource")
    private SegmentMetadata(IndexInput input, PrimaryKey.Factory primaryKeyFactory) throws IOException
    {
        this.segmentRowIdOffset = input.readLong();
        this.numRows = input.readLong();
        this.minSSTableRowId = input.readLong();
        this.maxSSTableRowId = input.readLong();
        this.minKey = primaryKeyFactory.createPartitionKeyOnly(DatabaseDescriptor.getPartitioner().decorateKey(readBytes(input)));
        this.maxKey = primaryKeyFactory.createPartitionKeyOnly(DatabaseDescriptor.getPartitioner().decorateKey(readBytes(input)));
        this.minTerm = readBytes(input);
        this.maxTerm = readBytes(input);
        this.componentMetadatas = new SegmentMetadata.ComponentMetadataMap(input);
    }

    @SuppressWarnings("resource")
    public static List<SegmentMetadata> load(MetadataSource source, PrimaryKey.Factory primaryKeyFactory) throws IOException
    {
        IndexInput input = source.get(NAME);

        int segmentCount = input.readVInt();

        List<SegmentMetadata> segmentMetadata = new ArrayList<>(segmentCount);

        for (int i = 0; i < segmentCount; i++)
        {
            segmentMetadata.add(new SegmentMetadata(input, primaryKeyFactory));
        }

        return segmentMetadata;
    }

    /**
     * Writes disk metadata for the given segment list.
     */
    @SuppressWarnings("resource")
    public static void write(MetadataWriter writer, List<SegmentMetadata> segments) throws IOException
    {
        try (IndexOutput output = writer.builder(NAME))
        {
            output.writeVInt(segments.size());

            for (SegmentMetadata metadata : segments)
            {
                output.writeLong(metadata.segmentRowIdOffset);
                output.writeLong(metadata.numRows);
                output.writeLong(metadata.minSSTableRowId);
                output.writeLong(metadata.maxSSTableRowId);

                Stream.of(metadata.minKey.partitionKey().getKey(),
                          metadata.maxKey.partitionKey().getKey(),
                          metadata.minTerm, metadata.maxTerm).forEach(bb -> writeBytes(bb, output));

                metadata.componentMetadatas.write(output);
            }
        }
    }

    @Override
    public int compareTo(SegmentMetadata other)
    {
        return Long.compare(this.segmentRowIdOffset, other.segmentRowIdOffset);
    }

    @Override
    public String toString()
    {
        return "SegmentMetadata{" +
               "segmentRowIdOffset=" + segmentRowIdOffset +
               ", minSSTableRowId=" + minSSTableRowId +
               ", maxSSTableRowId=" + maxSSTableRowId +
               ", numRows=" + numRows +
               ", componentMetadatas=" + componentMetadatas +
               '}';
    }

    private static ByteBuffer readBytes(IndexInput input) throws IOException
    {
        int len = input.readVInt();
        byte[] bytes = new byte[len];
        input.readBytes(bytes, 0, len);
        return ByteBuffer.wrap(bytes);
    }

    private static void writeBytes(ByteBuffer buf, IndexOutput out)
    {
        try
        {
            byte[] bytes = ByteBufferUtil.getArray(buf);
            out.writeVInt(bytes.length);
            out.writeBytes(bytes, 0, bytes.length);
        }
        catch (IOException ioe)
        {
            throw new RuntimeException(ioe);
        }
    }

    long getIndexRoot(IndexComponentType indexComponentType)
    {
        return componentMetadatas.get(indexComponentType).root;
    }

    public int toSegmentRowId(long sstableRowId)
    {
        int segmentRowId = Math.toIntExact(sstableRowId - segmentRowIdOffset);

        if (segmentRowId == PostingList.END_OF_STREAM)
            throw new IllegalArgumentException("Illegal segment row id: END_OF_STREAM found");

        return segmentRowId;
    }

    public static class ComponentMetadataMap
    {
        private final Map<IndexComponentType, ComponentMetadata> metas = new HashMap<>();

        ComponentMetadataMap(IndexInput input) throws IOException
        {
            int size = input.readInt();

            for (int i = 0; i < size; i++)
            {
                metas.put(IndexComponentType.valueOf(input.readString()), new ComponentMetadata(input));
            }
        }

        public ComponentMetadataMap()
        {
        }

        public void put(IndexComponentType indexComponentType, long root, long offset, long length)
        {
            metas.put(indexComponentType, new ComponentMetadata(root, offset, length));
        }

        public void put(IndexComponentType indexComponentType, long root, long offset, long length, Map<String, String> additionalMap)
        {
            metas.put(indexComponentType, new ComponentMetadata(root, offset, length, additionalMap));
        }

        private void write(IndexOutput output) throws IOException
        {
            output.writeInt(metas.size());

            for (Map.Entry<IndexComponentType, ComponentMetadata> entry : metas.entrySet())
            {
                output.writeString(entry.getKey().name());
                entry.getValue().write(output);
            }
        }

        public ComponentMetadata get(IndexComponentType indexComponentType)
        {
            if (!metas.containsKey(indexComponentType))
                throw new IllegalArgumentException(indexComponentType + " ComponentMetadata not found");

            return metas.get(indexComponentType);
        }

        public Map<String, Map<String, String>> asMap()
        {
            Map<String, Map<String, String>> metaAttributes = new HashMap<>();

            for (Map.Entry<IndexComponentType, ComponentMetadata> entry : metas.entrySet())
            {
                String name = entry.getKey().name();
                ComponentMetadata metadata = entry.getValue();

                Map<String, String> componentAttributes = metadata.asMap();

                assert !metaAttributes.containsKey(name) : "Found duplicate index type: " + name;
                metaAttributes.put(name, componentAttributes);
            }

            return metaAttributes;
        }

        @Override
        public String toString()
        {
            return "ComponentMetadataMap{" +
                   "metas=" + metas +
                   '}';
        }

        public double indexSize()
        {
            return metas.values().stream().mapToLong(meta -> meta.length).sum();
        }
    }

    public static class ComponentMetadata
    {
        public static final String ROOT = "Root";
        public static final String OFFSET = "Offset";
        public static final String LENGTH = "Length";

        public final long root;
        public final long offset;
        public final long length;
        public final Map<String,String> attributes;

        public ComponentMetadata(long root, long offset, long length)
        {
            this.root = root;
            this.offset = offset;
            this.length = length;
            this.attributes = Collections.emptyMap();
        }

        ComponentMetadata(long root, long offset, long length, Map<String, String> attributes)
        {
            this.root = root;
            this.offset = offset;
            this.length = length;
            this.attributes = attributes;
        }

        ComponentMetadata(IndexInput input) throws IOException
        {
            this.root = input.readLong();
            this.offset = input.readLong();
            this.length = input.readLong();
            int size = input.readInt();

            attributes = new HashMap<>(size);
            for (int x=0; x < size; x++)
            {
                String key = input.readString();
                String value = input.readString();

                attributes.put(key, value);
            }
        }

        public void write(IndexOutput output) throws IOException
        {
            output.writeLong(root);
            output.writeLong(offset);
            output.writeLong(length);

            output.writeInt(attributes.size());
            for (Map.Entry<String,String> entry : attributes.entrySet())
            {
                output.writeString(entry.getKey());
                output.writeString(entry.getValue());
            }
        }

        @Override
        public String toString()
        {
            return String.format("ComponentMetadata{root=%d, offset=%d, length=%d, attributes=%s}", root, offset, length, attributes.toString());
        }

        public Map<String, String> asMap()
        {
            return ImmutableMap.<String, String>builder().putAll(attributes).put(OFFSET, Long.toString(offset)).put(LENGTH, Long.toString(length)).put(ROOT, Long.toString(root)).build();
        }
    }
}
