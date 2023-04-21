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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.GaugeProvider;
import org.apache.cassandra.io.sstable.IScrubber;
import org.apache.cassandra.io.sstable.MetricsProviders;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.filter.BloomFilterMetrics;
import org.apache.cassandra.io.sstable.format.AbstractSSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableReaderLoadingBuilder;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.SortedTableScrubber;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;
import org.apache.cassandra.utils.Pair;

/**
 * Bigtable format with trie indices
 */
public class BtiFormat extends AbstractSSTableFormat<BtiTableReader, BtiTableWriter>
{
    private final static Logger logger = LoggerFactory.getLogger(BtiFormat.class);

    public static final BtiFormat instance = new BtiFormat();

    public static final Version latestVersion = new BtiVersion(BtiVersion.current_version);
    static final BtiTableReaderFactory readerFactory = new BtiTableReaderFactory();
    static final BtiTableWriterFactory writerFactory = new BtiTableWriterFactory();

    public static class Components extends AbstractSSTableFormat.Components
    {
        public static class Types extends AbstractSSTableFormat.Components.Types
        {
            public static final Component.Type PARTITION_INDEX = Component.Type.createSingleton("PARTITION_INDEX", "Partitions.db", BtiFormat.class);
            public static final Component.Type ROW_INDEX = Component.Type.createSingleton("ROW_INDEX", "Rows.db", BtiFormat.class);
        }

        public final static Component PARTITION_INDEX = Types.PARTITION_INDEX.getSingleton();

        public final static Component ROW_INDEX = Types.ROW_INDEX.getSingleton();

        private final static Set<Component> STREAMING_COMPONENTS = ImmutableSet.of(DATA,
                                                                                   PARTITION_INDEX,
                                                                                   ROW_INDEX,
                                                                                   STATS,
                                                                                   COMPRESSION_INFO,
                                                                                   FILTER,
                                                                                   DIGEST,
                                                                                   CRC);

        private final static Set<Component> PRIMARY_COMPONENTS = ImmutableSet.of(DATA,
                                                                                 PARTITION_INDEX);

        private final static Set<Component> MUTABLE_COMPONENTS = ImmutableSet.of(STATS);

        private static final Set<Component> UPLOAD_COMPONENTS = ImmutableSet.of(DATA,
                                                                                PARTITION_INDEX,
                                                                                ROW_INDEX,
                                                                                COMPRESSION_INFO,
                                                                                STATS);

        private static final Set<Component> BATCH_COMPONENTS = ImmutableSet.of(DATA,
                                                                               PARTITION_INDEX,
                                                                               ROW_INDEX,
                                                                               COMPRESSION_INFO,
                                                                               FILTER,
                                                                               STATS);

        private final static Set<Component> ALL_COMPONENTS = ImmutableSet.of(DATA,
                                                                             PARTITION_INDEX,
                                                                             ROW_INDEX,
                                                                             STATS,
                                                                             COMPRESSION_INFO,
                                                                             FILTER,
                                                                             DIGEST,
                                                                             CRC,
                                                                             TOC);

        private final static Set<Component> GENERATED_ON_LOAD_COMPONENTS = ImmutableSet.of(FILTER);
    }


    private BtiFormat()
    {

    }

    public static BtiFormat getInstance()
    {
        return instance;
    }

    public static boolean isDefault()
    {
        return getInstance().getType() == Type.current();
    }

    @Override
    public Version getLatestVersion()
    {
        return latestVersion;
    }

    @Override
    public Version getVersion(String version)
    {
        return new BtiVersion(version);
    }

    @Override
    public BtiTableWriterFactory getWriterFactory()
    {
        return writerFactory;
    }

    @Override
    public BtiTableReaderFactory getReaderFactory()
    {
        return readerFactory;
    }

    @Override
    public Set<Component> streamingComponents()
    {
        return Components.STREAMING_COMPONENTS;
    }

    @Override
    public Set<Component> primaryComponents()
    {
        return Components.PRIMARY_COMPONENTS;
    }

    @Override
    public Set<Component> batchComponents()
    {
        return Components.BATCH_COMPONENTS;
    }

    @Override
    public Set<Component> uploadComponents()
    {
        return Components.UPLOAD_COMPONENTS;
    }

    @Override
    public Set<Component> mutableComponents()
    {
        return Components.MUTABLE_COMPONENTS;
    }

    @Override
    public Set<Component> allComponents()
    {
        return Components.ALL_COMPONENTS;
    }

    @Override
    public Set<Component> generatedOnLoadComponents()
    {
        return Components.GENERATED_ON_LOAD_COMPONENTS;
    }

    @Override
    public SSTableFormat.KeyCacheValueSerializer<BtiTableReader, TrieIndexEntry> getKeyCacheValueSerializer()
    {
        throw new AssertionError("BTI sstables do not use key cache");
    }

    @Override
    public IScrubber getScrubber(ColumnFamilyStore cfs, LifecycleTransaction transaction, OutputHandler outputHandler, IScrubber.Options options)
    {
        Preconditions.checkArgument(cfs.metadata().equals(transaction.onlyOne().metadata()));
        return new BtiTableScrubber(cfs, transaction, outputHandler, options);
    }

    @Override
    public BtiTableReader cast(SSTableReader sstr)
    {
        return (BtiTableReader) sstr;
    }

    @Override
    public BtiTableWriter cast(SSTableWriter sstw)
    {
        return (BtiTableWriter) sstw;
    }

    @Override
    public MetricsProviders getFormatSpecificMetricsProviders()
    {
        return BtiTableSpecificMetricsProviders.instance;
    }

    @Override
    public void deleteOrphanedComponents(Descriptor descriptor, Set<Component> components)
    {
        SortedTableScrubber.deleteOrphanedComponents(descriptor, components);
    }

    private void delete(Descriptor desc, List<Component> components)
    {
        logger.info("Deleting sstable: {}", desc);

        if (components.remove(SSTableFormat.Components.DATA))
            components.add(0, SSTableFormat.Components.DATA); // DATA component should be first

        for (Component component : components)
        {
            logger.trace("Deleting component {} of {}", component, desc);
            desc.fileFor(component).deleteIfExists();
        }
    }

    @Override
    public void delete(Descriptor desc)
    {
        try
        {
            delete(desc, Lists.newArrayList(Sets.intersection(allComponents(), desc.discoverComponents())));
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
        }
    }

    static class BtiTableReaderFactory implements SSTableReaderFactory<BtiTableReader, BtiTableReader.Builder>
    {
        @Override
        public SSTableReader.Builder<BtiTableReader, BtiTableReader.Builder> builder(Descriptor descriptor)
        {
            return new BtiTableReader.Builder(descriptor);
        }

        @Override
        public SSTableReaderLoadingBuilder<BtiTableReader, BtiTableReader.Builder> loadingBuilder(Descriptor descriptor, TableMetadataRef tableMetadataRef, Set<Component> components)
        {
            return new BtiTableReaderLoadingBuilder(new SSTable.Builder<>(descriptor).setTableMetadataRef(tableMetadataRef)
                                                                                     .setComponents(components));
        }

        @Override
        public Pair<DecoratedKey, DecoratedKey> readKeyRange(Descriptor descriptor, IPartitioner partitioner) throws IOException
        {
            return PartitionIndex.readFirstAndLastKey(descriptor.fileFor(Components.PARTITION_INDEX), partitioner);
        }

        @Override
        public Class<BtiTableReader> getReaderClass()
        {
            return BtiTableReader.class;
        }
    }

    static class BtiTableWriterFactory implements SSTableWriterFactory<BtiTableWriter, BtiTableWriter.Builder>
    {
        @Override
        public BtiTableWriter.Builder builder(Descriptor descriptor)
        {
            return new BtiTableWriter.Builder(descriptor);
        }

        @Override
        public long estimateSize(SSTableWriter.SSTableSizeParameters parameters)
        {
            return (long) ((parameters.partitionCount() // index entries
                            + parameters.partitionCount() // keys in data file
                            + parameters.dataSize()) // data
                           * 1.2); // bloom filter and row index overhead
        }
    }

    // versions are denoted as [major][minor].  Minor versions must be forward-compatible:
    // new fields are allowed in e.g. the metadata component, but fields can't be removed
    // or have their size changed.
    //
    static class BtiVersion extends Version
    {
        public static final String current_version = "da";
        public static final String earliest_supported_version = "da";

        // versions aa-cz are not supported in OSS
        // da - initial OSS version of the BIT format, Cassandra 5.0
        // NOTE: when adding a new version, please add that to LegacySSTableTest, too.

        private final boolean isLatestVersion;

        private final int correspondingMessagingVersion;

        BtiVersion(String version)
        {
            super(instance, version);

            isLatestVersion = version.compareTo(current_version) == 0;
            correspondingMessagingVersion = MessagingService.VERSION_40;
        }

        @Override
        public boolean isLatestVersion()
        {
            return isLatestVersion;
        }

        @Override
        public int correspondingMessagingVersion()
        {
            return correspondingMessagingVersion;
        }

        @Override
        public boolean hasCommitLogLowerBound()
        {
            return true;
        }

        @Override
        public boolean hasCommitLogIntervals()
        {
            return true;
        }

        @Override
        public boolean hasMaxCompressedLength()
        {
            return true;
        }

        @Override
        public boolean hasPendingRepair()
        {
            return true;
        }

        @Override
        public boolean hasIsTransient()
        {
            return true;
        }

        @Override
        public boolean hasMetadataChecksum()
        {
            return true;
        }

        @Override
        public boolean hasOldBfFormat()
        {
            return false;
        }

        @Override
        public boolean hasAccurateMinMax()
        {
            return true;
        }

        public boolean hasLegacyMinMax()
        {
            return false;
        }

        @Override
        public boolean hasOriginatingHostId()
        {
            return true;
        }

        @Override
        public boolean hasImprovedMinMax() {
            return true;
        }

        @Override
        public boolean hasPartitionLevelDeletionsPresenceMarker()
        {
            return true;
        }

        @Override
        public boolean hasKeyRange()
        {
            return true;
        }

        @Override
        public boolean isCompatible()
        {
            return version.compareTo(earliest_supported_version) >= 0 && version.charAt(0) <= current_version.charAt(0);
        }

        @Override
        public boolean isCompatibleForStreaming()
        {
            return isCompatible() && version.charAt(0) == current_version.charAt(0);
        }

    }

    private static class BtiTableSpecificMetricsProviders implements MetricsProviders
    {
        private final static BtiTableSpecificMetricsProviders instance = new BtiTableSpecificMetricsProviders();

        private final Iterable<GaugeProvider<?>> gaugeProviders = BloomFilterMetrics.instance.getGaugeProviders();

        @Override
        public Iterable<GaugeProvider<?>> getGaugeProviders()
        {
            return gaugeProviders;
        }
    }
}