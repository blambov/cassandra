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
package org.apache.cassandra.db.memtable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.BufferDecoratedKey;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionInfo;
import org.apache.cassandra.db.MutableDeletionInfo;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ClusteringIndexFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.BTreePartitionUpdate;
import org.apache.cassandra.db.partitions.BTreePartitionUpdater;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.partitions.TriePartitionUpdate;
import org.apache.cassandra.db.partitions.TriePartitionUpdater;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryDTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.metrics.TrieMemtableMetricsView;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.memory.EnsureOnHeap;
import org.apache.cassandra.utils.memory.HeapCloner;
import org.apache.cassandra.utils.memory.MemtableAllocator;
import org.github.jamm.Unmetered;

/**
 * Trie memtable implementation. Improves memory usage, garbage collection efficiency and lookup performance.
 * The implementation is described in detail in the paper:
 *       https://www.vldb.org/pvldb/vol15/p3359-lambov.pdf
 *
 * The configuration takes a single parameter:
 * - shards: the number of shards to split into, defaulting to the number of CPU cores.
 *
 * Also see Memtable_API.md.
 */
public class TrieMemtable extends AbstractShardedMemtable
{
    private static final Logger logger = LoggerFactory.getLogger(TrieMemtable.class);

    /** Buffer type to use for memtable tries (on- vs off-heap) */
    public static final BufferType BUFFER_TYPE = DatabaseDescriptor.getMemtableAllocationType().toBufferType();
    public static final BufferType BUFFER_TYPE;

    /**
     * Force copy checker (see MemtableTrie.ApplyState) ensuring all modifications apply atomically and consistently to
     * the whole partition.
     */
    public static final Predicate<InMemoryDTrie.NodeFeatures<Object>> FORCE_COPY_PARTITION_BOUNDARY = features -> isPartitionBoundary(features.content());

    public static final Predicate<Object> IS_PARTITION_BOUNDARY = TrieMemtable::isPartitionBoundary;

    /** If keys is below this length, we will use a recursive procedure for inserting data in the memtable trie. */
    @VisibleForTesting
    public static final int MAX_RECURSIVE_KEY_LENGTH = 128;

    /** The byte-ordering conversion version to use for memtables. */
    public static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS50;

    // Set to true when the memtable requests a switch (e.g. for trie size limit being reached) to ensure only one
    // thread calls cfs.switchMemtableIfCurrent.
    private final AtomicBoolean switchRequested = new AtomicBoolean(false);

    /**
     * Sharded memtable sections. Each is responsible for a contiguous range of the token space (between boundaries[i]
     * and boundaries[i+1]) and is written to by one thread at a time, while reads are carried out concurrently
     * (including with any write).
     */
    private final MemtableShard[] shards;

    /**
     * A merged view of the memtable map. Used for partition range queries and flush.
     * For efficiency we serve single partition requests off the shard which offers more direct InMemoryDTrie methods.
     */
    private final Trie<Object> mergedTrie;

    @Unmetered
    private final TrieMemtableMetricsView metrics;

    TrieMemtable(AtomicReference<CommitLogPosition> commitLogLowerBound, TableMetadataRef metadataRef, Owner owner, Integer shardCountOption)
    {
        super(commitLogLowerBound, metadataRef, owner, shardCountOption);
        this.metrics = new TrieMemtableMetricsView(metadataRef.keyspace, metadataRef.name);
        this.shards = generatePartitionShards(boundaries.shardCount(), allocator, metadataRef, metrics);
        this.mergedTrie = makeMergedTrie(shards);
    }

    private static MemtableShard[] generatePartitionShards(int splits,
                                                           MemtableAllocator allocator,
                                                           TableMetadataRef metadata,
                                                           TrieMemtableMetricsView metrics)
    {
        MemtableShard[] partitionMapContainer = new MemtableShard[splits];
        for (int i = 0; i < splits; i++)
            partitionMapContainer[i] = new MemtableShard(metadata, allocator, metrics);

        return partitionMapContainer;
    }

    private static Trie<Object> makeMergedTrie(MemtableShard[] shards)
    {
        List<Trie<Object>> tries = new ArrayList<>(shards.length);
        for (MemtableShard shard : shards)
            tries.add(shard.data);
        return Trie.mergeDistinct(tries);
    }

    @Override
    public boolean isClean()
    {
        for (MemtableShard shard : shards)
            if (!shard.isClean())
                return false;
        return true;
    }

    @Override
    public void discard()
    {
        super.discard();
        // metrics here are not thread safe, but I think we can live with that
        metrics.lastFlushShardDataSizes.reset();
        for (MemtableShard shard : shards)
        {
            metrics.lastFlushShardDataSizes.update(shard.liveDataSize());
        }
        // the buffer release is a longer-running process, do it in a separate loop to not make the metrics update wait
        for (MemtableShard shard : shards)
        {
            shard.data.discardBuffers();
        }
    }

    /**
     * Should only be called by ColumnFamilyStore.apply via Keyspace.apply, which supplies the appropriate
     * OpOrdering.
     *
     * commitLogSegmentPosition should only be null if this is a secondary index, in which case it is *expected* to be null
     */
    @Override
    public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
    {
        try
        {
            DecoratedKey key = update.partitionKey();
            MemtableShard shard = shards[boundaries.getShardForKey(key)];
            long colUpdateTimeDelta = shard.put(update, indexer, opGroup);

            if (shard.data.reachedAllocatedSizeThreshold() && !switchRequested.getAndSet(true))
            {
                logger.info("Scheduling flush due to trie size limit reached.");
                owner.signalFlushRequired(this, ColumnFamilyStore.FlushReason.MEMTABLE_LIMIT);
            }

            return colUpdateTimeDelta;
        }
        catch (TrieSpaceExhaustedException e)
        {
            // This should never happen as {@link InMemoryDTrie#reachedAllocatedSizeThreshold} should become
            // true and trigger a memtable switch long before this limit is reached.
            throw new IllegalStateException(e);
        }
    }

    @Override
    public long getLiveDataSize()
    {
        long total = 0L;
        for (MemtableShard shard : shards)
            total += shard.liveDataSize();
        return total;
    }

    @Override
    public long operationCount()
    {
        long total = 0L;
        for (MemtableShard shard : shards)
            total += shard.currentOperations();
        return total;
    }

    @Override
    public long partitionCount()
    {
        int total = 0;
        for (MemtableShard shard : shards)
            total += shard.partitionCount();
        return total;
    }

    /**
     * Returns the minTS if one available, otherwise NO_MIN_TIMESTAMP.
     *
     * EncodingStats uses a synthetic epoch TS at 2015. We don't want to leak that (CASSANDRA-18118) so we return NO_MIN_TIMESTAMP instead.
     *
     * @return The minTS or NO_MIN_TIMESTAMP if none available
     */
    @Override
    public long getMinTimestamp()
    {
        long min = Long.MAX_VALUE;
        for (MemtableShard shard : shards)
            min =  Long.min(min, shard.minTimestamp());
        return min != EncodingStats.NO_STATS.minTimestamp ? min : NO_MIN_TIMESTAMP;
    }

    @Override
    public long getMinLocalDeletionTime()
    {
        long min = Long.MAX_VALUE;
        for (MemtableShard shard : shards)
            min =  Long.min(min, shard.minLocalDeletionTime());
        return min;
    }

    @Override
    RegularAndStaticColumns columns()
    {
        for (MemtableShard shard : shards)
            columnsCollector.update(shard.columns);
        return columnsCollector.get();
    }

    @Override
    EncodingStats encodingStats()
    {
        for (MemtableShard shard : shards)
            statsCollector.update(shard.stats);
        return statsCollector.get();
    }

    static boolean isPartitionBoundary(Object content)
    {
        return content != null && content instanceof PartitionData;
    }

    @Override
    public MemtableUnfilteredPartitionIterator partitionIterator(final ColumnFilter columnFilter,
                                                                 final DataRange dataRange,
                                                                 SSTableReadsListener readsListener)
    {
        AbstractBounds<PartitionPosition> keyRange = dataRange.keyRange();

        PartitionPosition left = keyRange.left;
        PartitionPosition right = keyRange.right;
        if (left.isMinimum())
            left = null;
        if (right.isMinimum())
            right = null;

        // TODO: Check if these partition positions are properly encoded to include/exclude ends
        Trie<Object> subMap = mergedTrie.subtrie(left, right);

        return new MemtableUnfilteredPartitionIterator(metadata(),
                                                       allocator.ensureOnHeap(),
                                                       subMap,
                                                       columnFilter,
                                                       dataRange);
        // readsListener is ignored as it only accepts sstable signals
    }

    private Partition getPartition(DecoratedKey key)
    {
        int shardIndex = boundaries.getShardForKey(key);
        Trie<Object> trie = shards[shardIndex].data.tailTrie(key);
        return createPartition(metadata(), allocator.ensureOnHeap(), key, trie);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key, Slices slices, ColumnFilter selectedColumns, boolean reversed, SSTableReadsListener listener)
    {
        Partition p = getPartition(key);
        if (p == null)
            return null;
        else
            return p.unfilteredIterator(selectedColumns, slices, reversed);
    }

    @Override
    public UnfilteredRowIterator rowIterator(DecoratedKey key)
    {
        Partition p = getPartition(key);
        return p != null ? p.unfilteredIterator() : null;
    }

    private static TrieBackedPartition createPartition(TableMetadata metadata, EnsureOnHeap ensureOnHeap, DecoratedKey key, Trie<Object> trie)
    {
        if (trie == null)
            return null;
        PartitionData holder = (PartitionData) trie.get(ByteComparable.EMPTY);
        if (holder == null)
            return null;

        return TrieBackedPartition.create(key,
                                          holder.columns(),
                                          holder.stats(),
                                          trie,
                                          metadata,
                                          ensureOnHeap);
    }

    private static TrieBackedPartition getPartitionFromTrieEntry(TableMetadata metadata, EnsureOnHeap ensureOnHeap, Map.Entry<ByteComparable, Trie<Object>> en)
    {
        DecoratedKey key = BufferDecoratedKey.fromByteComparable(en.getKey(),
                                                                 BYTE_COMPARABLE_VERSION,
                                                                 metadata.partitioner);
        return createPartition(metadata, ensureOnHeap, key, en.getValue());
    }

    /**
     * Metadata object signifying the root node of a partition. Holds the deletion information as well as a link
     * to the owning subrange, which is used for compiling statistics and column sets.
     *
     * Descends from MutableDeletionInfo to permit tail tries to be passed directly to TrieBackedPartition.
     */
    public static class PartitionData extends MutableDeletionInfo
    {
        public final MemtableShard owner;

        /**
         * Covered data size. This may be updated without creating a new PartitionData object, because it is not
         * something that readers care about.
         * Does not need to be volatile or atomic as it is updated by the single subrange writer thread, and is only
         * otherwise used by FlushDataCollector, which is run after a write barrier has signalled that all writes that
         * can end up in this memtable have completed.
         */
        public long dataSize;

        public static final long HEAP_SIZE = ObjectSizes.measure(new PartitionData(DeletionInfo.LIVE, null, 0));

        public PartitionData(DeletionInfo deletion,
                             MemtableShard owner,
                             long dataSize)
        {
            super(deletion.getPartitionDeletion(), deletion.copyRanges(HeapCloner.instance));
            this.owner = owner;
            this.dataSize = dataSize;
        }

        public PartitionData(PartitionData existing,
                             DeletionInfo update,
                             long dataSizeDelta)
        {
            // Start with the update content, to properly copy it
            this(update, existing.owner, existing.dataSize + dataSizeDelta);
            add(existing);
        }

        public RegularAndStaticColumns columns()
        {
            return owner.columns;
        }

        public EncodingStats stats()
        {
            return owner.stats;
        }

        public String toString()
        {
            return "partition " + super.toString();
        }

        public long unsharedHeapSize()
        {
            return super.unsharedHeapSize() + HEAP_SIZE - MutableDeletionInfo.EMPTY_SIZE;
        }
    }

    @Override
    public FlushablePartitionSet<TrieBackedPartition> getFlushSet(PartitionPosition from, PartitionPosition to)
    {
        Trie<Object> toFlush = mergedTrie.subtrie(from, to);
        var toFlushIterable = toFlush.tailTries(IS_PARTITION_BOUNDARY, Direction.FORWARD);
        long keySize = 0;
        int keyCount = 0;

        for (Map.Entry<ByteComparable, Trie<Object>> en : toFlushIterable)
        {
            byte[] keyBytes = DecoratedKey.keyFromByteSource(ByteSource.peekable(en.getKey().asComparableBytes(BYTE_COMPARABLE_VERSION)),
                                                             BYTE_COMPARABLE_VERSION,
                                                             metadata().partitioner);
            keySize += keyBytes.length;
            keyCount++;
        }
        long partitionKeySize = keySize;
        int partitionCount = keyCount;

        return new AbstractFlushablePartitionSet<TrieBackedPartition>()
        {
            public Memtable memtable()
            {
                return TrieMemtable.this;
            }

            public PartitionPosition from()
            {
                return from;
            }

            public PartitionPosition to()
            {
                return to;
            }

            public long partitionCount()
            {
                return partitionCount;
            }

            public Iterator<TrieBackedPartition> iterator()
            {
                // TODO: avoid the transform by using a TailTrieIterator subclass
                return Iterators.transform(toFlushIterable.iterator(),
                                           // During flushing we are certain the memtable will remain at least until
                                           // the flush completes. No copying to heap is necessary.
                                           entry -> getPartitionFromTrieEntry(metadata(), EnsureOnHeap.NOOP, entry));
            }

            public long partitionKeysSize()
            {
                return partitionKeySize;
            }
        };
    }

    public static class MemtableShard
    {
        // The following fields are volatile as we have to make sure that when we
        // collect results from all sub-ranges, the thread accessing the value
        // is guaranteed to see the changes to the values.

        // The smallest timestamp for all partitions stored in this shard
        private volatile long minTimestamp = Long.MAX_VALUE;

        private volatile long minLocalDeletionTime = Long.MAX_VALUE;

        private volatile long liveDataSize = 0;

        private volatile long currentOperations = 0;

        private volatile int partitionCount = 0;

        @Unmetered
        private final ReentrantLock writeLock = new ReentrantLock();

        // Content map for the given shard. This is implemented as a memtable trie which uses the prefix-free
        // byte-comparable ByteSource representations of the keys to address the partitions.
        //
        // This map is used in a single-producer, multi-consumer fashion: only one thread will insert items but
        // several threads may read from it and iterate over it. Iterators (especially partition range iterators)
        // may operate for a long period of time and thus iterators should not throw ConcurrentModificationExceptions
        // if the underlying map is modified during iteration, they should provide a weakly consistent view of the map
        // instead.
        //
        // Also, this data is backed by memtable memory, when accessing it callers must specify if it can be accessed
        // unsafely, meaning that the memtable will not be discarded as long as the data is used, or whether the data
        // should be copied on heap for off-heap allocators.
        @VisibleForTesting
        final InMemoryDTrie<Object> data;

        RegularAndStaticColumns columns;

        EncodingStats stats;

        @Unmetered  // total pool size should not be included in memtable's deep size
        private final MemtableAllocator allocator;

        @Unmetered
        private final TrieMemtableMetricsView metrics;

        private final TableMetadataRef metadata;

        @VisibleForTesting
        MemtableShard(TableMetadataRef metadata, MemtableAllocator allocator, TrieMemtableMetricsView metrics)
        {
            this.metadata = metadata;
            this.data = new InMemoryDTrie<>(BUFFER_TYPE);
            this.columns = RegularAndStaticColumns.NONE;
            this.stats = EncodingStats.NO_STATS;
            this.allocator = allocator;
            this.metrics = metrics;
        }

        public long put(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup)
        throws TrieSpaceExhaustedException
        {
            TriePartitionUpdater updater = new TriePartitionUpdater(allocator, allocator.cloner(opGroup), indexer, metadata.get(), this);
            boolean locked = writeLock.tryLock();
            if (locked)
            {
                metrics.uncontendedPuts.inc();
            }
            else
            {
                metrics.contendedPuts.inc();
                long lockStartTime = Clock.Global.nanoTime();
                writeLock.lock();
                metrics.contentionTime.addNano(Clock.Global.nanoTime() - lockStartTime);
            }
            try
            {
                try
                {
                    long onHeap = data.sizeOnHeap();
                    long offHeap = data.sizeOffHeap();
                    // Use the fast recursive put if we know the key is small enough to not cause a stack overflow.
                    data.apply(TriePartitionUpdate.asMergableTrie(update),
                               updater,
                               FORCE_COPY_PARTITION_BOUNDARY);
                    allocator.offHeap().adjust(data.sizeOffHeap() - offHeap, opGroup);
                    allocator.onHeap().adjust(data.sizeOnHeap() - onHeap, opGroup);
                    partitionCount += updater.partitionsAdded;
                }
                finally
                {
                    minTimestamp = Math.min(minTimestamp, update.stats().minTimestamp);
                    minLocalDeletionTime = Math.min(minLocalDeletionTime, update.stats().minLocalDeletionTime);
                    liveDataSize += updater.dataSize;
                    currentOperations += update.operationCount();

                    columns = columns.mergeTo(update.columns());
                    stats = stats.mergeWith(update.stats());
                }
            }
            finally
            {
                writeLock.unlock();
            }
            return updater.colUpdateTimeDelta;
        }

        public boolean isClean()
        {
            return data.isEmpty();
        }

        public int partitionCount()
        {
            return partitionCount;
        }

        long minTimestamp()
        {
            return minTimestamp;
        }

        long liveDataSize()
        {
            return liveDataSize;
        }

        long currentOperations()
        {
            return currentOperations;
        }

        long minLocalDeletionTime()
        {
            return minLocalDeletionTime;
        }
    }

    static class MemtableUnfilteredPartitionIterator extends AbstractUnfilteredPartitionIterator implements UnfilteredPartitionIterator
    {
        private final TableMetadata metadata;
        private final EnsureOnHeap ensureOnHeap;
        private final Iterator<Map.Entry<ByteComparable, Trie<Object>>> iter;
        private final ColumnFilter columnFilter;
        private final DataRange dataRange;

        public MemtableUnfilteredPartitionIterator(TableMetadata metadata,
                                                   EnsureOnHeap ensureOnHeap,
                                                   Trie<Object> source,
                                                   ColumnFilter columnFilter,
                                                   DataRange dataRange)
        {
            this.metadata = metadata;
            this.ensureOnHeap = ensureOnHeap;
            // TODO: avoid the transform by using a TailTrieIterator subclass
            this.iter = source.tailTries(IS_PARTITION_BOUNDARY, Direction.FORWARD).iterator();
            this.columnFilter = columnFilter;
            this.dataRange = dataRange;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        public boolean hasNext()
        {
            return iter.hasNext();
        }

        public UnfilteredRowIterator next()
        {
            Partition partition = getPartitionFromTrieEntry(metadata(), ensureOnHeap, iter.next());
            DecoratedKey key = partition.partitionKey();
            ClusteringIndexFilter filter = dataRange.clusteringIndexFilter(key);

            return filter.getUnfilteredRowIterator(columnFilter, partition);
        }
    }

    public static Factory factory(Map<String, String> optionsCopy)
    {
        String shardsString = optionsCopy.remove(SHARDS_OPTION);
        Integer shardCount = shardsString != null ? Integer.parseInt(shardsString) : null;
        return new Factory(shardCount);
    }

    static class Factory implements Memtable.Factory
    {
        final Integer shardCount;

        Factory(Integer shardCount)
        {
            this.shardCount = shardCount;
        }

        public Memtable create(AtomicReference<CommitLogPosition> commitLogLowerBound,
                               TableMetadataRef metadaRef,
                               Owner owner)
        {
            return new TrieMemtable(commitLogLowerBound, metadaRef, owner, shardCount);
        }

        @Override
        public PartitionUpdate.Factory partitionUpdateFactory()
        {
            return TriePartitionUpdate.FACTORY;
        }

        @Override
        public TableMetrics.ReleasableMetric createMemtableMetrics(TableMetadataRef metadataRef)
        {
            TrieMemtableMetricsView metrics = new TrieMemtableMetricsView(metadataRef.keyspace, metadataRef.name);
            return metrics::release;
        }

        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Factory factory = (Factory) o;
            return Objects.equals(shardCount, factory.shardCount);
        }

        public int hashCode()
        {
            return Objects.hash(shardCount);
        }
    }

    @VisibleForTesting
    public long unusedReservedOnHeapMemory()
    {
        long size = 0;
        for (MemtableShard shard : shards)
            size += shard.data.unusedReservedOnHeapMemory();
        return size;
    }

    /**
     * How data should be accessed. Select UNSAFE if you are
     * sure the memtable backing memory will still be available as
     * this will avoid a copy, otherwise select ON_HEAP knowing that
     * for off-heap allocators this will copy the data.
     */
    public enum DataAccess
    {
        /**
         * The data can be backed by the memtable off-heap memory, this can only be used
         * when we are sure that the memtable won't be discarded for the entire life duration
         * of the data being accessed.
         */
        UNSAFE,
        /**
         * The data will be copied on heap if required. This means the data can outlive the
         * memtable. It is the safest option but it may incur a copy on the heap.
         */
        ON_HEAP
    }
}
