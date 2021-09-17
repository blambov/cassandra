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
package org.apache.cassandra.db.compaction.writers;

import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.compaction.CompactionSSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.compaction.LeveledManifest;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;

public class MajorLeveledCompactionWriter extends CompactionAwareWriter
{
    private final long maxSSTableSize;
    private int currentLevel = 1;
    private long averageEstimatedKeysPerSSTable;
    private long partitionsWritten = 0;
    private long totalWrittenInLevel = 0;
    private int sstablesWritten = 0;
    private final long keysPerSSTable;
    private final int levelFanoutSize;

    public MajorLeveledCompactionWriter(ColumnFamilyStore cfs,
                                        Directories directories,
                                        LifecycleTransaction txn,
                                        Set<SSTableReader> nonExpiredSSTables,
                                        long maxSSTableSize)
    {
        this(cfs, directories, txn, nonExpiredSSTables, maxSSTableSize, false);
    }

    @SuppressWarnings("resource")
    public MajorLeveledCompactionWriter(ColumnFamilyStore cfs,
                                        Directories directories,
                                        LifecycleTransaction txn,
                                        Set<SSTableReader> nonExpiredSSTables,
                                        long maxSSTableSize,
                                        boolean keepOriginals)
    {
        super(cfs, directories, txn, nonExpiredSSTables, keepOriginals);
        this.maxSSTableSize = maxSSTableSize;
        this.levelFanoutSize = cfs.getLevelFanoutSize();
        long estimatedSSTables = Math.max(1, CompactionSSTable.getTotalBytes(nonExpiredSSTables) / maxSSTableSize);
        keysPerSSTable = estimatedTotalKeys / estimatedSSTables;
    }

    @Override
    public boolean append(UnfilteredRowIterator partition)
    {
        partitionsWritten++;
        return super.append(partition);
    }

    @Override
    protected boolean shouldSwitchWriterInCurrentLocation(DecoratedKey key)
    {
        long totalWrittenInCurrentWriter = sstableWriter.currentWriter().getEstimatedOnDiskBytesWritten();
        if (totalWrittenInCurrentWriter > maxSSTableSize)
        {
            totalWrittenInLevel += totalWrittenInCurrentWriter;
            if (totalWrittenInLevel > LeveledManifest.maxBytesForLevel(currentLevel, levelFanoutSize, maxSSTableSize))
            {
                totalWrittenInLevel = 0;
                currentLevel++;
            }
            return true;
        }
        return false;

    }

    @Override
    public void switchCompactionWriter(Directories.DataDirectory location)
    {
        averageEstimatedKeysPerSSTable = Math.round(((double) averageEstimatedKeysPerSSTable * sstablesWritten + partitionsWritten) / (sstablesWritten + 1));
        partitionsWritten = 0;
        sstablesWritten = 0;
        super.switchCompactionWriter(location);
    }

    @Override
    @SuppressWarnings("resource")
    protected SSTableWriter sstableWriter(Directories.DataDirectory directory, PartitionPosition diskBoundary)
    {
        return SSTableWriter.create(cfs.newSSTableDescriptor(getDirectories().getLocationForDisk(directory)),
                                    keysPerSSTable,
                                    minRepairedAt,
                                    pendingRepair,
                                    isTransient,
                                    cfs.metadata,
                                    new MetadataCollector(txn.originals(), cfs.metadata().comparator, currentLevel),
                                    SerializationHeader.make(cfs.metadata(), txn.originals()),
                                    cfs.indexManager.listIndexGroups(),
                                    txn);
    }
}
