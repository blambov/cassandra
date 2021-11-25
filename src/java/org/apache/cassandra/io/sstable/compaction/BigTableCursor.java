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

package org.apache.cassandra.io.sstable.compaction;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBoundOrBoundary;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.UnfilteredValidation;
import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.db.rows.UnfilteredSerializer;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.IOException;

/**
 * Cursor over sstable data files.
 * Supports both BIG and BTI formats (which differ only in index and whose data file formats are identical).
 */
public class BigTableCursor implements SSTableCursor
{
    private final RandomAccessReader dataFile;
    private final SSTableReader sstable;
    private final DeserializationHelper helper;
    private final SerializationHeader header;

    private DecoratedKey partitionKey;
    private ClusteringPrefix<?> clusteringKey;

    private int rowFlags;
    private Columns columns;
    private int currentColumnIndex;
    private ColumnMetadata columnMetadata;
    private int cellsLeftInColumn;
    private Cell<byte[]> currentCell;

    private DeletionTime partitionLevelDeletion;
    private DeletionTime activeRangeDeletion;
    private DeletionTime rowLevelDeletion;
    private LivenessInfo rowLivenessInfo;
    private DeletionTime complexColumnDeletion;

    private Type currentType = Type.UNINITIALIZED;

    public BigTableCursor(SSTableReader sstable)
    {
        this(sstable, sstable.openDataReader());
    }

    public BigTableCursor(SSTableReader sstable, RateLimiter limiter)
    {
        this(sstable, sstable.openDataReader(limiter));
    }

    public BigTableCursor(SSTableReader sstable, RandomAccessReader dataFile)
    {
        this.dataFile = dataFile;
        this.header = sstable.header;
        this.helper = new DeserializationHelper(sstable.metadata(), sstable.descriptor.version.correspondingMessagingVersion(), DeserializationHelper.Flag.LOCAL);
        this.sstable = sstable;
        this.activeRangeDeletion = DeletionTime.LIVE;
    }

    private boolean consumePartitionHeader() throws IOException
    {
        if (dataFile.isEOF())
        {
            currentType = Type.EXHAUSTED;
            return false;
        }

        currentType = Type.PARTITION;
        partitionKey = sstable.decorateKey(ByteBufferUtil.readWithShortLength(dataFile));
        partitionLevelDeletion = DeletionTime.serializer.deserialize(dataFile);
        if (!partitionLevelDeletion.validate())
            UnfilteredValidation.handleInvalid(sstable.metadata(), partitionKey, sstable, "partitionLevelDeletion="+partitionLevelDeletion.toString());
        if (!activeRangeDeletion.isLive())
            throw new IOException(String.format("Invalid active range tombstone at the beginning of partition %s: %s",
                                                partitionKey.toString(),
                                                activeRangeDeletion));
        rowLevelDeletion = null;
        rowLivenessInfo = null;
        complexColumnDeletion = null;
        currentCell = null;
        columnMetadata = null;
        clusteringKey = null;
        return true;
    }

    private boolean consumeUnfilteredHeader() throws IOException
    {
        boolean haveData;
        do
        {
            rowFlags = dataFile.readUnsignedByte();
            if (UnfilteredSerializer.isEndOfPartition(rowFlags))
                return false;

            int rowExtendedFlags = UnfilteredSerializer.readExtendedFlags(dataFile, rowFlags);

            switch (UnfilteredSerializer.kind(rowFlags))
            {
                case ROW:
                    haveData = consumeRowHeader(rowExtendedFlags);
                    currentType = Type.ROW;
                    break;
                case RANGE_TOMBSTONE_MARKER:
                    haveData = consumeRangeTombstoneMarker();
                    currentType = Type.RANGE_TOMBSTONE;
                    break;
                default:
                    throw new AssertionError();
            }
        }
        while (!haveData);
        complexColumnDeletion = null;
        currentCell = null;
        columnMetadata = null;
        return true;
    }

    private boolean consumeRangeTombstoneMarker() throws IOException
    {
        ClusteringBoundOrBoundary<?> bound = ClusteringBoundOrBoundary.serializer.deserialize(dataFile, helper.version, header.clusteringTypes());

        if (header.isForSSTable())
        {
            dataFile.readUnsignedVInt(); // marker size
            dataFile.readUnsignedVInt(); // previous unfiltered size
        }

        if (bound.kind().isEnd())
        {
            DeletionTime endDeletion = header.readDeletionTime(dataFile);
            if (!endDeletion.equals(activeRangeDeletion))
                throw new IOException(String.format("Invalid tombstone end boundary in partition %s, expected %s was %s",
                                                    partitionKey.toString(),
                                                    activeRangeDeletion,
                                                    endDeletion));
        }

        if (bound.kind().isStart())
            rowLevelDeletion = header.readDeletionTime(dataFile);
        else
            rowLevelDeletion = DeletionTime.LIVE;

        if (!rowLevelDeletion.validate())
            UnfilteredValidation.handleInvalid(sstable.metadata(), partitionKey, sstable, "rowLevelDeletion="+rowLevelDeletion.toString());

        clusteringKey = bound;
        return true;
    }

    /**
     * @return false if empty
     * @throws IOException
     */
    private boolean consumeRowHeader(int rowExtendedFlags) throws IOException
    {
        boolean isStatic = UnfilteredSerializer.isStatic(rowExtendedFlags);

        if (isStatic)
        {
            if (!header.hasStatic())
                throw new IOException(String.format("Static row encountered in partition %s on table without static columns",
                                                    partitionKey.toString()));

            clusteringKey = Clustering.STATIC_CLUSTERING;
        }
        else
            clusteringKey = Clustering.serializer.deserialize(dataFile, helper.version, header.clusteringTypes());

        if (header.isForSSTable())
        {
            dataFile.readUnsignedVInt(); // Skip row size
            dataFile.readUnsignedVInt(); // previous unfiltered size
        }

        boolean hasTimestamp = (rowFlags & UnfilteredSerializer.HAS_TIMESTAMP) != 0;
        boolean hasTTL = (rowFlags & UnfilteredSerializer.HAS_TTL) != 0;
        boolean hasDeletion = (rowFlags & UnfilteredSerializer.HAS_DELETION) != 0;
        // shadowable deletions are obsolete
        boolean hasAllColumns = (rowFlags & UnfilteredSerializer.HAS_ALL_COLUMNS) != 0;
        Columns headerColumns = header.columns(isStatic);

        if (hasTimestamp)
        {
            long timestamp = header.readTimestamp(dataFile);
            int ttl = hasTTL ? header.readTTL(dataFile) : LivenessInfo.NO_TTL;
            int localDeletionTime = hasTTL ? header.readLocalDeletionTime(dataFile) : LivenessInfo.NO_EXPIRATION_TIME;
            rowLivenessInfo = LivenessInfo.withExpirationTime(timestamp, ttl, localDeletionTime);
            if (rowLivenessInfo.isExpiring() && (rowLivenessInfo.ttl() < 0 || rowLivenessInfo.localExpirationTime() < 0))
                UnfilteredValidation.handleInvalid(sstable.metadata(), partitionKey, sstable, "rowLivenessInfo="+rowLivenessInfo.toString());
        }
        else
            rowLivenessInfo = LivenessInfo.EMPTY;

        if (hasDeletion)
        {
            rowLevelDeletion = header.readDeletionTime(dataFile);
            if (!rowLevelDeletion.validate())
                UnfilteredValidation.handleInvalid(sstable.metadata(), partitionKey, sstable, "rowLevelDeletion="+rowLevelDeletion.toString());
        }
        else
            rowLevelDeletion = DeletionTime.LIVE;

        // TODO: Improve column presence decoding, there should be no need for an array here
        columns = hasAllColumns ? headerColumns : Columns.serializer.deserializeSubset(headerColumns, dataFile);

        if (!hasTimestamp && !hasDeletion && columns.isEmpty())
            return false;

        this.currentColumnIndex = -1;
        this.cellsLeftInColumn = 0;
        return true;
    }

    public boolean consumeColumn() throws IOException
    {
        while (true)
        {
            if (cellsLeftInColumn == 0)
            {
                if (++currentColumnIndex == columns.size())
                    return false;

                columnMetadata = this.columns.getSimple(currentColumnIndex);
                assert helper.includes(columnMetadata);  // we are fetching all columns
                if (columnMetadata.isComplex())
                {
                    helper.startOfComplexColumn(columnMetadata);
                    DeletionTime complexDeletion = DeletionTime.LIVE;
                    if ((rowFlags & UnfilteredSerializer.HAS_COMPLEX_DELETION) != 0)
                    {
                        complexDeletion = header.readDeletionTime(dataFile);
                        if (!complexDeletion.validate())
                            UnfilteredValidation.handleInvalid(sstable.metadata(), partitionKey, sstable,
                                                               "complexColumnDeletion="+complexDeletion.toString());
                        if (helper.isDroppedComplexDeletion(complexDeletion))
                            complexDeletion = DeletionTime.LIVE;
                    }

                    cellsLeftInColumn = (int) dataFile.readUnsignedVInt();

                    currentType = Type.COMPLEX_COLUMN;
                    complexColumnDeletion = complexDeletion;
                    return true;
                    // not issuing helper.endOfComplexColumn, but that should be okay
                }
                else
                {
                    currentType = Type.SIMPLE_COLUMN;
                    Cell<byte[]> cell = Cell.serializer.deserialize(dataFile, rowLivenessInfo, columnMetadata, header, helper, ByteArrayAccessor.instance);
                    if (cell.hasInvalidDeletions())
                        UnfilteredValidation.handleInvalid(sstable.metadata(), partitionKey, sstable, cell.toString());
                    if (!helper.isDropped(cell, false))
                    {
                        currentCell = cell;
                        return true;
                    }
                }
            }
        }
    }

    public boolean consumeComplexCell() throws IOException
    {
        while (cellsLeftInColumn > 0)
        {
            --cellsLeftInColumn;
            Cell<byte[]> cell = Cell.serializer.deserialize(dataFile, rowLivenessInfo, columnMetadata, header, helper, ByteArrayAccessor.instance);
            if (cell.hasInvalidDeletions())
                UnfilteredValidation.handleInvalid(sstable.metadata(), partitionKey, sstable, cell.toString());
            if (!helper.isDropped(cell, true))
            {
                currentType = Type.COMPLEX_COLUMN_CELL;
                currentCell = cell;
                return true;
            }
        }
        return false;
    }

    public Type advance()
    {
        if (currentType == Type.RANGE_TOMBSTONE)
            activeRangeDeletion = rowLevelDeletion;

        try
        {
            switch (currentType)
            {
                case EXHAUSTED:
                    throw new IllegalStateException("Cursor advanced after exhaustion.");
                case COMPLEX_COLUMN_CELL:
                case COMPLEX_COLUMN:
                    if (consumeComplexCell())
                        return currentType;
                    // else fall through
                case SIMPLE_COLUMN:
                case ROW:
                    if (consumeColumn())
                        return currentType;
                    // else fall through
                case RANGE_TOMBSTONE:
                case PARTITION:
                    if (consumeUnfilteredHeader())
                        return currentType;
                    // else fall through
                case UNINITIALIZED:
                    consumePartitionHeader();
                    return currentType;
                default:
                    throw new AssertionError();
            }
        }
        catch (IOException | IndexOutOfBoundsException e)
        {
            sstable.markSuspect();
            throw new CorruptSSTableException(e, dataFile.getFile().path());
        }
    }

    public Type type()
    {
        return currentType;
    }

    public DecoratedKey partitionKey()
    {
        return partitionKey;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public ClusteringPrefix<?> clusteringKey()
    {
        return clusteringKey;
    }

    public LivenessInfo clusteringKeyLivenessInfo()
    {
        return rowLivenessInfo;
    }

    public DeletionTime rowLevelDeletion()
    {
        return rowLevelDeletion;
    }

    public DeletionTime activeRangeDeletion()
    {
        return activeRangeDeletion;
    }

    public DeletionTime complexColumnDeletion()
    {
        return complexColumnDeletion;
    }

    public ColumnMetadata column()
    {
        return columnMetadata;
    }

    public Cell<byte[]> cell()
    {
        return currentCell;
    }

    public long bytesProcessed()
    {
        return dataFile.getFilePointer();
    }

    public long bytesTotal()
    {
        return dataFile.length();
    }

    public void close()
    {
        FileUtils.closeQuietly(dataFile);
    }
}
