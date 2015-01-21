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
package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CLibrary;
import org.apache.cassandra.utils.PureJavaCrc32;
import org.apache.cassandra.utils.concurrent.OpOrder;
import org.apache.cassandra.utils.concurrent.WaitQueue;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A single commit log file on disk. Manages creation of the file and writing mutations to disk,
 * as well as tracking the last mutation position of any "dirty" CFs covered by the segment file. Segment
 * files are initially allocated to a fixed size and can grow to accomidate a larger value if necessary.
 */
public abstract class CommitLogSegment
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogSegment.class);

    private final static long idBase;
    private final static AtomicInteger nextId = new AtomicInteger(1);
    static
    {
        long maxId = Long.MIN_VALUE;
        for (File file : new File(DatabaseDescriptor.getCommitLogLocation()).listFiles())
        {
            if (CommitLogDescriptor.isValid(file.getName()))
                maxId = Math.max(CommitLogDescriptor.fromFileName(file.getName()).id, maxId);
        }
        idBase = Math.max(System.currentTimeMillis(), maxId + 1);
    }

    // The commit log entry overhead in bytes (int: length + int: head checksum + int: tail checksum)
    public static final int ENTRY_OVERHEAD_SIZE = 4 + 4 + 4;

    // The commit log (chained) sync marker/header size in bytes (int: length + int: checksum [segmentId, position])
    static final int SYNC_MARKER_SIZE = 4 + 4;

    // The OpOrder used to order appends wrt sync
    private final OpOrder appendOrder = new OpOrder();

    private final AtomicInteger allocatePosition = new AtomicInteger();

    // Everything before this offset has been synced and written.  The SYNC_MARKER_SIZE bytes after
    // each sync are reserved, and point forwards to the next such offset.  The final
    // sync marker in a segment will be zeroed out, or point to EOF.
    private volatile int lastSyncedOffset;
    // The position where sync was initiated last.
    private volatile int lastSyncStartedOffset;

    // a signal for writers to wait on to confirm the log message they provided has been written to disk
    private final WaitQueue syncComplete = new WaitQueue();

    // a map of Cf->dirty position; this is used to permit marking Cfs clean whilst the log is still in use
    private final NonBlockingHashMap<UUID, AtomicInteger> cfDirty = new NonBlockingHashMap<>(1024);

    // a map of Cf->clean position; this is used to permit marking Cfs clean whilst the log is still in use
    private final ConcurrentHashMap<UUID, AtomicInteger> cfClean = new ConcurrentHashMap<>();

    public final long id;

    protected RandomAccessFile logFileAccessor;
    int fd;
    protected ByteBuffer buffer;
    protected int bufferSize;
    protected final File logFile;

    public final CommitLogDescriptor descriptor;

    /**
     * @return a newly minted segment file
     */
    static CommitLogSegment freshSegment()
    {
        return createSegment(null);
    }

    static CommitLogSegment createSegment(String reusePath)
    {
        return CommitLog.compressor != null ? new CompressedSegment(reusePath) : new MemoryMappedSegment(reusePath);
    }

    static long getNextId()
    {
        return idBase + nextId.getAndIncrement();
    }

    /**
     * Constructs a new segment file.
     *
     * @param filePath  if not null, recycles the existing file by renaming it and truncating it to CommitLog.SEGMENT_SIZE.
     */
    CommitLogSegment(String filePath)
    {
        id = getNextId();
        descriptor = new CommitLogDescriptor(id);
        logFile = new File(DatabaseDescriptor.getCommitLogLocation(), descriptor.fileName());
        if (filePath != null)
            recycleFile(filePath);

        try
        {
            // Open the initial the segment file
            logFileAccessor = new RandomAccessFile(logFile, "rw");

            fd = CLibrary.getfd(logFileAccessor.getFD());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, logFile);
        }
        
        buffer = createBuffer();
        bufferSize = buffer.capacity();
        // write the header
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        lastSyncedOffset = lastSyncStartedOffset = buffer.position();
        // mark the initial sync marker as uninitialised
        buffer.putInt(lastSyncedOffset + 0, 0);
        buffer.putInt(lastSyncedOffset + 4, 0);
        allocatePosition.set(lastSyncedOffset + SYNC_MARKER_SIZE);
    }

    abstract void recycleFile(String filePath);

    abstract ByteBuffer createBuffer();

    /**
     * Allocate space in this buffer for the provided mutation, and return the allocated Allocation object.
     * Returns null if there is not enough space in this segment, and a new segment is needed.
     */
    Allocation allocate(Mutation mutation, int size)
    {
        final OpOrder.Group opGroup = appendOrder.start();
        try
        {
            int position = allocate(size);
            if (position < 0)
            {
                opGroup.close();
                return null;
            }
            markDirty(mutation, position);
            return new Allocation(this, opGroup, position, (ByteBuffer) buffer.duplicate().position(position).limit(position + size));
        }
        catch (Throwable t)
        {
            opGroup.close();
            throw t;
        }
    }

    // allocate bytes in the segment, or return -1 if not enough space
    private int allocate(int size)
    {
        while (true)
        {
            int prev = allocatePosition.get();
            int next = prev + size;
            if (next >= bufferSize)
                return -1;
            if (allocatePosition.compareAndSet(prev, next))
                return prev;
        }
    }

    // ensures no more of this segment is writeable, by allocating any unused section at the end and marking it discarded
    void discardUnusedTail()
    {
        // we guard this with the OpOrdering instead of synchronised due to potential dead-lock with CLSM.advanceAllocatingFrom()
        // this actually isn't strictly necessary, as currently all calls to discardUnusedTail occur within a block
        // already protected by this OpOrdering, but to prevent future potential mistakes, we duplicate the protection here
        // so that the contract between discardUnusedTail() and sync() is more explicit.
        try (OpOrder.Group group = appendOrder.start())
        {
            while (true)
            {
                bufferSize = allocatePosition.get();
                if (allocatePosition.compareAndSet(bufferSize, bufferSize + 1))
                {
                    return;
                }
            }
        }
    }

    /**
     * Wait for any appends or discardUnusedTail() operations started before this method was called
     */
    void waitForModifications()
    {
        // issue a barrier and wait for it
        appendOrder.awaitNewBarrier();
    }

    /**
     * Forces a disk flush for this segment file.
     * @param syncStarted Timestamp for this sync request (needs to be monotonically increasing). This value or bigger
     *     will be returned when the sync started at that point has retired.
     */
    synchronized long sync(long syncStarted)
    {
        int startMarker;
        int nextMarker;
        boolean close = false;
        // check we have more work to do
        if (allocatePosition.get() <= lastSyncStartedOffset + SYNC_MARKER_SIZE)
            return retireInFlightWrites(syncStarted);

        // allocate a new sync marker; this is both necessary in itself, but also serves to demarcate
        // the point at which we can safely consider records to have been completely written to
        nextMarker = allocate(SYNC_MARKER_SIZE);
        if (nextMarker < 0)
        {
            // ensure no more of this CLS is writeable, and mark ourselves for closing
            discardUnusedTail();
            close = true;

            // wait for modifications guards both discardedTailFrom, and any outstanding appends
            waitForModifications();

            if (bufferSize < buffer.capacity() - SYNC_MARKER_SIZE)
            {
                // if there's room in the discard section to write an empty header, use that as the nextMarker
                nextMarker = bufferSize;
            }
            else
            {
                // not enough space left in the buffer, so mark the next sync marker as the EOF position
                nextMarker = buffer.capacity();
            }
        }
        else
        {
            waitForModifications();
        }
    
        assert nextMarker > lastSyncStartedOffset;

        // write previous sync marker to point to next sync marker
        // we don't chain the crcs here to ensure this method is idempotent if it fails
        writeSyncMarker(buffer, lastSyncStartedOffset, lastSyncStartedOffset, nextMarker);

        // zero out the next sync marker so replayer can cleanly exit
        if (nextMarker < buffer.capacity())
        {
            buffer.putInt(nextMarker, 0);
            buffer.putInt(nextMarker + 4, 0);
        }
        startMarker = lastSyncStartedOffset;
        lastSyncStartedOffset = nextMarker;

        write(startMarker, nextMarker, syncStarted, close);
        return retireInFlightWrites(syncStarted);
    }
        
    void retireWrite(int nextMarker, boolean close)
    {
        if (close) {
            close();
        }
        lastSyncedOffset = nextMarker;
        syncComplete.signalAll();
    }

    protected void writeSyncMarker(ByteBuffer buffer, int offset, int filePos, int nextMarker)
    {
        final PureJavaCrc32 crc = new PureJavaCrc32();
        crc.updateInt((int) (id & 0xFFFFFFFFL));
        crc.updateInt((int) (id >>> 32));
        crc.updateInt(filePos);
        buffer.putInt(offset, nextMarker);
        buffer.putInt(offset + 4, crc.getCrc());
    }

    abstract void write(int lastSyncedOffset, int nextMarker, long syncTimestamp, boolean close);
    abstract long retireInFlightWrites(long syncTimestamp);

    public boolean isStillAllocating()
    {
        return allocatePosition.get() < bufferSize;
    }

    /**
     * Completely discards a segment file by deleting it. (Potentially blocking operation)
     */
    void delete()
    {
       FileUtils.deleteWithConfirm(logFile);
    }

    /**
     * Recycle processes an unneeded segment file for reuse.
     *
     * @return a new CommitLogSegment representing the newly reusable segment.
     */
    abstract CommitLogSegment recycle();

    /**
     * @return the current ReplayPosition for this log segment
     */
    public ReplayPosition getContext()
    {
        return new ReplayPosition(id, allocatePosition.get());
    }

    /**
     * @return the file path to this segment
     */
    public String getPath()
    {
        return logFile.getPath();
    }

    /**
     * @return the file name of this segment
     */
    public String getName()
    {
        return logFile.getName();
    }

    void waitForFinalSync()
    {
        while (true)
        {
            WaitQueue.Signal signal = syncComplete.register();
            if (lastSyncedOffset < bufferSize)
            {
                signal.awaitUninterruptibly();
            }
            else
            {
                signal.cancel();
                break;
            }
        }
    }

    void waitForSync(int position)
    {
        while (lastSyncedOffset < position)
        {
            WaitQueue.Signal signal = syncComplete.register();
            if (lastSyncedOffset < position)
                signal.awaitUninterruptibly();
            else
                signal.cancel();
        }
    }

    /**
     * Close the segment file.
     */
    void close()
    {
        try
        {
            logFileAccessor.close();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    void markDirty(Mutation mutation, int allocatedPosition)
    {
        for (ColumnFamily columnFamily : mutation.getColumnFamilies())
        {
            // check for deleted CFS
            CFMetaData cfm = columnFamily.metadata();
            if (cfm.isPurged())
                logger.error("Attempted to write commit log entry for unrecognized table: {}", columnFamily.id());
            else
                ensureAtleast(cfDirty, cfm.cfId, allocatedPosition);
        }
    }

    /**
     * Marks the ColumnFamily specified by cfId as clean for this log segment. If the
     * given context argument is contained in this file, it will only mark the CF as
     * clean if no newer writes have taken place.
     *
     * @param cfId    the column family ID that is now clean
     * @param context the optional clean offset
     */
    public synchronized void markClean(UUID cfId, ReplayPosition context)
    {
        if (!cfDirty.containsKey(cfId))
            return;
        if (context.segment == id)
            markClean(cfId, context.position);
        else if (context.segment > id)
            markClean(cfId, Integer.MAX_VALUE);
    }

    private void markClean(UUID cfId, int position)
    {
        ensureAtleast(cfClean, cfId, position);
        removeCleanFromDirty();
    }

    private static void ensureAtleast(ConcurrentMap<UUID, AtomicInteger> map, UUID cfId, int value)
    {
        AtomicInteger i = map.get(cfId);
        if (i == null)
        {
            AtomicInteger i2 = map.putIfAbsent(cfId, i = new AtomicInteger());
            if (i2 != null)
                i = i2;
        }
        while (true)
        {
            int cur = i.get();
            if (cur > value)
                break;
            if (i.compareAndSet(cur, value))
                break;
        }
    }

    private void removeCleanFromDirty()
    {
        // if we're still allocating from this segment, don't touch anything since it can't be done thread-safely
        if (isStillAllocating())
            return;

        Iterator<Map.Entry<UUID, AtomicInteger>> iter = cfClean.entrySet().iterator();
        while (iter.hasNext())
        {
            Map.Entry<UUID, AtomicInteger> clean = iter.next();
            UUID cfId = clean.getKey();
            AtomicInteger cleanPos = clean.getValue();
            AtomicInteger dirtyPos = cfDirty.get(cfId);
            if (dirtyPos != null && dirtyPos.intValue() <= cleanPos.intValue())
            {
                cfDirty.remove(cfId);
                iter.remove();
            }
        }
    }

    /**
     * @return a collection of dirty CFIDs for this segment file.
     */
    public synchronized Collection<UUID> getDirtyCFIDs()
    {
        if (cfClean.isEmpty() || cfDirty.isEmpty())
            return cfDirty.keySet();

        List<UUID> r = new ArrayList<>(cfDirty.size());
        for (Map.Entry<UUID, AtomicInteger> dirty : cfDirty.entrySet())
        {
            UUID cfId = dirty.getKey();
            AtomicInteger dirtyPos = dirty.getValue();
            AtomicInteger cleanPos = cfClean.get(cfId);
            if (cleanPos == null || cleanPos.intValue() < dirtyPos.intValue())
                r.add(dirty.getKey());
        }
        return r;
    }

    /**
     * @return true if this segment is unused and safe to recycle or delete
     */
    public synchronized boolean isUnused()
    {
        // if room to allocate, we're still in use as the active allocatingFrom,
        // so we don't want to race with updates to cfClean with removeCleanFromDirty
        if (isStillAllocating())
            return false;

        removeCleanFromDirty();
        return cfDirty.isEmpty();
    }

    /**
     * Check to see if a certain ReplayPosition is contained by this segment file.
     *
     * @param   context the replay position to be checked
     * @return  true if the replay position is contained by this segment file.
     */
    public boolean contains(ReplayPosition context)
    {
        return context.segment == id;
    }

    // For debugging, not fast
    public String dirtyString()
    {
        StringBuilder sb = new StringBuilder();
        for (UUID cfId : getDirtyCFIDs())
        {
            CFMetaData m = Schema.instance.getCFMetaData(cfId);
            sb.append(m == null ? "<deleted>" : m.cfName).append(" (").append(cfId).append("), ");
        }
        return sb.toString();
    }

    @Override
    public String toString()
    {
        return "CommitLogSegment(" + getPath() + ')';
    }

    public static class CommitLogSegmentFileComparator implements Comparator<File>
    {
        public int compare(File f, File f2)
        {
            CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(f.getName());
            CommitLogDescriptor desc2 = CommitLogDescriptor.fromFileName(f2.getName());
            return (int) (desc.id - desc2.id);
        }
    }

    /**
     * A simple class for tracking information about the portion of a segment that has been allocated to a log write.
     * The constructor leaves the fields uninitialized for population by CommitlogManager, so that it can be
     * stack-allocated by escape analysis in CommitLog.add.
     */
    static class Allocation
    {

        private final CommitLogSegment segment;
        private final OpOrder.Group appendOp;
        private final int position;
        private final ByteBuffer buffer;

        Allocation(CommitLogSegment segment, OpOrder.Group appendOp, int position, ByteBuffer buffer)
        {
            this.segment = segment;
            this.appendOp = appendOp;
            this.position = position;
            this.buffer = buffer;
        }

        CommitLogSegment getSegment()
        {
            return segment;
        }

        ByteBuffer getBuffer()
        {
            return buffer;
        }

        // markWritten() MUST be called once we are done with the segment or the CL will never flush
        // but must not be called more than once
        void markWritten()
        {
            appendOp.close();
        }

        void awaitDiskSync()
        {
            while (segment.lastSyncedOffset < position)
            {
                WaitQueue.Signal signal = segment.syncComplete.register(CommitLog.instance.metrics.waitingOnCommit.time());
                if (segment.lastSyncedOffset < position)
                    signal.awaitUninterruptibly();
                else
                    signal.cancel();
            }
        }

        public ReplayPosition getReplayPosition()
        {
            return new ReplayPosition(segment.id, buffer.limit());
        }

    }

}
