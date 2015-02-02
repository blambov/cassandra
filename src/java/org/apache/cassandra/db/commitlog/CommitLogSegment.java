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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
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

import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.core.Timer;

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

    // The end position of the buffer. Initially set to its capacity and updated to point to the last written position
    // as the segment is being closed.
    // No need to be volatile as writes are protected by appendOrder barrier.
    private int endOfBuffer;

    // a signal for writers to wait on to confirm the log message they provided has been written to disk
    private final WaitQueue syncComplete = new WaitQueue();

    // a map of Cf->dirty position; this is used to permit marking Cfs clean whilst the log is still in use
    private final NonBlockingHashMap<UUID, AtomicInteger> cfDirty = new NonBlockingHashMap<>(1024);

    // a map of Cf->clean position; this is used to permit marking Cfs clean whilst the log is still in use
    private final ConcurrentHashMap<UUID, AtomicInteger> cfClean = new ConcurrentHashMap<>();

    public final long id;

    final File logFile;
    final FileChannel channel;
    final int fd;

    ByteBuffer buffer;

    public final CommitLogDescriptor descriptor;

    /**
     * @return a newly minted segment file
     */
    static CommitLogSegment freshSegment(CommitLog commitLog)
    {
        return createSegment(commitLog, null);
    }

    static CommitLogSegment createSegment(CommitLog commitLog, String reusePath)
    {
        return commitLog.compressor != null ? new CompressedSegment(reusePath, commitLog) : new MemoryMappedSegment(reusePath, commitLog);
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
    CommitLogSegment(String filePath, CommitLog commitLog)
    {
        id = getNextId();
        descriptor = new CommitLogDescriptor(id, commitLog.compressorClass);
        logFile = new File(commitLog.location, descriptor.fileName());
        if (filePath != null)
            recycleFile(filePath);

        try
        {
            channel = FileChannel.open(logFile.toPath(), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
            fd = CLibrary.getfd(channel);
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, logFile);
        }
        
        buffer = createBuffer();
        // write the header
        CommitLogDescriptor.writeHeader(buffer, descriptor);
        endOfBuffer = buffer.capacity();
        lastSyncedOffset = lastSyncStartedOffset = buffer.position();
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
            if (next >= endOfBuffer)
                return -1;
            if (allocatePosition.compareAndSet(prev, next))
            {
                assert buffer != null;
                return prev;
            }
        }
    }

    // ensures no more of this segment is writeable, by allocating any unused section at the end and marking it discarded
    void discardUnusedTail()
    {
        // We guard this with the OpOrdering instead of synchronised due to potential dead-lock with CLSM.advanceAllocatingFrom()
        // Ensures endOfBuffer update is reflected in the buffer end position picked up by sync().
        // This actually isn't strictly necessary, as currently all calls to discardUnusedTail are executed either by the thread
        // running sync or within a mutation already protected by this OpOrdering, but to prevent future potential mistakes,
        // we duplicate the protection here so that the contract between discardUnusedTail() and sync() is more explicit.
        try (OpOrder.Group group = appendOrder.start())
        {
            while (true)
            {
                int prev = allocatePosition.get();

                int next = endOfBuffer + 1;
                if (prev >= next)
                {
                    // Already stopped allocating, might also be closed.
                    assert buffer == null || prev == buffer.capacity() + 1;
                    return;
                }
                if (allocatePosition.compareAndSet(prev, next))
                {
                    // Stopped allocating now. Can only succeed once, no further allocation or discardUnusedTail can succeed.
                    endOfBuffer = prev;
                    assert buffer != null && next == buffer.capacity() + 1;
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
     */
    void sync()
    {
        int startMarker;
        int nextMarker;
        boolean close = false;

        synchronized (this) 
        {
            startMarker = lastSyncStartedOffset;

            // Check we have more work to do.
            // Note: Even if the very first allocation of this sync section failed, we still want to enter this
            // to ensure the segment is closed. As allocatePosition is set to 1 beyond the capacity of the buffer,
            // this will always be true when a mutation allocation was attempted after the marker allocation succeeded
            // in the previous sync. 
            if (allocatePosition.get() > startMarker + SYNC_MARKER_SIZE)
            {
                assert buffer != null;  // Only close once.
                // Allocate a new sync marker; this is both necessary in itself, but also serves to demarcate
                // the point at which we can safely consider records to have been completely written to.
                nextMarker = allocate(SYNC_MARKER_SIZE);
                if (nextMarker < 0)
                {
                    // Ensure no more of this CLS is writeable, and mark ourselves for closing.
                    discardUnusedTail();
                    close = true;

                    // The endOfBuffer position may be incorrect at this point (to be written by another stalled thread).
                    // We will wait for the precise end position outside of the synchronized block.
                    nextMarker = buffer.capacity();
                }

                lastSyncStartedOffset = nextMarker;
            } else
                // Nothing needs to be done, but we must still wait for outstanding writes to complete.
                nextMarker = startMarker;
        }
        
        
        if (nextMarker > startMarker)
        {
            // Wait for mutations to complete as well as endOfBuffer to have been written.
            waitForModifications();
            int sectionEnd = close ? endOfBuffer : nextMarker;

            // Perform compression, writing to file and flush. Some or all of this may execute in parallel.
            write(startMarker, sectionEnd);

            // Signal the sync as complete. This section needs to be executed only after any previous syncs have finished.
            waitForSync(startMarker, null);
            assert lastSyncedOffset == startMarker;
            lastSyncedOffset = nextMarker;
            if (close)
                close();
            syncComplete.signalAll();
        } else
            waitForSync(startMarker, null);
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

    abstract void write(int lastSyncedOffset, int nextMarker);

    public boolean isStillAllocating()
    {
        return allocatePosition.get() < endOfBuffer;
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
    abstract CommitLogSegment recycle(CommitLog commitLog);

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
            if (lastSyncedOffset < endOfBuffer)
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

    void waitForSync(int position, Timer waitingOnCommit)
    {
        while (lastSyncedOffset < position)
        {
            WaitQueue.Signal signal = waitingOnCommit != null ?
                                      syncComplete.register(waitingOnCommit.time()) :
                                      syncComplete.register();
            if (lastSyncedOffset < position)
                signal.awaitUninterruptibly();
            else
                signal.cancel();
        }
    }

    /**
     * Close the segment file. Do not call from outside this class, use syncAndClose() instead.
     */
    protected void close()
    {
        try
        {
            channel.close();
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    /**
     * Stop writing to this file, sync and close it. Does nothing if the file is already closed.
     */
    public void syncAndClose()
    {
        discardUnusedTail();
        sync();
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
            return Long.compare(desc.id, desc2.id);
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

        void awaitDiskSync(Timer waitingOnCommit)
        {
            segment.waitForSync(position, waitingOnCommit);
        }

        public ReplayPosition getReplayPosition()
        {
            return new ReplayPosition(segment.id, buffer.limit());
        }

    }
}
