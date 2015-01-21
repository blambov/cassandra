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

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.utils.concurrent.WaitQueue;

import org.slf4j.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.cassandra.db.commitlog.CommitLogSegment.Allocation;

public abstract class AbstractCommitLogService
{
    // how often should we log syngs that lag behind our desired period
    private static final long LAG_REPORT_INTERVAL = TimeUnit.MINUTES.toMillis(5);

    // all Allocations written before this time are synced.
    protected volatile long lastSyncedAt = System.currentTimeMillis();

    // counts of total written, and pending, log messages
    private final AtomicLong written = new AtomicLong(0);
    protected final AtomicLong pending = new AtomicLong(0);

    // signal that writers can wait on to be notified of a completed sync
    protected final WaitQueue syncComplete = new WaitQueue();

    private static final Logger logger = LoggerFactory.getLogger(AbstractCommitLogService.class);
    private final ScheduledExecutorService executor;
    private final Runnable runnable;

    long firstLagAt = 0;
    long totalSyncDuration = 0; // total time spent syncing since firstLagAt
    long syncExceededIntervalBy = 0; // time that syncs exceeded pollInterval since firstLagAt
    int lagCount = 0;
    int syncCount = 0;
    
    volatile boolean shutdown = false;

    /**
     * CommitLogService provides a fsync service for Allocations, fulfilling either the
     * Batch or Periodic contract.
     *
     * Subclasses may be notified when a sync finishes by using the syncComplete WaitQueue.
     */
    AbstractCommitLogService(final CommitLog commitLog, final String name, final long pollIntervalMillis)
    {
        if (pollIntervalMillis < 1)
            throw new IllegalArgumentException(String.format("Commit log flush interval must be positive: %dms", pollIntervalMillis));

        runnable = new Runnable()
        {
            public void run()
            {
                try
                {
                    // always run once after shutdown signalled
                    if (shutdown)
                        executor.shutdown();

                    // sync and signal
                    long syncStarted = System.currentTimeMillis();
                    long syncedAt = commitLog.sync(shutdown, syncStarted);
                    if (syncedAt > lastSyncedAt)
                    {
                        lastSyncedAt = syncedAt;
                        syncComplete.signalAll();
                    }

                    // FIXME: The calculations below should be based on syncedAt.

                    // sleep any time we have left before the next one is due
                    long now = System.currentTimeMillis();
                    long sleep = syncStarted + pollIntervalMillis - now;
                    if (sleep < 0)
                    {
                        // if we have lagged noticeably, update our lag counter
                        if (firstLagAt == 0)
                        {
                            firstLagAt = now;
                            totalSyncDuration = syncExceededIntervalBy = syncCount = lagCount = 0;
                        }
                        syncExceededIntervalBy -= sleep;
                        lagCount++;
                    }
                    syncCount++;
                    totalSyncDuration += now - syncStarted;

                    if (firstLagAt > 0 && now - firstLagAt >= LAG_REPORT_INTERVAL)
                    {
                        logger.warn(String.format("Out of %d commit log syncs over the past %ds with average duration of %.2fms, %d have exceeded the configured commit interval by an average of %.2fms",
                                                  syncCount, (now - firstLagAt) / 1000, (double) totalSyncDuration / syncCount, lagCount, (double) syncExceededIntervalBy / lagCount));
                        firstLagAt = 0;
                    }
                }
                catch (Throwable t)
                {
                    if (!CommitLog.handleCommitError("Failed to persist commits to disk", t))
                        executor.shutdown();
                }
            }
        };

        int threadCount = 1; // DatabaseDescriptor.getCommitLogSyncThreadCount();
        executor = Executors.newScheduledThreadPool(threadCount, new NamedThreadFactory("commit-log-service"));
        for (int i=0; i<threadCount; ++i)
            executor.scheduleAtFixedRate(runnable, pollIntervalMillis * (i + 1), pollIntervalMillis * threadCount, TimeUnit.MILLISECONDS);
    }

    /**
     * Block for @param alloc to be sync'd as necessary, and handle bookkeeping
     */
    public void finishWriteFor(Allocation alloc)
    {
        maybeWaitForSync(alloc);
        written.incrementAndGet();
    }

    protected abstract void maybeWaitForSync(Allocation alloc);

    /**
     * Sync immediately, but don't block for the sync to cmplete
     */
    public void requestExtraSync()
    {
        executor.submit(runnable);
    }

    /**
     * Sync immediately and block until the sync is complete.
     */
    public void blockingSync()
    {
        long started = System.currentTimeMillis();
        executor.submit(runnable);
        while (started > lastSyncedAt)
        {
            WaitQueue.Signal signal = syncComplete.register(CommitLog.instance.metrics.waitingOnCommit.time());
            if (started > lastSyncedAt)
                signal.awaitUninterruptibly();
            else
                signal.cancel();
        }
    }

    public void shutdown()
    {
        shutdown = true;
        requestExtraSync();
    }

    public void awaitTermination() throws InterruptedException
    {
        blockingSync();
        executor.shutdown();
        executor.awaitTermination(3600, TimeUnit.SECONDS);
    }

    public long getCompletedTasks()
    {
        return written.incrementAndGet();
    }

    public long getPendingTasks()
    {
        return pending.incrementAndGet();
    }
}