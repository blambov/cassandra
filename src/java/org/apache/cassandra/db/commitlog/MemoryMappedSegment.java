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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A single commit log file on disk. Manages creation of the file and writing mutations to disk,
 * as well as tracking the last mutation position of any "dirty" CFs covered by the segment file. Segment
 * files are initially allocated to a fixed size and can grow to accomidate a larger value if necessary.
 */
public class MemoryMappedSegment extends CommitLogSegment
{
    private static final Logger logger = LoggerFactory.getLogger(MemoryMappedSegment.class);

    private MappedByteBuffer mappedBuffer;

    /**
     * Constructs a new segment file.
     *
     * @param filePath  if not null, recycles the existing file by renaming it and truncating it to CommitLog.SEGMENT_SIZE.
     */
    MemoryMappedSegment(String filePath)
    {
        super(filePath);
    }

    void recycleFile(String filePath)
    {
        File oldFile = new File(filePath);

        if (oldFile.exists())
        {
            logger.debug("Re-using discarded CommitLog segment for {} from {}", id, filePath);
            if (!oldFile.renameTo(logFile))
                throw new FSWriteError(new IOException("Rename from " + filePath + " to " + id + " failed"), filePath);
        } else {
            logger.debug("Creating new commit log segment {}", logFile.getPath());
        }
    }

    ByteBuffer createBuffer()
    {
        try
        {
            // Map the segment, extending or truncating it to the standard segment size.
            // (We may have restarted after a segment size configuration change, leaving "incorrectly"
            // sized segments on disk.)
            logFileAccessor.setLength(DatabaseDescriptor.getCommitLogSegmentSize());
            mappedBuffer = logFileAccessor.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, DatabaseDescriptor.getCommitLogSegmentSize());
            return mappedBuffer;
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, logFile);
        }
    }

    @Override
    synchronized int write(int lastSyncedOffset, int nextMarker, boolean close)
    {
        try {
            // actually perform the sync and signal those waiting for it
            mappedBuffer.force();
        }
        catch (Exception e) // MappedByteBuffer.force() does not declare IOException but can actually throw it
        {
            throw new FSWriteError(e, getPath());
        }
        if (close) {
            nextMarker = mappedBuffer.capacity();
            close();
        }
        return nextMarker;
    }

    /**
     * Recycle processes an unneeded segment file for reuse.
     *
     * @return a new CommitLogSegment representing the newly reusable segment.
     */
    MemoryMappedSegment recycle()
    {
        try
        {
            sync();
        }
        catch (FSWriteError e)
        {
            logger.error("I/O error flushing {} {}", this, e.getMessage());
            throw e;
        }

        close();

        return new MemoryMappedSegment(getPath());
    }

    @Override
    void close()
    {
        if (FileUtils.isCleanerAvailable())
            FileUtils.clean(mappedBuffer);
        super.close();
    }
}
