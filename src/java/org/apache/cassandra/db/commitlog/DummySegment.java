/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.cassandra.utils.ByteBufferUtil;

public class DummySegment extends CommitLogSegment
{
    DummySegment(CommitLog commitLog)
    {
        super(commitLog);
    }

    @Override
    FileChannel channel(File logFile) throws IOException
    {
        return null;
    }

    @Override
    int fd(FileChannel channel)
    {
        return 0;
    }

    ByteBuffer createBuffer(CommitLog commitLog)
    {
        return ByteBufferUtil.EMPTY_BYTE_BUFFER;
    }

    void writeHeader(ByteBuffer buffer, CommitLogDescriptor descriptor)
    {
    }

    void write(int lastSyncedOffset, int nextMarker)
    {
        throw new IllegalStateException();
    }

    public long onDiskSize()
    {
        return 0;
    }
}
