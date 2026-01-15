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

package org.apache.cassandra.db.tries;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;

public interface ContentSerializer<T>
{
    // size cannot be more than 32 bytes
    int serializedSizeOrSpecial(T content, boolean shouldPresentAfterBranch);

    T special(int id);

    boolean shouldPreserveWithoutChildren(int id);

    // Has serialized size bytes to work with
    void serialize(T content, boolean shouldPresentAfterBranch, UnsafeBuffer buffer, int offset);

    // Must know/store the length of the payload
    T deserialize(UnsafeBuffer buffer, int offset);

    // uses same shouldPresentAfterBranch value
    boolean setInPlace(UnsafeBuffer buffer, int offset, T newContent);

    boolean releaseNeeded(int id);

    void releaseContent(UnsafeBuffer buffer, int offset);

    boolean shouldPresentSpecialAfterBranch(int id);

    boolean shouldPresentAfterBranch(UnsafeBuffer buffer, int offset);

    void completeMutation();

    void abortMutation();

    long usedSizeOnHeap();

    long usedSizeOffHeap();

    String dumpSpecial(int id);

    default String dumpContent(UnsafeBuffer buffer, int offset)
    {
        return ByteBufferUtil.bytesToHex(buffer.byteBuffer().duplicate().position(offset).limit(offset + 32));
    }
}
