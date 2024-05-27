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
package org.apache.cassandra.io.compress;

import java.nio.ByteBuffer;

import org.agrona.concurrent.UnsafeBuffer;

public enum BufferType
{
    ON_HEAP
    {
        public ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocate(size);
        }

        public void expand(UnsafeBuffer buffer, int newSize)
        {
            // Use byte array directly, saving some unnecessary allocations
            byte[] newBuffer = new byte[newSize];
            buffer.getBytes(0, newBuffer, 0, buffer.capacity());
            buffer.wrap(newBuffer);
        }
    },
    OFF_HEAP
    {
        public ByteBuffer allocate(int size)
        {
            return ByteBuffer.allocateDirect(size);
        }

        public void expand(UnsafeBuffer buffer, int newSize)
        {
            ByteBuffer newBuffer = allocate(newSize);
            buffer.getBytes(0, newBuffer, 0, buffer.capacity());
            buffer.wrap(newBuffer);
        }
    };

    public abstract ByteBuffer allocate(int size);
    public abstract void expand(UnsafeBuffer buffer, int newSize);

    public static BufferType typeOf(ByteBuffer buffer)
    {
        return buffer.isDirect() ? OFF_HEAP : ON_HEAP;
    }
}
