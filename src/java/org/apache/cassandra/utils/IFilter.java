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
package org.apache.cassandra.utils;

import java.io.Closeable;
import java.nio.ByteBuffer;

public interface IFilter extends Closeable
{
    public interface FilterKey {
        ByteBuffer getKey();

        /**
         * Places precomputed murmur3 hash of the key, if available.
         * @param dest Array of size at least two to receive the hash.
         * @return true if precomputed hash was available, false otherwise.
         */
        boolean retrieveCachedFilterHash(long[] dest);
    }

    void add(FilterKey key);

    boolean isPresent(FilterKey key);

    void clear();

    long serializedSize();

    void close();
}
