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
import java.util.Iterator;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

// so we can instantiate anonymous classes implementing both interfaces
public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable
{
    public void close();
    
    public static interface Closer
    {
        void close();
    }

    public default Closer closer()
    {
        return this::close;
    }

    /**
     * Turns iteration into a next supplier, which allows for easy transformation.
     * Note: Can only get supplier once, and cannot use iteration after supplier is passed.
     * Supplier _can_ be used after hasNext / isEmpty.
     */
    public default Supplier<T> supplier()
    {
        return () -> hasNext() ? Preconditions.checkNotNull(next()) : null;
    }
}
