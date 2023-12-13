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

package org.apache.cassandra.index.sai.disk.vector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.IntUnaryOperator;
import javax.annotation.concurrent.NotThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import io.github.jbellis.jvector.util.RamUsageEstimator;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.io.util.SequentialWriter;

@NotThreadSafe
public class CompactionVectorValues implements RamAwareVectorValues
{
    private final int dimension;
    private final ArrayList<ByteBuffer> values = new ArrayList<>();
    private final VectorType<Float> type;

    public CompactionVectorValues(VectorType<Float> type)
    {
        this.dimension = type.dimension;
        this.type = type;
    }

    @Override
    public int size()
    {
        return values.size();
    }

    @Override
    public int dimension()
    {
        return dimension;
    }

    @Override
    public float[] vectorValue(int i)
    {
        return type.getSerializer().deserializeFloatArray(values.get(i));
    }

    /** return approximate bytes used by the new vector */
    public long add(int ordinal, ByteBuffer value)
    {
        if (ordinal != values.size())
            throw new IllegalArgumentException(String.format("CVV requires vectors to be added in ordinal order (%d given, expected %d)",
                                                             ordinal, values.size()));
        values.add(value);
        return RamEstimation.concurrentHashMapRamUsed(1) + oneVectorBytesUsed();
    }

    @Override
    public CompactionVectorValues copy()
    {
        return this;
    }

    @VisibleForTesting
    public long write(SequentialWriter writer, IntUnaryOperator ordinalMapper) throws IOException
    {
        writer.writeInt(size());
        writer.writeInt(dimension());

        for (var i = 0; i < size(); i++) {
            int ord = ordinalMapper.applyAsInt(i);
            var bb = values.get(ord);
            assert bb != null : "null vector at index " + i + " of " + size();
            writer.write(bb);
        }

        return writer.position();
    }

    @Override
    public boolean isValueShared()
    {
        return false;
    }

    public long ramBytesUsed()
    {
        long REF_BYTES = RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        return 2 * REF_BYTES
               + RamEstimation.concurrentHashMapRamUsed(values.size())
               + values.size() * oneVectorBytesUsed();
    }

    private long oneVectorBytesUsed()
    {
        return RamUsageEstimator.NUM_BYTES_OBJECT_REF;
    }
}