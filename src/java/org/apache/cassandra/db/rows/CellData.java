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

package org.apache.cassandra.db.rows;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.memory.ByteBufferCloner;
import org.apache.cassandra.utils.memory.Cloner;

public interface CellData<V>
{
    public static final int NO_TTL = 0;
    public static final int NO_DELETION_TIME = Integer.MAX_VALUE;

    /**
     * Whether the cell is a counter cell or not.
     *
     * @return whether the cell is a counter cell or not.
     */
    boolean isCounterCell();

    V value();

    ValueAccessor<V> accessor();

    default int valueSize()
    {
        return accessor().size(value());
    }

    default ByteBuffer buffer()
    {
        return accessor().toBuffer(value());
    }

    /**
     * The cell timestamp.
     * <p>
     * @return the cell timestamp.
     */
    long timestamp();

    /**
     * The cell ttl.
     *
     * @return the cell ttl, or {@code NO_TTL} if the cell isn't an expiring one.
     */
    int ttl();

    /**
     * The cell local deletion time.
     *
     * @return the cell local deletion time, or {@code NO_DELETION_TIME} if the cell is neither
     * a tombstone nor an expiring one.
     */
    int localDeletionTime();

    /**
     * Whether the cell is a tombstone or not.
     *
     * @return whether the cell is a tombstone or not.
     */
    default boolean isTombstone()
    {
        return localDeletionTime() != NO_DELETION_TIME && ttl() == NO_TTL;
    }


    /**
     * Whether the cell is an expiring one or not.
     * <p>
     * Note that this only correspond to whether the cell liveness info
     * have a TTL or not, but doesn't tells whether the cell is already expired
     * or not. You should use {@link #isLive} for that latter information.
     *
     * @return whether the cell is an expiring one or not.
     */
    default boolean isExpiring()
    {
        return ttl() != NO_TTL;
    }

    /**
     * Whether the cell is live or not given the current time.
     *
     * @param nowInSec the current time in seconds. This is used to
     * decide if an expiring cell is expired or live.
     * @return whether the cell is live or not at {@code nowInSec}.
     */
    default boolean isLive(int nowInSec)
    {
        return localDeletionTime() == NO_DELETION_TIME || (ttl() != NO_TTL && nowInSec < localDeletionTime());
    }

    default boolean hasInvalidDeletions()
    {
        if (ttl() < 0 || localDeletionTime() < 0 || (isExpiring() && localDeletionTime() == NO_DELETION_TIME))
            return true;
        return false;
    }

    long unsharedHeapSizeExcludingData();


    CellData<?> withUpdatedTimestampAndLocalDeletionTime(long newTimestamp, int newLocalDeletionTime);

    CellData<?> updateAllTimestamp(long newTimestamp);

    /**
     * Used to apply the same optimization as in {@link Cell.Serializer#deserialize} when
     * the column is not queried but eventhough it's used for digest calculation.
     * @return a cell with an empty buffer as value
     */
    CellData<?> withSkippedValue();

    CellData<?> clone(Cloner cloner);

    CellData<?> clone(ByteBufferCloner cloner);

    CellData<?> purge(DeletionPurger purger, int nowInSec);

    CellData<?> markCounterLocalToBeCleared();

    /**
     * Returns a cell with the same column and path as this one, but with new timestamp and value (deletion time and
     * TTL are set to none).
     * Note that this can and will return a cell/CellData of a different type.
     */
    CellData<?> withNewValue(long timestamp, ByteBuffer value);

    Cell<?> toCell(ColumnMetadata column, CellPath cellPath);
}
