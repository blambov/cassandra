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
import java.util.Iterator;
import java.util.function.Function;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.partitions.TrieBackedPartition;
import org.apache.cassandra.db.tries.DeletionAwareTrie;
import org.apache.cassandra.db.tries.Direction;
import org.apache.cassandra.db.tries.InMemoryDeletionAwareTrie;
import org.apache.cassandra.db.tries.Trie;
import org.apache.cassandra.db.tries.TrieEntriesIterator;
import org.apache.cassandra.db.tries.TrieSpaceExhaustedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;
import org.apache.cassandra.utils.memory.Cloner;

import static org.apache.cassandra.db.partitions.TrieBackedPartition.BYTE_COMPARABLE_VERSION;

/**
 * The data for a complex column, that is it's cells and potential complex
 * deletion time.
 */
public class TrieBackedComplexColumn extends ComplexColumnData
{
    static final DeletionAwareTrie<Object, TrieTombstoneMarker> NO_CELLS = DeletionAwareTrie.empty(BYTE_COMPARABLE_VERSION);

    // The cells for 'column' sorted by cell path.
    private final DeletionAwareTrie<Object, TrieTombstoneMarker> data;

    TrieBackedComplexColumn(ColumnMetadata column, DeletionAwareTrie<Object, TrieTombstoneMarker> data)
    {
        super(column);
        assert column.isComplex();
        this.data = data;
    }

    // Used by CNDB
    public boolean hasCells() {
        return data.contentOnlyTrie().valueIterator().hasNext();
    }

    public int cellsCount()
    {
        return Iterators.size(data.contentOnlyTrie().valueIterator());
    }

    public Cell<?> getCell(CellPath path)
    {
        // TODO: change users to avoid
        // TODO: deleted cells?
        return (Cell<?>) data.contentOnlyTrie().get(path);
    }

    public Cell<?> getCellByIndex(int idx)
    {
        // TODO: should deleted cells be included?
        return (Cell<?>) Iterators.get(data.contentOnlyTrie().valueIterator(), idx, null);
    }

    /**
     * The complex deletion time of the complex column.
     * <p>
     * The returned "complex deletion" is a deletion of all the cells of the column. For instance,
     * for a collection, this correspond to a full collection deletion.
     * Please note that this deletion says nothing about the individual cells of the complex column:
     * there can be no complex deletion but some of the individual cells can be deleted.
     *
     * @return the complex deletion time for the column this is the data of or {@code DeletionTime.LIVE}
     * if the column is not deleted.
     */
    public DeletionTime complexDeletion()
    {
        return TrieTombstoneMarker.deletionOfCovering(data.deletionOnlyTrie().applicableRange(ByteComparable.EMPTY));
    }

    DeletionAwareTrie<Object, TrieTombstoneMarker> tree()
    {
        return data;
    }

    static class CellsWithPath extends TrieEntriesIterator<Object, Cell<?>>
    {
        // TODO: deleted cells?
        protected CellsWithPath(Trie<Object> trie, Direction direction)
        {
            super(trie, direction, Predicates.alwaysTrue());
        }

        @Override
        protected Cell<?> mapContent(Object content, byte[] bytes, int byteLength)
        {
            if (!(content instanceof Cell))
                return null;

            Cell<?> c = (Cell<?>) content;
            if (c.path() != null)
                return c;
            ByteSource.Peekable pathBytes = ByteSource.preencoded(bytes, 0, byteLength);
            return c.withPath(CellPath.create(ByteBuffer.wrap(ByteSourceInverse.getUnescapedBytes(pathBytes))));
        }
    }

    public Iterator<Cell<?>> iterator()
    {
        // TODO: what about deleted cells?
        return new CellsWithPath(data.contentOnlyTrie(), Direction.FORWARD);
    }

    public Iterator<Cell<?>> reverseIterator()
    {
        // TODO: what about deleted cells?
        return new CellsWithPath(data.contentOnlyTrie(), Direction.REVERSE);
    }

    @Override
    public ComplexColumnData transformAndFilter(Function<? super Cell<?>, ? extends Cell<?>> function)
    {
        InMemoryDeletionAwareTrie<Object, TrieTombstoneMarker> transformedData = InMemoryDeletionAwareTrie.shortLived(BYTE_COMPARABLE_VERSION);
        try
        {
            transformedData.apply(data,
                                  (empty, v) -> function.apply((Cell<?>) v),
                                  TrieBackedPartition.mergeTombstoneRanges(),
                                  TrieBackedPartition.noIncomingSelfDeletion(),
                                  TrieBackedPartition.noExistingSelfDeletion(),
                                  true,
                                  Predicates.alwaysFalse());
        }
        catch (TrieSpaceExhaustedException e)
        {
            throw new AssertionError(e);
        }
        return new TrieBackedComplexColumn(column, transformedData);
    }

    @Override
    public <V> ComplexColumnData transform(Function<? super Cell<?>, ? extends Cell<?>> function)
    {
        return transformAndFilter(function);
    }

    @Override
    public long accumulate(LongAccumulator<Cell<?>> accumulator, long initialValue)
    {
        class Accumulator implements DeletionAwareTrie.ValueConsumer<Object, TrieTombstoneMarker>
        {
            long longValue;

            @Override
            public void deletionMarker(TrieTombstoneMarker marker)
            {
                // TODO: process deleted cells?
            }

            @Override
            public void content(Object content)
            {
                // TODO: does this need paths?
                longValue = accumulator.apply((Cell<?>) content, longValue);
            }
        }
        Accumulator consumer = new Accumulator();
        data.process(Direction.FORWARD, consumer);
        return consumer.longValue;
    }

    @Override
    public <A> long accumulate(BiLongAccumulator<A, Cell<?>> accumulator, A arg, long initialValue)
    {
        class Accumulator implements DeletionAwareTrie.ValueConsumer<Object, TrieTombstoneMarker>
        {
            long longValue;

            @Override
            public void deletionMarker(TrieTombstoneMarker marker)
            {
                // TODO: process deleted cells?
            }

            @Override
            public void content(Object content)
            {
                // TODO: does this need paths?
                longValue = accumulator.apply(arg, (Cell<?>) content, longValue);
            }
        }
        Accumulator consumer = new Accumulator();
        data.process(Direction.FORWARD, consumer);
        return consumer.longValue;
    }

    public int dataSize()
    {
        throw new AssertionError("Should be collected by TrieBackedRow");
    }

    @Override
    public int liveDataSize(int nowInSec)
    {
        throw new AssertionError("Should be collected by TrieBackedRow");
    }

    public long unsharedHeapSizeExcludingData()
    {
        throw new AssertionError("Should be collected by TrieBackedRow");
    }

    public void validate()
    {
        throw new AssertionError("Should be done by TrieBackedRow");
    }

    public void digest(Digest digest)
    {
        throw new AssertionError("Should be collected by TrieBackedRow");
    }

    public boolean hasInvalidDeletions()
    {
        throw new AssertionError("Should be collected by TrieBackedRow");
    }

    public TrieBackedComplexColumn markCounterLocalToBeCleared()
    {
        throw new AssertionError("Should be done by TrieBackedRow");
    }

    public TrieBackedComplexColumn purge(DeletionPurger purger, int nowInSec)
    {
        throw new AssertionError("Should be done by TrieBackedRow");
//        DeletionTime newDeletion = complexDeletion.isLive() || purger.shouldPurge(complexDeletion) ? DeletionTime.LIVE : complexDeletion;
//        return transformAndFilter(newDeletion, (cell) -> cell.purge(purger, nowInSec));
    }

    @Override
    public ColumnData clone(Cloner cloner)
    {
        throw new AssertionError("Should be done by TrieBackedRow");
//        return transform(c -> cloner.clone(c));
    }

    public TrieBackedComplexColumn updateAllTimestamp(long newTimestamp)
    {
        throw new AssertionError("Should be done by TrieBackedRow");
//        DeletionTime newDeletion = complexDeletion.isLive() ? complexDeletion : new DeletionTime(newTimestamp - 1, complexDeletion.localDeletionTime());
//        return transformAndFilter(newDeletion, (cell) -> (Cell<?>) cell.updateAllTimestamp(newTimestamp));
    }

    public long maxTimestamp()
    {
        throw new AssertionError("Should be collected by TrieBackedRow");
//        long timestamp = complexDeletion.markedForDeleteAt();
//        for (Cell<?> cell : this)
//            timestamp = Math.max(timestamp, cell.timestamp());
//        return timestamp;
    }

    public long minTimestamp()
    {
        throw new AssertionError("Should be collected by TrieBackedRow");
//        long timestamp = complexDeletion.isLive()
//                         ? Long.MAX_VALUE
//                         : complexDeletion.markedForDeleteAt();
//        for (Cell cell : this)
//            timestamp = Math.min(timestamp, cell.timestamp());
//        return timestamp;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other)
            return true;

        if(!(other instanceof TrieBackedComplexColumn))
            return false;

        TrieBackedComplexColumn that = (TrieBackedComplexColumn)other;
        return this.column().equals(that.column())
               && Iterables.elementsEqual(this, that);
    }

    @Override
    public int hashCode()
    {
        throw new AssertionError("Should not be used");
//        return Objects.hash(column(), complexDeletion(), Iterables.
//                            BTree.hashCode(cells));
    }

    @Override
    public String toString()
    {
        return String.format("[%s=%s %s]",
                             column().name,
                             complexDeletion(),
                             Iterators.toString(iterator()));
    }
}
