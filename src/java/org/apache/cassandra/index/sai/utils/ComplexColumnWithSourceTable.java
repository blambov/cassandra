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

package org.apache.cassandra.index.sai.utils;

import java.util.Iterator;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Digest;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.ColumnData;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.utils.BiLongAccumulator;
import org.apache.cassandra.utils.LongAccumulator;
import org.apache.cassandra.utils.memory.Cloner;

public class ComplexColumnWithSourceTable extends ComplexColumnData
{
    private final ComplexColumnData source;
    private final Object sourceTable;

    public ComplexColumnWithSourceTable(ComplexColumnData source, Object sourceTable)
    {
        super(source.column());
        this.source = source;
        this.sourceTable = sourceTable;
    }

    public Object sourceTable()
    {
        return sourceTable;
    }

    @Override
    public int dataSize()
    {
        return source.dataSize();
    }

    @Override
    public int liveDataSize(int nowInSec)
    {
        return source.liveDataSize(nowInSec);
    }

    @Override
    public long unsharedHeapSizeExcludingData()
    {
        return source.unsharedHeapSizeExcludingData();
    }

    @Override
    public void validate()
    {
        source.validate();
    }

    @Override
    public boolean hasInvalidDeletions()
    {
        return source.hasInvalidDeletions();
    }

    @Override
    public void digest(Digest digest)
    {
        source.digest(digest);
    }

    @Override
    public ColumnData clone(Cloner cloner)
    {
        return null;
    }

    @Override
    public ComplexColumnData updateAllTimestamp(long newTimestamp)
    {
        return wrapIfNew(((ComplexColumnData) source.updateAllTimestamp(newTimestamp)));
    }

    @Override
    public ComplexColumnData markCounterLocalToBeCleared()
    {
        return wrapIfNew(((ComplexColumnData) source.markCounterLocalToBeCleared()));
    }

    @Override
    public boolean hasCells()
    {
        return source.hasCells();
    }

    @Override
    public int cellsCount()
    {
        return source.cellsCount();
    }

    private Cell<?> wrapCell(Cell<?> c)
    {
        return c != null ? new CellWithSourceTable<>(c, source) : null;
    }

    @Override
    public Cell<?> getCell(CellPath path)
    {
        return wrapCell(source.getCell(path));
    }

    @Override
    public Cell<?> getCellByIndex(int idx)
    {
        return wrapCell(source.getCellByIndex(idx));
    }

    @Override
    public DeletionTime complexDeletion()
    {
        return source.complexDeletion();
    }

    @Override
    public Iterator<Cell<?>> iterator()
    {
        return Iterators.transform(source.iterator(), this::wrapCell);
    }

    @Override
    public Iterator<Cell<?>> reverseIterator()
    {
        return Iterators.transform(source.reverseIterator(), this::wrapCell);
    }

    @Override
    public long accumulate(LongAccumulator<Cell<?>> accumulator, long initialValue)
    {
        return source.accumulate(accumulator, initialValue);
    }

    @Override
    public <A> long accumulate(BiLongAccumulator<A, Cell<?>> accumulator, A arg, long initialValue)
    {
        return source.accumulate(accumulator, arg, initialValue);
    }

    @Override
    public ComplexColumnData purge(DeletionPurger purger, int nowInSec)
    {
        return wrapIfNew(source.purge(purger, nowInSec));
    }

    @Override
    public long maxTimestamp()
    {
        return source.maxTimestamp();
    }

    @Override
    public long minTimestamp()
    {
        return source.minTimestamp();
    }

    private ComplexColumnData wrapIfNew(ComplexColumnData maybeNewCell)
    {
        if (maybeNewCell == null)
            return null;
        // If the source's method returned a reference to the same source, then
        // we can skip creating a new wrapper.
        if (maybeNewCell == this.source)
            return this;
        return new ComplexColumnWithSourceTable(maybeNewCell, sourceTable);
    }
}
