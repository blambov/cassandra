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

package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.RangeTombstoneBoundaryMarker;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.WrappingUnfilteredRowIterator;

class RecombiningUnfilteredRowIterator extends WrappingUnfilteredRowIterator
{
    Unfiltered bufferedOne;
    Unfiltered bufferedTwo;
    Unfiltered next;
    boolean nextPrepared;

    protected RecombiningUnfilteredRowIterator(UnfilteredRowIterator wrapped)
    {
        super(wrapped);
        bufferedOne = wrapped.hasNext() ? wrapped.next() : null;
        nextPrepared = false;
    }

    @Override
    public boolean hasNext()
    {
        return computeNext() != null;
    }

    @Override
    public Unfiltered next()
    {
        Unfiltered next = computeNext();
        nextPrepared = false;
        return next;
    }

    private Unfiltered computeNext()
    {
        if (nextPrepared)
            return next;

        if (bufferedOne == null)
        {
            next = null;
        }
        else if (bufferedTwo != null)
        {
            next = bufferedOne;
            bufferedOne = bufferedTwo;
            bufferedTwo = null;
        }
        else if (bufferedOne.isRow())
        {
            next = bufferedOne;
            bufferedOne = wrapped.hasNext() ? wrapped.next() : null;
        }
        else
        {
            RangeTombstoneMarker marker1 = (RangeTombstoneMarker) bufferedOne;
            boolean reversed = isReverseOrder();
            int clusteringSize = metadata().comparator.size();
            bufferedOne = wrapped.hasNext() ? wrapped.next() : null;
            if (!marker1.isOpen(reversed)
                || !marker1.openIsInclusive(reversed)
                || marker1.clustering().size() != clusteringSize
                || (clusteringSize > 0 && marker1.clustering().get(clusteringSize - 1) == null))
                next = marker1;
            else
            {
                RangeTombstoneMarker marker2;
                final DeletionTime deletionTime = marker1.openDeletionTime(reversed);
                if (bufferedOne == null || bufferedOne.isRangeTombstoneMarker())
                {
                    marker2 = (RangeTombstoneMarker) bufferedOne;
                    assert marker2.isClose(reversed);
                    if (!marker2.closeIsInclusive(reversed)
                        || !marker2.closeDeletionTime(reversed).equals(deletionTime)
                        || !clusteringPositionsEqual(marker1, marker2))
                        next = marker1;
                    else
                    {
                        // The recombination applies. We have to transform the open side of marker1 and the close side
                        // of marker2 into a row with deletion time.
                        next = BTreeRow.emptyDeletedRow(clusteringPositionOf(marker1), Row.Deletion.regular(deletionTime));
                        processOtherSidesAndAdjustBuffered(reversed, marker1, marker2, deletionTime);
                    }
                }
                else
                {
                    if (!clusteringPositionsEqual(marker1, bufferedOne))
                        next = marker1;
                    else
                    {
                        bufferedTwo = wrapped.hasNext() ? wrapped.next() : null;
                        if (bufferedTwo == null || bufferedTwo.isRow())
                            next = marker1;
                        else
                        {
                            marker2 = (RangeTombstoneMarker) bufferedTwo;
                            if (!marker2.closeIsInclusive(reversed) || !clusteringPositionsEqual(marker1, marker2))
                                next = marker1;
                            else
                            {
                                BTreeRow row = (BTreeRow) bufferedOne;
                                bufferedTwo = null;
                                next = BTreeRow.create(row.clustering(), row.primaryKeyLivenessInfo(), Row.Deletion.regular(deletionTime), row.getBTree());
                                processOtherSidesAndAdjustBuffered(reversed, marker1, marker2, deletionTime);
                            }
                        }
                    }
                }
            }
        }

        nextPrepared = true;
        return next;
    }

    private void processOtherSidesAndAdjustBuffered(boolean reversed, RangeTombstoneMarker marker1, RangeTombstoneMarker marker2, DeletionTime deletionTime)
    {
        // Check if any of the markers is a boundary, and if so, report the other side.
        if (marker1.isClose(reversed))
        {
            final DeletionTime closeDeletionTime = marker1.closeDeletionTime(reversed);
            if (marker2.isOpen(reversed) && marker2.openDeletionTime(reversed).equals(closeDeletionTime) && !closeDeletionTime.supersedes(deletionTime))
            {
                // The row interrupts a covering deletion, we can still drop both markers and report a
                // deleted row.
                bufferedOne = wrapped.hasNext() ? wrapped.next() : null;
            }
            else
            {
                if (marker2.isOpen(reversed))
                    bufferedTwo = ((RangeTombstoneBoundaryMarker) marker2).createCorrespondingOpenMarker(reversed);

                bufferedOne = next;
                next = ((RangeTombstoneBoundaryMarker) marker1).createCorrespondingCloseMarker(reversed);
            }
        }
        else if (marker2.isOpen(reversed))
        {
            bufferedOne = ((RangeTombstoneBoundaryMarker) marker2).createCorrespondingOpenMarker(reversed);
        }
        else
        {
            bufferedOne = wrapped.hasNext() ? wrapped.next() : null;
        }
    }

    static boolean clusteringPositionsEqual(Unfiltered l, Unfiltered r)
    {
        return clusteringPositionsEqual(l.clustering(), r.clustering());
    }

    static <L, R> boolean clusteringPositionsEqual(ClusteringPrefix<L> cl, ClusteringPrefix<R> cr)
    {
        if (cl.size() != cr.size())
            return false;
        for (int i = cl.size() - 1; i >= 0; --i)
            if (cl.accessor().compare(cl.get(i), cr.get(i), cr.accessor()) != 0)
                return false;
        return true;
    }

    static Clustering<?> clusteringPositionOf(Unfiltered unfiltered)
    {
        return clusteringPositionOf(unfiltered.clustering());
    }

    static <V> Clustering<V> clusteringPositionOf(ClusteringPrefix<V> prefix)
    {
        return prefix.accessor().factory().clustering(prefix.getRawValues());
    }
}
