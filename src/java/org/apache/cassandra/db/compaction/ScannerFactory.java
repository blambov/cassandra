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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Collection;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;

public interface ScannerFactory
{
    /**
     * Returns a list of KeyScanners given sstables and a range on which to scan.
     * The default implementation simply grab one SSTableScanner per-sstable, but overriding this method
     * allow for a more memory efficient solution if we know the sstable don't overlap (see
     * LeveledCompactionStrategy for instance).
     */
    ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges);

    /**
     * Returns a list of KeyScanners given sstables and a range on which to scan.
     * The default implementation simply grab one SSTableScanner per-sstable, but overriding this method
     * allow for a more memory efficient solution if we know the sstable don't overlap (see
     * LeveledCompactionStrategy for instance).
     */
    default ScannerList getScanners(Collection<SSTableReader> sstables)
    {
        return getScanners(sstables, (Collection<Range<Token>>) null);
    }

    /**
     * Returns a list of KeyScanners given sstables and a range on which to scan.
     * The default implementation simply grab one SSTableScanner per-sstable, but overriding this method
     * allow for a more memory efficient solution if we know the sstable don't overlap (see
     * LeveledCompactionStrategy for instance).
     */
    default ScannerList getScanners(Collection<SSTableReader> sstables, Range<Token> range)
    {
        return getScanners(sstables, range != null ? ImmutableList.of(range) : null);
    }

    ScannerFactory DEFAULT = new ScannerFactory()
    {
        public ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
        {
            ArrayList<ISSTableScanner> scanners = new ArrayList<>();
            try
            {
                for (SSTableReader sstable : sstables)
                    scanners.add(sstable.getScanner(ranges));
            }
            catch (Throwable t)
            {
                ISSTableScanner.closeAllAndPropagate(scanners, t);
            }
            return new ScannerList(scanners);
        }
    };
}
