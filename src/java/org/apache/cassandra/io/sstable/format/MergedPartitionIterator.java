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

package org.apache.cassandra.io.sstable.format;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.trieindex.TrieIndexFormat;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Reducer;

public class MergedPartitionIterator
{
    public static Iterator<KeyAndSSTables> getFromDescriptors(Collection<Descriptor> live, Collection<Descriptor> obsolete, TableMetadata metadata)
    {
        List<IndexIterator> iterators = new ArrayList<>();
        SSTableReader.Factory factory = TrieIndexFormat.instance.getReaderFactory();

        for (Descriptor oneLive : live)
            iterators.add(new IndexIterator(factory.indexIterator(oneLive, metadata), oneLive, true, metadata));

        for (Descriptor oneObsolete : obsolete)
            iterators.add(new IndexIterator(factory.indexIterator(oneObsolete, metadata), oneObsolete, false, metadata));

        Iterator<KeyAndSSTables> mergeIterator =
            MergeIterator.getCloseable(iterators,
                                       Comparator.comparing(k -> k.key),
                                       new KeyAndSSTablesReducer());

        return mergeIterator;
    }

    public static Iterator<KeyAndSSTables> getFromReaders(Collection<SSTableReader> live, Collection<SSTableReader> obsolete, TableMetadata metadata)
    {
        List<UIndexIterator> iterators = new ArrayList<>();

        for (var oneLive : live)
            iterators.add(new UIndexIterator(oneLive.getScanner(), oneLive.descriptor, true));

        for (var oneObsolete : obsolete)
            iterators.add(new UIndexIterator(oneObsolete.getScanner(), oneObsolete.descriptor, false));

        Iterator<KeyAndSSTables> mergeIterator =
            MergeIterator.getCloseable(iterators,
                                       Comparator.comparing(k -> k.key),
                                       new KeyAndSSTablesReducer());

        return mergeIterator;
    }

    static class KeyAndSSTablesReducer extends Reducer<KeyAndSSTables, KeyAndSSTables>
    {
        List<KeyAndSSTables> sources = new ArrayList<>();

        @Override
        public void onKeyChange()
        {
            sources.clear();
        }

        @Override
        public void reduce(int idx, KeyAndSSTables current)
        {
            sources.add(current);
        }

        @Override
        public KeyAndSSTables getReduced()
        {
            if (sources.size() == 1)
                return sources.get(0);

            boolean hasLive = false;
            Set<Descriptor> ss = new HashSet<>();
            for (KeyAndSSTables ks : sources)
            {
                ss.addAll(ks.sstables);
                hasLive |= ks.hasLive;
            }
            return new KeyAndSSTables(sources.get(0).key, ss, hasLive);
        }
    }

    static class KeyAndSSTables
    {
        final DecoratedKey key;
        final Set<Descriptor> sstables;
        final boolean hasLive;

        KeyAndSSTables(DecoratedKey key, Set<Descriptor> sstables, boolean hasLive)
        {
            this.key = key;
            this.sstables = sstables;
            this.hasLive = hasLive;
        }
    }

    static class IndexIterator implements CloseableIterator<KeyAndSSTables>
    {
        final PartitionIndexIterator source;
        final Set<Descriptor> sourceSet;
        final boolean isLive;
        final TableMetadata metadata;
        KeyAndSSTables next;

        IndexIterator(PartitionIndexIterator source, Descriptor sourceDesc, boolean isLive, TableMetadata metadata)
        {
            this.source = source;
            this.sourceSet = Set.of(sourceDesc);
            this.isLive = isLive;
            this.metadata = metadata;
            next = new KeyAndSSTables(metadata.partitioner.decorateKey(source.key()), sourceSet, isLive);
        }

        public boolean hasNext()
        {
            return next != null;
        }

        public KeyAndSSTables next()
        {
            KeyAndSSTables toReturn = next;
            try
            {
                if (source.advance())
                    next = new KeyAndSSTables(metadata.partitioner.decorateKey(source.key()), sourceSet, isLive);
                else
                    next = null;
            }
            catch (IOException e)
            {
                throw new CorruptSSTableException(e, sourceSet.iterator().next().pathFor(Component.PARTITION_INDEX));
            }
            return toReturn;
        }

        @Override
        public void close()
        {
            source.close();
        }
    }


    static class UIndexIterator implements CloseableIterator<KeyAndSSTables>
    {
        final ISSTableScanner source;
        final Set<Descriptor> sourceSet;
        final boolean isLive;

        UIndexIterator(ISSTableScanner source, Descriptor sourceDesc, boolean isLive)
        {
            this.source = source;
            this.sourceSet = Set.of(sourceDesc);
            this.isLive = isLive;
        }

        public boolean hasNext()
        {
            return source.hasNext();
        }

        public KeyAndSSTables next()
        {
            return new KeyAndSSTables(source.next().partitionKey(), sourceSet, isLive);
        }

        @Override
        public void close()
        {
            source.close();
        }
    }

    public void test(Iterator<KeyAndSSTables> iter)
    {
        while (iter.hasNext())
        {
            var next = iter.next();
            if (!next.hasLive)
                System.out.format("Key %s not in live. Found in %s.", next.key, next.sstables);
        }
    }
}
