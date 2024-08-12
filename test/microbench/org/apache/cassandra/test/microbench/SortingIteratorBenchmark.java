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

package org.apache.cassandra.test.microbench;

import org.apache.cassandra.index.sai.utils.PriorityQueueIterator;
import org.apache.cassandra.utils.LucenePriorityQueue;
import org.apache.cassandra.utils.SortingIterator;
import org.apache.cassandra.utils.SortingIteratorNulls;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

@Fork(1)
@Warmup(iterations = 5, time = 3)
@Measurement(iterations = 10, time = 3)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@State(Scope.Thread)
public class SortingIteratorBenchmark {

    @Param({"100", "1000", "10000"})
    public int size;

    @Param({"0.1"})
    double nullChance = 0.1;

    @Param({"0", "0.1", "1"})
    double consumeRatio = 0.1;

    @Param({"0", "100"})
    int comparatorSlowDown = 0;

    public List<Integer> data;

    @Setup(Level.Trial)
    public void setUp() {
        Random random = new Random();
        data = new ArrayList<>(size * 100);
        for (int i = 0; i < size * 100; i++) {
            data.add(random.nextDouble() < nullChance ? null : random.nextInt());
        }
        comparator = comparatorSlowDown <= 0 ? Comparator.naturalOrder()
                                             : (x, y) ->
                                               {
                                                   Blackhole.consumeCPU(comparatorSlowDown);
                                                   return Integer.compare(x, y);
                                               };
    }

    Comparator<Integer> comparator;

    @Benchmark
    public void testSortingIterator(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        Iterator<Integer> iterator = new SortingIterator<>(Integer.class, comparator, integers, x -> x);
        int i = (int) Math.ceil(consumeRatio * size + 0.01);
        while (iterator.hasNext() && i-- > 0) {
            bh.consume(iterator.next());
        }
    }


    @Benchmark
    public void testSortingIteratorNulls(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        Iterator<Integer> iterator = new SortingIteratorNulls<>(Integer.class, comparator, integers, x -> x);
        int i = (int) Math.ceil(consumeRatio * size + 0.01);
        while (iterator.hasNext() && i-- > 0) {
            bh.consume(iterator.next());
        }
    }


    @Benchmark
    public void testLuceneIterator(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        Iterator<Integer> iterator = new LucenePriorityQueue.SortingIterator<>(comparator, integers);
        int i = (int) Math.ceil(consumeRatio * size + 0.01);
        while (iterator.hasNext() && i-- > 0) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testPriorityQueueIterator(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        var pq = new PriorityQueue<Integer>(data.size(), comparator);
        for (Integer item : integers)
            if (item != null)
                pq.add(item);
        var iterator = new PriorityQueueIterator<>(pq);
        int i = (int) Math.ceil(consumeRatio * size + 0.01);
        while (iterator.hasNext() && i-- > 0) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testArrayListSortIterator(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        var al = new ArrayList<Integer>(data.size());
        for (Integer item : integers)
            if (item != null)
                al.add(item);
        al.sort(comparator);
        var iterator = al.iterator();
        int i = (int) Math.ceil(consumeRatio * size + 0.01);
        while (iterator.hasNext() && i-- > 0) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testArraySortIterator(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        Integer[] al = new Integer[data.size()];
        int sz = 0;
        for (Integer item : integers)
            if (item != null)
                al[sz++] = item;
        Arrays.sort(al, 0, sz, comparator);
        var iterator = Iterators.forArray(al);

        int i = (int) Math.ceil(consumeRatio * size + 0.01);
        while (iterator.hasNext() && i-- > 0) {
            bh.consume(iterator.next());
        }
    }

    @Benchmark
    public void testStreamIterator(Blackhole bh) {
        int startIndex = ThreadLocalRandom.current().nextInt(data.size() - size);
        List<Integer> integers = data.subList(startIndex, startIndex + size);
        var iterator = integers.stream().filter(Predicates.notNull()).sorted(comparator).iterator();
        int i = (int) Math.ceil(consumeRatio * size + 0.01);
        while (iterator.hasNext() && i-- > 0) {
            bh.consume(iterator.next());
        }
    }
}
