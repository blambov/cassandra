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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

import org.apache.cassandra.utils.SortingIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

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
        Iterator<Integer> iterator = SortingIterator.create(comparator, integers);
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
