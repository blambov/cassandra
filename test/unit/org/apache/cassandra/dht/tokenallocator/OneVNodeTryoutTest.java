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

package org.apache.cassandra.dht.tokenallocator;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.junit.Test;

public class OneVNodeTryoutTest
{
    private double target(int n)
    {
        double sum = 0;
        for (int i = n; i < 2*n; ++i)
            sum += 1.0 / i;
        return 1.0/sum;
    }


    @Test
    public void oneVnodeTryout()
    {
        int targetSize = 100;
        int targetPoint = 1;
        double[] tokenSizes = new double[targetSize];
        tokenSizes[0] = 1.0;
        double maxMax = 0;
        int maxIndex = 0;
        double minMin = Double.POSITIVE_INFINITY;
        int minIndex = 0;
        for (int i = 1; i < targetSize; i++)
        {
            double max = tokenSizes[i - 1];
            double min = tokenSizes[0];
            System.out.println(String.format("\ni %d max %.3f target(i) %.3f min %.3f", i, max * i, target(i), min * i));
            if (max * i > maxMax)
            {
                maxMax = max * i;
                maxIndex = i;
            }
            if (min * i < minMin)
            {
                minMin = min * i;
                minIndex = i;
            }
            final double buffer = 0.000001;

            for (int j = 2*i - 1 - targetPoint; j >= 0; j--)
            {
                // if next is above the limit for the step after next, we have to target a further point
                final int point = targetPoint + 1;
                if (tokenSizes[j] * point > target(point) + buffer)
                    ++targetPoint;
                else
                    break;
            }
            double target = target(targetPoint);
            System.out.println(String.format("\nTarget at %d : %.4f ", targetPoint, target));
            assert targetPoint >= i;

            double split = (target / targetPoint) / max;
            System.out.println("Splitting by " + split);
            assert split > 0 && split < 1;

            // TODO: decide on the split
            tokenSizes[i - 1] = max * split;
            tokenSizes[i] = max * (1.0 - split);//1.0 - DoubleStream.of(tokenSizes).limit(i).sum();
            int size = i + 1;
            Arrays.sort(tokenSizes, 0, size);
//                System.out.println(DoubleStream.of(tokenSizes).limit(size).summaryStatistics());
            print("sizes", DoubleStream.of(tokenSizes).limit(size));
//                print("1/szs", DoubleStream.of(tokenSizes).limit(size).map(x -> 1 / x));
//                print("t/szs", DoubleStream.of(tokenSizes).limit(size).map(x -> target / x));
            print("tn/sz", IntStream.range(0, size).mapToDouble(x -> target(2*size-1 - x) / tokenSizes[x]));
//            print("tdiff", IntStream.range(0, size).mapToDouble(x -> target/(2*size-1 - x)+buffer - tokenSizes[x]));
        }
        System.out.println("maxMax: " + maxMax + " at " + maxIndex);
        System.out.println("minMin: " + minMin + " at " + minIndex);
//            System.out.println("Succeeded for target " + target);
    }

    private void print(String head, DoubleStream s)
    {
        System.out.println(head + ": " + s.mapToObj(x -> String.format("%.4f", x)).collect(Collectors.joining(", ")));
    }
}
