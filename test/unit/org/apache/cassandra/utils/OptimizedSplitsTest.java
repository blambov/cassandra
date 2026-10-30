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

package org.apache.cassandra.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OptimizedSplitsTest
{
    static final double EPSILON = 1e-6;

    @Test
    public void testOptimizedSplitsSingleSet()
    {
        SplitProblem problem = singleSetProblem();
        assertEquals(1, problem.sets.size());
        assertEquals(List.of(), verifySplits(problem, 100, 0));
        assertEquals(List.of(), verifySplits(problem, 6, 0));
        assertEquals(List.of(), verifySplits(problem, 1, 0));
    }

    private static SplitProblem singleSetProblem()
    {
        // A single set [0, 4) with weight 6.
        return new SplitProblem(List.of(new SizedItem("A", 0, 4, 1, 1),
                                        new SizedItem("B", 1, 3, 1, 1)));
    }

    @Test
    public void testOptimizedSplitsPrefersCheapSplitPoints()
    {
        SplitProblem problem = cheapSplitPointsProblem();
        assertEquals(List.of(4, 6, 8, 10, 12), problem.boundaries);

        // Everything fits in one group.
        assertEquals(List.of(), verifySplits(problem, 28, 0));
        // A single split at 8 would do, but splitting the cheap B and C is better even if it makes more groups.
        assertEquals(List.of(6, 10), verifySplits(problem, 14, 2));
        assertEquals(List.of(6, 10), verifySplits(problem, 10, 2));
        // AX and ZD can't be grouped with anything, the rest should still be split at the cheap points.
        assertEquals(List.of(4, 6, 10, 12), verifySplits(problem, 8, 22));
    }

    private static SplitProblem cheapSplitPointsProblem()
    {
        // Sets AX [0, 4), XB [4, 6), BY [6, 8), YC [8, 10), CZ [10, 12), ZD [12, 16) with weights 6, 4, 4, 4, 4, 6.
        // Splitting at 4, 6, 8, 10 or 12 costs 10, 1, 5, 1 and 10 respectively.
        return new SplitProblem(List.of(new SizedItem("A", 0, 4, 1, 100),
                                        new SizedItem("B", 4, 8, 1, 1),
                                        new SizedItem("C", 8, 12, 1, 1),
                                        new SizedItem("D", 12, 16, 1, 100),
                                        new SizedItem("X", 2, 6, 1, 10),
                                        new SizedItem("Y", 6, 10, 1, 5),
                                        new SizedItem("Z", 10, 14, 1, 10)));
    }

    @Test
    public void testOptimizedSplitsChargesItemOnce()
    {
        // Sets AL [0, 4), LBY [4, 8), LYC [8, 12), LD [12, 16) with weights 8, 10, 10, 8.
        // L crosses all boundaries, Y only the one at 8.
        SplitProblem problem = new SplitProblem(List.of(new SizedItem("A", 0, 4, 1, 100),
                                                        new SizedItem("B", 4, 8, 1, 100),
                                                        new SizedItem("C", 8, 12, 1, 100),
                                                        new SizedItem("D", 12, 16, 1, 100),
                                                        new SizedItem("Y", 6, 10, 1, 4),
                                                        new SizedItem("L", 0, 16, 1, 5)));
        assertEquals(List.of(4, 8, 12), problem.boundaries);

        // Splitting L twice costs 5, which is less than splitting both L and Y once at 8.
        assertEquals(List.of(4, 12), verifySplits(problem, 20, 5));
        // The middle can't be grouped, so we have to split Y. Any further splits of L are free (multiple optima).
        verifySplits(problem, 18, 9);
        assertEquals(List.of(4, 8, 12), verifySplits(problem, 10, 9));
    }

    @Test
    public void testOptimizedSplitsOversizedSet()
    {
        // Sets AL [0, 4), LH [4, 8), LC [8, 12) with weights 6, 44, 6. LH is over the limit on its own, so it must be
        // in a group by itself.
        SplitProblem problem = new SplitProblem(List.of(new SizedItem("A", 0, 4, 1, 100),
                                                        new SizedItem("H", 4, 8, 10, 100),
                                                        new SizedItem("C", 8, 12, 1, 100),
                                                        new SizedItem("L", 2, 10, 1, 3)));
        assertEquals(List.of(4, 8), problem.boundaries);

        assertEquals(List.of(4, 8), verifySplits(problem, 20, 3));
        assertEquals(List.of(4, 8), verifySplits(problem, 44, 3));
        assertEquals(List.of(), verifySplits(problem, 56, 0));
    }

    @Test
    public void testOptimizedSplitsRandom()
    {
        long seed = System.nanoTime();
        Random rand = new Random(seed);
        try
        {
            for (int i = 0; i < 10000; ++i)
            {
                // Limit the number of items so that the number of sets is small enough to brute-force.
                int count = 1 + rand.nextInt(13);
                List<SizedItem> items = new ArrayList<>();
                for (int j = 0; j < count; ++j)
                {
                    int start = rand.nextInt(50);
                    int length = 1 + rand.nextInt(rand.nextBoolean() ? 5 : 30);
                    items.add(new SizedItem(Character.toString('A' + j),
                                            start,
                                            start + length,
                                            1 + rand.nextInt(5),
                                            rand.nextInt(4) == 0 ? 0 : 1 + rand.nextInt(20)));
                }
                SplitProblem problem = new SplitProblem(items);
                double totalWeight = problem.totalWeight();
                for (int k = 0; k < 5; ++k)
                {
                    double limit = rand.nextInt((int) totalWeight + 2);
                    verifySplits(problem, limit, problem.bruteForceCost(limit));
                }
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError("Failed with seed " + seed, t);
        }
    }

    @Test(timeout = 10000)
    public void testOptimizedSplitsManySets()
    {
        long seed = System.nanoTime();
        SplitProblem problem = new SplitProblem(manyItems(new Random(seed)));
        assertTrue(problem.sets.size() > 500);
        double totalWeight = problem.totalWeight();
        // We can't verify optimality here, only that the result is valid and that it is computed in reasonable time
        // when groups span many sets.
        try
        {
            solveAndVerify(problem, totalWeight / 20, false);
        }
        catch (Throwable t)
        {
            throw new AssertionError("Failed with seed " + seed, t);
        }
    }

    private static List<SizedItem> manyItems(Random rand)
    {
        List<SizedItem> items = new ArrayList<>();
        for (int j = 0; j < 2000; ++j)
        {
            int start = rand.nextInt(100000);
            int length = 1 + rand.nextInt(rand.nextInt(20) == 0 ? 5000 : 200);
            items.add(new SizedItem("I" + j, start, start + length, 1 + rand.nextInt(5), 1 + rand.nextInt(20)));
        }
        return items;
    }


    @Test//(timeout = 10000)
    public void testOptimizedSplitsSplitterSingleSet()
    {
        SplitProblem problem = singleSetProblem();
        assertEquals(List.of(), verifySplitsWithSplitter(problem, 100, 0));
        assertEquals(List.of(), verifySplitsWithSplitter(problem, 6, 0));
        // The set is split where the first group reaches the limit, and the rest makes the next group.
        assertEquals(List.of(2), verifySplitsWithSplitter(problem, 3, 2));
        // The rest is still over the limit and must be split further. Splitting A again costs nothing extra.
        assertEquals(List.of(1, 2, 3), verifySplitsWithSplitter(problem, 2, 2));
    }

    @Test(timeout = 10000)
    public void testOptimizedSplitsSplitterOversizedSet()
    {
        // A single set [0, 12) with weight 48, made of two items that span all of it.
        SplitProblem problem = new SplitProblem(List.of(new SizedItem("F", 0, 12, 2, 1),
                                                        new SizedItem("G", 0, 12, 2, 1)));
        assertEquals(List.of(), verifySplitsWithSplitter(problem, 48, 0));
        assertEquals(List.of(6), verifySplitsWithSplitter(problem, 24, 2));
        // The items are only charged once, however many pieces they are split into.
        assertEquals(List.of(5, 10), verifySplitsWithSplitter(problem, 20, 2));
        assertEquals(IntStream.range(1, 12).boxed().collect(Collectors.toList()), verifySplitsWithSplitter(problem, 7, 2));
    }

    @Test(timeout = 10000)
    public void testOptimizedSplitsSplitterPrefersCheapBoundaries()
    {
        // Filling groups to the limit by splitting inside sets would split more expensive items than splitting at the
        // cheap boundaries 6 and 10.
        SplitProblem problem = cheapSplitPointsProblem();
        assertEquals(List.of(), verifySplitsWithSplitter(problem, 28, 0));
        assertEquals(List.of(6, 10), verifySplitsWithSplitter(problem, 14, 2));
        assertEquals(List.of(6, 10), verifySplitsWithSplitter(problem, 10, 2));
    }

    @Test(timeout = 10000)
    public void testOptimizedSplitsSplitterMergesWithNeighbours()
    {
        // Sets AX [0, 4), XH [4, 10), HZ [10, 12), ZC [12, 16) with weights 6, 26, 10, 6. The boundaries at 4 and 12
        // split the expensive X and Z, the one at 10 the cheap H.
        SplitProblem problem = new SplitProblem(List.of(new SizedItem("A", 0, 4, 1, 100),
                                                        new SizedItem("X", 2, 6, 1, 50),
                                                        new SizedItem("H", 4, 12, 4, 1),
                                                        new SizedItem("Z", 10, 14, 1, 50),
                                                        new SizedItem("C", 12, 16, 1, 100)));
        assertEquals(List.of(4, 10, 12), problem.boundaries);

        // AX can take the part of XH up to 7, which only splits H, after which splitting at 10 is free.
        assertEquals(List.of(7, 10), verifySplitsWithSplitter(problem, 20, 1));
        // Without splitting inside sets, XH must be in a group of its own, splitting X and H.
        assertEquals(List.of(4, 10), verifySplits(problem, 20, 51));
    }

    @Test(timeout = 10000)
    public void testOptimizedSplitsSplitterFullerGroupIsNotAlwaysBetter()
    {
        // Sets P [3, 9), AFD [9, 16), ADG [16, 18), GC [18, 21) with weights 30, 39, 17, 16. Only D, G and C are
        // expensive to split. The boundary at 16 splits D, the one at 18 G.
        SplitProblem problem = new SplitProblem(List.of(new SizedItem("P", 3, 9, 5, 0),
                                                        new SizedItem("A", 9, 17, 3, 0),
                                                        new SizedItem("F", 13, 16, 5, 0),
                                                        new SizedItem("D", 15, 18, 3, 11),
                                                        new SizedItem("G", 16, 19, 4, 6),
                                                        new SizedItem("C", 18, 21, 4, 9)));
        assertEquals(List.of(9, 16, 18), problem.boundaries);

        // Filling the first group by splitting AFD at 11 costs no more than splitting at 9, but the rest of AFD then
        // has to be split at 16. Splitting at 9 lets AFD be split at 15, after which splitting at 18 is enough.
        assertEquals(List.of(9, 15, 18), verifySplitsWithSplitter(problem, 37, 6));
    }

    @Test(timeout = 10000)
    public void testOptimizedSplitsSplitterEarlierBoundaryCanBeBetter()
    {
        // Sets BD [0, 8), A [8, 11), EC [11, 18), EF [18, 23) with weights 10, 5, 32, 32. Only C is expensive to split.
        SplitProblem problem = new SplitProblem(List.of(new SizedItem("B", 0, 4, 1, 7),
                                                        new SizedItem("D", 2, 4, 3, 3),
                                                        new SizedItem("A", 8, 9, 5, 3),
                                                        new SizedItem("E", 11, 21, 4, 2),
                                                        new SizedItem("C", 13, 15, 2, 15),
                                                        new SizedItem("F", 18, 23, 4, 0)));
        assertEquals(List.of(8, 11, 18), problem.boundaries);

        // Splitting at 8 and at 11 are both free, but only a group that starts at 8 can end inside EC before C starts,
        // at 13. Starting at 11, EC would be split at 14, splitting C. The rest only splits E and F (multiple optima).
        verifySplitsWithSplitter(problem, 17, 2);
    }

    @Test(timeout = 60000)
    public void testOptimizedSplitsSplitterRandom()
    {
        long seed = System.nanoTime();
        Random rand = new Random(seed);
        try
        {
            for (int i = 0; i < 500; ++i)
            {
                int count = 1 + rand.nextInt(10);
                List<SizedItem> items = new ArrayList<>();
                for (int j = 0; j < count; ++j)
                {
                    int start = rand.nextInt(20);
                    int length = 1 + rand.nextInt(rand.nextBoolean() ? 5 : 15);
                    items.add(new SizedItem(Character.toString('A' + j),
                                            start,
                                            start + length,
                                            1 + rand.nextInt(5),
                                            rand.nextInt(4) == 0 ? 0 : 1 + rand.nextInt(20)));
                }
                SplitProblem problem = new SplitProblem(items);
                double totalWeight = problem.totalWeight();
                // The space can't be split finer than unit length, so sets can't be split to fit a limit below the
                // weight of a unit-length piece. Also keep the number of groups small enough for the brute force.
                int minLimit = (int) Math.ceil(Math.max(problem.maxUnitWeight(), totalWeight / 8));
                for (int k = 0; k < 3; ++k)
                {
                    double limit = minLimit + rand.nextInt((int) totalWeight - minLimit + 2);
                    verifySplitsWithSplitter(problem, limit, problem.bruteForceCostWithSplitter(limit));
                }
            }
        }
        catch (Throwable t)
        {
            throw new AssertionError("Failed with seed " + seed, t);
        }
    }

    @Test(timeout = 10000)
    public void testOptimizedSplitsSplitterManySets()
    {
        long seed = System.nanoTime();
        SplitProblem problem = new SplitProblem(manyItems(new Random(seed)));
        assertTrue(problem.sets.size() > 500);
        // A limit that many sets are over on their own. A whole number keeps all weights in the solution whole, so
        // that rounding split points down never takes a group over the limit.
        double limit = Math.floor(problem.totalWeight() / 1000);
        assertTrue(limit >= problem.maxUnitWeight());
        try
        {
            solveAndVerify(problem, limit, true);
        }
        catch (Throwable t)
        {
            throw new AssertionError("Failed with seed " + seed, t);
        }
    }

    /// Runs `optimizedSplits` with the given limit in a space that can't be split, so that groups can only end at set
    /// boundaries, validates the result, checks that its cost is as expected, and returns the points where it splits.
    private static List<Integer> verifySplits(SplitProblem problem, double limit, double expectedCost)
    {
        return verifySplits(problem, limit, false, expectedCost);
    }

    /// As above, but in a space that can be split at any integer point, so that groups can end inside sets.
    private static List<Integer> verifySplitsWithSplitter(SplitProblem problem, double limit, double expectedCost)
    {
        return verifySplits(problem, limit, true, expectedCost);
    }

    private static List<Integer> verifySplits(SplitProblem problem, double limit, boolean splittable, double expectedCost)
    {
        OptimizedSplits.Split<SizedItem, Integer> solution = solveAndVerify(problem, limit, splittable);
        List<Integer> splitPoints = new ArrayList<>();
        for (OptimizedSplits.Split<SizedItem, Integer> group = solution; group != null; group = group.previous)
            if (group.pointAfter != null)
                splitPoints.add(group.pointAfter);
        Collections.reverse(splitPoints);
        assertEquals("Split cost with limit " + limit + (splittable ? " splitting inside sets" : "") +
                     " for " + problem + " splitting at " + splitPoints,
                     expectedCost, solution.accumulatedSplitCost, EPSILON);
        return splitPoints;
    }

    private static OptimizedSplits.Split<SizedItem, Integer> solveAndVerify(SplitProblem problem, double limit, boolean splittable)
    {
        try
        {
            OptimizedSplits.Split<SizedItem, Integer> solution = problem.solve(limit, splittable);
            problem.verify(solution, limit, splittable);
            return solution;
        }
        catch (Throwable t)
        {
            throw new AssertionError("Failed with limit " + limit + (splittable ? " splitting inside sets" : "") +
                                     " for " + problem, t);
        }
    }

    /// An interval with a uniformly distributed size and a cost of splitting it.
    private static class SizedItem
    {
        final String name;
        final int start;
        final int end;
        final int density;
        final double splitCost;

        SizedItem(String name, int start, int end, int density, double splitCost)
        {
            assert start < end;
            this.name = name;
            this.start = start;
            this.end = end;
            this.density = density;
            this.splitCost = splitCost;
        }

        /// The size of the part of the item that lies between the given points, where null means unbounded.
        double weight(Integer pointBefore, Integer pointAfter)
        {
            int s = pointBefore != null ? Math.max(start, pointBefore) : start;
            int e = pointAfter != null ? Math.min(end, pointAfter) : end;
            return density * Math.max(0, e - s);
        }

        boolean isSplitBy(int point)
        {
            return start < point && point < end;
        }

        @Override
        public String toString()
        {
            return String.format("%s[%d,%d)x%d$%.0f", name, start, end, density, splitCost);
        }
    }

    /// A space of integer points, over which the weight of each item is spread uniformly. If it isn't splittable, its
    /// spans can't be split, so groups can only end at the boundaries between overlap sets.
    private static class SizedItemSpace implements OptimizedSplits.ItemSpace<SizedItem, Integer>
    {
        static final SizedItemSpace SPLITTABLE = new SizedItemSpace(true);
        static final SizedItemSpace UNSPLITTABLE = new SizedItemSpace(false);

        final boolean splittable;

        SizedItemSpace(boolean splittable)
        {
            this.splittable = splittable;
        }

        @Override
        public Integer startOf(SizedItem item)
        {
            return item.start;
        }

        @Override
        public Integer endOf(SizedItem item)
        {
            return item.end;
        }

        @Override
        public double weight(SizedItem item, Integer sliceStart, Integer sliceEnd)
        {
            if (sliceStart < item.start || sliceEnd > item.end || sliceStart >= sliceEnd)
                throw new AssertionError("Slice [" + sliceStart + ", " + sliceEnd + ") is not inside " + item);
            return item.weight(sliceStart, sliceEnd);
        }

        @Override
        public Integer split(Integer start, Integer end, double ratio)
        {
            if (!splittable)
                return null;
            // Round down, allowing for floating-point error in the ratio so that whole points are not missed.
            return (int) Math.floor(start + (end - start) * ratio + 1e-9);
        }
    }

    /// The overlap sets of a list of items and the boundaries between them, used to validate the solutions of
    /// [OptimizedSplits#optimizedSplits] and to find the optimal cost by brute force.
    ///
    /// Splitting at a point splits the items that are present on both sides of it, and the cost of splitting an item is
    /// charged once, however many times it is split. If the space isn't splittable, groups can only end at set
    /// boundaries, and a group may only be over the limit if it is a single set. Otherwise, a group that would go over
    /// the limit may instead end inside the set that takes it over, at the furthest point where it is still within the
    /// limit, and no group may be over the limit.
    private static class SplitProblem
    {
        final List<SizedItem> items;
        final List<Set<SizedItem>> sets;
        /// The point between sets i and i + 1.
        final List<Integer> boundaries = new ArrayList<>();

        SplitProblem(List<SizedItem> items)
        {
            this.items = items;
            this.sets = Overlaps.constructOverlapSets(new ArrayList<>(items),
                                                      (x, y) -> x.start >= y.end,
                                                      Comparator.comparingInt(x -> x.start),
                                                      Comparator.comparingInt(x -> x.end));
            for (int i = 0; i + 1 < sets.size(); ++i)
                boundaries.add(pointBetween(sets.get(i), sets.get(i + 1)));
        }

        static Integer pointBetween(Set<SizedItem> set, Set<SizedItem> next)
        {
            // All items that are in set but not in next end before any item of next that is not in set starts.
            return next.stream().filter(x -> !set.contains(x)).mapToInt(x -> x.start).min().getAsInt();
        }

        /// The furthest point after `pointBefore` (null meaning the start of the set) up to which the items of the set
        /// weigh at most `weight`.
        static Integer splitPoint(Set<SizedItem> set, Integer pointBefore, double weight)
        {
            int start = pointBefore != null ? pointBefore : set.stream().mapToInt(x -> x.start).min().getAsInt();
            int low = start;
            int high = set.stream().mapToInt(x -> x.end).max().getAsInt();
            while (low < high)
            {
                int mid = (low + high + 1) >>> 1;
                if (weightBetween(set, start, mid) <= weight)
                    low = mid;
                else
                    high = mid - 1;
            }
            return low;
        }

        static double weightBetween(Collection<SizedItem> items, Integer pointBefore, Integer pointAfter)
        {
            double sum = 0;
            for (SizedItem item : items)
                sum += item.weight(pointBefore, pointAfter);
            return sum;
        }

        double totalWeight()
        {
            return weightBetween(items, null, null);
        }

        /// The biggest weight of a piece of unit length. Sets can't be split to fit a limit below this.
        double maxUnitWeight()
        {
            NavigableMap<Integer, Integer> densityChanges = new TreeMap<>();
            for (SizedItem item : items)
            {
                densityChanges.merge(item.start, item.density, Integer::sum);
                densityChanges.merge(item.end, -item.density, Integer::sum);
            }
            int density = 0;
            int maxDensity = 0;
            for (int change : densityChanges.values())
            {
                density += change;
                maxDensity = Math.max(maxDensity, density);
            }
            return maxDensity;
        }

        Set<SizedItem> splitBy(int point)
        {
            return items.stream().filter(x -> x.isSplitBy(point)).collect(Collectors.toSet());
        }

        static double cost(Collection<SizedItem> splitItems)
        {
            return splitItems.stream().mapToDouble(x -> x.splitCost).sum();
        }

        boolean hasBoundaryBetween(Integer start, Integer end)
        {
            for (Integer boundary : boundaries)
                if ((start == null || boundary > start) && (end == null || boundary < end))
                    return true;
            return false;
        }

        OptimizedSplits.Split<SizedItem, Integer> solve(double limit, boolean splittable)
        {
            return OptimizedSplits.optimizedSplits(items,
                                                   splittable ? SizedItemSpace.SPLITTABLE : SizedItemSpace.UNSPLITTABLE,
                                                   x -> x.splitCost,
                                                   limit);
        }

        /// Checks that the solution is a sequence of groups split at increasing points, which must be set boundaries if
        /// the space isn't splittable, where each group is within the limit (or, if the space isn't splittable, a
        /// single set), and where the reported weights and accumulated costs match the items in each group and the
        /// items split so far.
        void verify(OptimizedSplits.Split<SizedItem, Integer> solution, double limit, boolean splittable)
        {
            List<OptimizedSplits.Split<SizedItem, Integer>> groups = new ArrayList<>();
            for (OptimizedSplits.Split<SizedItem, Integer> group = solution; group != null; group = group.previous)
                groups.add(group);
            Collections.reverse(groups);
            assertTrue("No groups returned", !groups.isEmpty());

            Set<SizedItem> splitItems = new HashSet<>();
            Integer start = null;
            for (OptimizedSplits.Split<SizedItem, Integer> group : groups)
            {
                Integer end = group.pointAfter;
                String name = "group [" + start + ", " + end + ")";
                if (group == solution)
                {
                    assertNull("Last group must not have a split point", end);
                }
                else
                {
                    assertTrue(name + " must have a split point", end != null);
                    assertTrue(name + " is empty", start == null || end > start);
                    assertTrue(name + " must end at a set boundary", splittable || boundaries.contains(end));
                    splitItems.addAll(splitBy(end));
                }

                double groupWeight = weightBetween(items, start, end);
                assertEquals("Weight of " + name, groupWeight, group.weight, EPSILON);
                assertTrue(name + " with weight " + groupWeight + " exceeds limit",
                           groupWeight <= limit || !splittable && !hasBoundaryBetween(start, end));
                assertEquals("Accumulated cost of " + name, cost(splitItems), group.accumulatedSplitCost, EPSILON);
                start = end;
            }
        }

        /// The minimum split cost if the space isn't splittable, found by trying all combinations of set boundaries.
        double bruteForceCost(double limit)
        {
            int m = boundaries.size();
            assert m <= 20 && items.size() <= 64;
            // The weights of the sets, and the items split at each boundary.
            double[] setWeights = new double[m + 1];
            long[] splitMasks = new long[m];
            for (int k = 0; k <= m; ++k)
            {
                Integer pointBefore = k > 0 ? boundaries.get(k - 1) : null;
                Integer pointAfter = k < m ? boundaries.get(k) : null;
                for (int j = 0; j < items.size(); ++j)
                {
                    setWeights[k] += items.get(j).weight(pointBefore, pointAfter);
                    if (k < m && items.get(j).isSplitBy(pointAfter))
                        splitMasks[k] |= 1L << j;
                }
            }

            double best = Double.POSITIVE_INFINITY;
            for (int splitSelection = 0; splitSelection < 1 << m; ++splitSelection)
            {
                long splitItems = 0;
                double groupWeight = 0;
                int groupStart = 0;
                boolean valid = true;
                for (int k = 0; k <= m && valid; ++k)
                {
                    groupWeight += setWeights[k];
                    valid = k == groupStart || groupWeight <= limit;
                    if (k < m && (splitSelection & (1 << k)) != 0)    // split at boundary k, i.e. after set k
                    {
                        splitItems |= splitMasks[k];
                        groupWeight = 0;
                        groupStart = k + 1;
                    }
                }
                if (!valid)
                    continue;

                double cost = 0;
                for (int j = 0; j < items.size(); ++j)
                    if ((splitItems & (1L << j)) != 0)
                        cost += items.get(j).splitCost;
                best = Math.min(best, cost);
            }
            return best;
        }

        /// The minimum split cost if the space is splittable, found by trying all ways to end each group: at any set
        /// boundary that keeps it within the limit, or inside the set that takes it over the limit, at the furthest
        /// point where it is still within the limit.
        double bruteForceCostWithSplitter(double limit)
        {
            return bruteForceCostWithSplitter(limit, null, 0, Set.of());
        }

        private double bruteForceCostWithSplitter(double limit, Integer start, int startSet, Set<SizedItem> splitItems)
        {
            double best = Double.POSITIVE_INFINITY;
            double weight = 0;
            for (int i = startSet; i < sets.size() && weight < limit; ++i)
            {
                Integer setStart = i == startSet ? start : boundaries.get(i - 1);
                Integer setEnd = i < boundaries.size() ? boundaries.get(i) : null;
                double setWeight = weightBetween(sets.get(i), setStart, setEnd);
                if (weight + setWeight > limit)
                {
                    Integer point = splitPoint(sets.get(i), setStart, limit - weight);
                    assertTrue("Splitting can't make progress at " + point, i > startSet || !point.equals(setStart));
                    best = Math.min(best, bruteForceCostWithSplitter(limit, point, i, union(splitItems, splitBy(point))));
                    break;
                }

                weight += setWeight;
                if (setEnd == null)
                    best = Math.min(best, cost(splitItems));
                else
                    best = Math.min(best, bruteForceCostWithSplitter(limit, setEnd, i + 1, union(splitItems, splitBy(setEnd))));
            }
            return best;
        }

        private static Set<SizedItem> union(Set<SizedItem> a, Set<SizedItem> b)
        {
            Set<SizedItem> result = new HashSet<>(a);
            result.addAll(b);
            return result;
        }

        @Override
        public String toString()
        {
            return items.toString();
        }
    }
}
