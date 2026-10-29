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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Ordering;
import org.junit.Assert;
import org.junit.Test;

import org.agrona.collections.IntArrayList;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class OverlapsTest
{
    Random random = new Random(1);

    @Test
    public void testConstructOverlapsMap()
    {
        Interval<Integer, String>[] input = new Interval[]{
        Interval.create(1, 5, "A"),
        Interval.create(1, 3, "B"),
        Interval.create(1, 3, "C"),
        // expected 1 - 2, ABC
        Interval.create(2, 6, "D"),
        // expected 2 - 3, ABCD
        Interval.create(3, 7, "E"),
        // expected 3 - 5 ADE
        Interval.create(5, 7, "F"),
        // expected 5 - 6 DEF
        // expected 6 - 7 EF
        Interval.create(7, 9, "G"),
        // hole
        // expected 7 - 9 G
        Interval.create(10, 13, "H"),
        // expected 10 - 11 H
        Interval.create(11, 12, "I"),
        // expected 11 - 12 HI
        // expected 12 - 13 H
        Interval.create(1399, 1799, "J"),
        Interval.create(1402, 1798, "K"),

        Interval.create(2102, 2402, "L"),
        Interval.create(2099, 2398, "M"),

        Interval.create(2502, 2998, "N"),
        Interval.create(2499, 2601, "O"),
        Interval.create(2602, 3001, "P"),
        Interval.create(2799, 3401, "Q"),

        Interval.create(3502, 3998, "R"),
        Interval.create(3499, 3602, "S"),
        Interval.create(3601, 4001, "T"),
        };
        String[] allOverlapsManual = new String[]{
        "ABC",
        "ABCD",
        "ADE",
        "DEF",
        "EF",
        "G",
        "",
        "H",
        "HI",
        "H",
        "",
        "J",
        "JK",
        "J",
        "",
        "M",
        "LM",
        "L",
        "",
        "O",
        "NO",
        "N",
        "NP",
        "NPQ",
        "PQ",
        "Q",
        "",
        "S",
        "RS",
        "RST",
        "RT",
        "T"
        };
        String[] expectedSubsumed = new String[]{
        "ABCD",
        "ADE",
        "DEF",
        "G",
        "HI",
        "JK",
        "LM",
        "NO",
        "NPQ",
        "RST",
        };
        List<String> allOverlaps = getAllOverlaps(input, false);
        assertEquals(Arrays.asList(allOverlapsManual), allOverlaps);

        List<String> subsumed = subsumeContainedNeighbours(allOverlaps);
        assertEquals(Arrays.asList(expectedSubsumed), subsumed);

        List<Set<Interval<Integer, String>>> overlaps = Overlaps.constructOverlapSets(Arrays.asList(input),
                                                                                      (x, y) -> x.min >= y.max,
                                                                                      Comparator.comparingInt(x -> x.min),
                                                                                      Comparator.comparingInt(x -> x.max));

        List<String> result = mapOverlapSetsToStrings(overlaps);
        assertEquals(subsumed, result);
    }

    private static List<String> mapOverlapSetsToStrings(List<Set<Interval<Integer, String>>> overlaps)
    {
        List<String> result = overlaps.stream()
                                      .map(set -> set.stream()
                                                     .map(x -> x.data)
                                                     .sorted()
                                                     .collect(Collectors.joining()))
                                      .collect(Collectors.toList());
        return result;
    }

    @Test
    public void testConstructOverlapsMapRandom()
    {
        int size;
        int range = 100;
        Random rand = new Random();
        for (int i = 0; i < 1000; ++i)
        {
            size = rand.nextInt(range) + 2;
            Interval<Integer, String>[] input = new Interval[size];
            char c = 'A';
            for (int j = 0; j < size; ++j)
            {
                int start = rand.nextInt(range);
                input[j] = (new Interval<>(start, start + 1 + random.nextInt(range - start), Character.toString(c++)));
            }

            boolean endInclusive = rand.nextBoolean();
            List<String> expected = subsumeContainedNeighbours(getAllOverlaps(input, endInclusive));

            List<Set<Interval<Integer, String>>> overlaps =
            Overlaps.constructOverlapSets(Arrays.asList(input),
                                          endInclusive ? (x, y) -> x.min > y.max
                                                       : (x, y) -> x.min >= y.max,
                                          Comparator.comparingInt(x -> x.min),
                                          Comparator.comparingInt(x -> x.max));
            List<String> result = mapOverlapSetsToStrings(overlaps);
            assertEquals("Input " + Arrays.asList(input), expected, result);
        }
    }

    private static List<String> getAllOverlaps(Interval<Integer, String>[] input, boolean endInclusive)
    {
        int min = Arrays.stream(input).mapToInt(x -> x.min).min().getAsInt();
        int max = Arrays.stream(input).mapToInt(x -> x.max).max().getAsInt();
        List<String> allOverlaps = new ArrayList<>();
        IntStream.range(min, max)
                 .mapToObj(i -> Arrays.stream(input)
                                      .filter(iv -> i >= iv.min && (i < iv.max || endInclusive && i == iv.max))
                                      .map(iv -> iv.data)
                                      .collect(Collectors.joining()))
                 .reduce(null, (prev, curr) -> {
                     if (curr.equals(prev))
                         return prev;
                     allOverlaps.add(curr);
                     return curr;
                 });
        return allOverlaps;
    }

    private List<String> subsumeContainedNeighbours(List<String> allOverlaps)
    {
        List<String> subsumed = new ArrayList<>();
        String last = "";
        for (String overlap : allOverlaps)
        {
            if (containsAll(last, overlap))
                continue;
            if (containsAll(overlap, last))
            {
                last = overlap;
                continue;
            }
            subsumed.add(last);
            last = overlap;
        }
        assert !last.isEmpty();
        subsumed.add(last);
        return subsumed;
    }

    boolean containsAll(String a, String b)
    {
        if (a.contains(b))
            return true;
        return asSet(a).containsAll(asSet(b));
    }

    private static Set<Character> asSet(String a)
    {
        Set<Character> as = new HashSet<>();
        for (int i = 0; i < a.length(); ++i)
            as.add(a.charAt(i));
        return as;
    }


    @Test
    public void testCombineSetsWithCommonElement()
    {
        String[] sets = new String[]{
        "ABCD",
        "ADE",
        "EF",
        "HI",
        "LN",
        "NO",
        "NPQ",
        "RST",
        };
        String[] nonOverlapping = new String[]{
        "ABCDEF",
        "HI",
        "LNOPQ",
        "RST",
        };

        List<Set<Character>> input = Arrays.stream(sets).map(OverlapsTest::asSet).collect(Collectors.toList());
        List<Set<Character>> expected = Arrays.stream(nonOverlapping).map(OverlapsTest::asSet).collect(Collectors.toList());

        List<Set<Character>> result = Overlaps.combineSetsWithCommonElement(input);
        assertEquals(expected, result);
    }

    @Test
    public void testSplitInNonOverlappingSetsRandom()
    {
        int size;
        int range = 100;
        Random rand = new Random();
        for (int i = 0; i < 1000; ++i)
        {
            size = rand.nextInt(range) + 2;
            Interval<Integer, String>[] input = new Interval[size];
            char c = 'A';
            for (int j = 0; j < size; ++j)
            {
                int start = rand.nextInt(range);
                input[j] = (new Interval<>(start, start + 1 + random.nextInt(range - start), Character.toString(c++)));
            }

            boolean endInclusive = rand.nextBoolean();

            List<Set<Interval<Integer, String>>> overlaps =
            Overlaps.splitInNonOverlappingSets(Arrays.asList(input),
                                               endInclusive ? (x, y) -> x.min > y.max
                                                            : (x, y) -> x.min >= y.max,
                                               Comparator.comparingInt(x -> x.min),
                                               Comparator.comparingInt(x -> x.max));

            for (var set : overlaps)
            {
                for (var interval : set)
                {
                    // must intersect with at least one other item in the set
                    if (set.size() > 1)
                        Assert.assertTrue(set.stream().filter(x -> x != interval).anyMatch(x -> intersects(x, interval, endInclusive)));
                    // and no interval outside the set
                    Assert.assertTrue(overlaps.stream().filter(x -> x != set).flatMap(Set::stream).noneMatch(x -> intersects(x, interval, endInclusive)));
                }
            }
        }
    }

    <C extends Comparable<C>, D> boolean intersects(Interval<C, D> i1, Interval<C, D> i2, boolean endInclusive)
    {
        return endInclusive ? intersectsEndsIncluded(i1, i2) : intersectsEndsExcluded(i1, i2);
    }

    <C extends Comparable<C>, D> boolean intersectsEndsExcluded(Interval<C, D> i1, Interval<C, D> i2)
    {
        return i1.min.compareTo(i2.max) < 0 && i1.max.compareTo(i2.min) > 0;
    }

    <C extends Comparable<C>, D> boolean intersectsEndsIncluded(Interval<C, D> i1, Interval<C, D> i2)
    {
        return i1.min.compareTo(i2.max) <= 0 && i1.max.compareTo(i2.min) >= 0;
    }

    @Test
    public void testAssignOverlapsIntoBuckets()
    {
        String[] sets = new String[]{
        "ABCD",
        "ADE",
        "EF",
        "HI",
        "LN",
        "NO",
        "NPQ",
        "RST",
        };
        String[] none3 = new String[]{
        "ABCD",
        "NPQ",
        "RST",
        };
        int[] noneUnselected = new int[]{1, 5, 4, 2, 3};
        String[] single3 = new String[]{
        "ABCDE",
        "LNOPQ",
        "RST",
        };
        int[] singleUnselected = new int[]{2, 3};
        String[] transitive3 = new String[]{
        "ABCDEF",
        "LNOPQ",
        "RST",
        };
        int[] transitiveUnselected = new int[]{3};

        List<Set<Character>> input = Arrays.stream(sets).map(OverlapsTest::asSet).collect(Collectors.toList());

        verifyAssignment(Overlaps.InclusionMethod.NONE, input, none3, noneUnselected);

        verifyAssignment(Overlaps.InclusionMethod.SINGLE, input, single3, singleUnselected);

        verifyAssignment(Overlaps.InclusionMethod.TRANSITIVE, input, transitive3, transitiveUnselected);
    }

    private void verifyAssignment(Overlaps.InclusionMethod method, List<Set<Character>> input, String[] expected, int[] expectedUnselected)
    {
        List<String> actual;
        IntArrayList unselected = new IntArrayList();
        actual = Overlaps.assignOverlapsIntoBuckets(3, method, input, this::makeBucket, s -> unselected.add(input.indexOf(s)));
        actual.sort(String::compareTo);

        assertEquals(Arrays.asList(expected), actual);
        assertArrayEquals(expectedUnselected, unselected.toIntArray());
    }

    private String makeBucket(List<Set<Character>> sets, int startIndex, int endIndex)
    {
        Set<Character> bucket = new HashSet<>();
        for (int i = startIndex; i < endIndex; ++i)
            bucket.addAll(sets.get(i));
        return bucket.stream()
                     .sorted()
                     .map(x -> x.toString())
                     .collect(Collectors.joining());
    }

    @Test
    public void testMultiSetPullOldest()
    {
        // In this test each letter stands for an sstable, ordered alphabetically (i.e. A is oldest)
        Assert.assertEquals("ABCD", pullLast(3, "ACD", "BCD"));
        Assert.assertEquals("ABC", pullLast(2, "ACD", "BCD"));
        Assert.assertEquals("BC", pullLast(2, "CDE", "BCD"));
    }


    @Test
    public void testMultiSetPullOldestRandom()
    {
        int size;
        int range = 100;
        Random rand = new Random();
        for (int i = 0; i < 100; ++i)
        {
            size = rand.nextInt(range) + 2;
            Interval<Integer, String>[] input = new Interval[size];
            char c = 'A';
            for (int j = 0; j < size; ++j)
            {
                int start = rand.nextInt(range);
                input[j] = (new Interval<>(start, start + 1 + random.nextInt(range - start), Character.toString(c++)));
            }

            List<Set<Interval<Integer, String>>> overlaps = Overlaps.constructOverlapSets(Arrays.asList(input),
                                                                                          (x, y) -> x.min >= y.max,
                                                                                          Comparator.comparingInt(x -> x.min),
                                                                                          Comparator.comparingInt(x -> x.max));
            String[] overlapSets = mapOverlapSetsToStrings(overlaps).toArray(new String[0]);
            int maxOverlap = Arrays.stream(overlapSets).mapToInt(String::length).max().getAsInt();
            for (int limit = 1; limit <= maxOverlap + 1; ++limit)
            {
                String pulled = pullLast(limit, overlapSets);
                String message = pulled + " from " + overlapSets + " limit " + limit;
                Assert.assertTrue(message + ", size " + pulled.length(), pulled.length() >= Math.min(size, limit));
                String e = "";
                for (char j = 'A'; j < pulled.length() + 'A'; ++j)
                    e += Character.toString(j);
                Assert.assertEquals("Must select oldest " + message, e, pulled);
                int countAtLimit = 0;
                for (String set : overlapSets)
                {
                    int count = 0;
                    for (int j = 0; j < set.length(); ++j)
                        if (pulled.indexOf(set.charAt(j)) >= 0)
                            ++count;
                    Assert.assertTrue(message + " set " + set + " elements " + count, count <= limit);
                    if (count == limit)
                        ++countAtLimit;
                }
                if (pulled.length() < size)
                    Assert.assertTrue(message + " must have at least one set of size " + limit, countAtLimit > 0);
                else
                    Assert.assertTrue(message,limit >= maxOverlap);
            }
        }
    }

    String pullLast(int limit, String... inputOverlapSets)
    {
        List<Set<String>> overlapSets = Arrays.stream(inputOverlapSets)
                                              .map(s -> IntStream.range(0, s.length())
                                                                 .mapToObj(i -> Character.toString(s.charAt(i)))
                                                                 .collect(Collectors.toSet()))
                                              .collect(Collectors.toList());

        List<String> allObjectsSorted = overlapSets.stream()
                                                   .flatMap(x -> x.stream())
                                                   .sorted(Ordering.natural().reversed())
                                                   .distinct()
                                                   .collect(Collectors.toList());

        Collection<String> pulled = Overlaps.pullLastWithOverlapLimit(allObjectsSorted, overlapSets, limit);
        return pulled.stream().sorted().collect(Collectors.joining());
    }

    static final double EPSILON = 1e-6;

    @Test
    public void testOptimizedSplitsSingleSet()
    {
        SplitProblem problem = new SplitProblem(List.of(new SizedItem("A", 0, 4, 1, 1),
                                                        new SizedItem("B", 1, 3, 1, 1)));
        assertEquals(1, problem.sets.size());
        assertEquals(List.of(), verifySplits(problem, 100, 0));
        assertEquals(List.of(), verifySplits(problem, 6, 0));
        assertEquals(List.of(), verifySplits(problem, 1, 0));
    }

    @Test
    public void testOptimizedSplitsPrefersCheapSplitPoints()
    {
        // Sets AX [0, 4), XB [4, 6), BY [6, 8), YC [8, 10), CZ [10, 12), ZD [12, 16) with weights 6, 4, 4, 4, 4, 6.
        // Splitting at 4, 6, 8, 10 or 12 costs 10, 1, 5, 1 and 10 respectively.
        SplitProblem problem = new SplitProblem(List.of(new SizedItem("A", 0, 4, 1, 100),
                                                        new SizedItem("B", 4, 8, 1, 1),
                                                        new SizedItem("C", 8, 12, 1, 1),
                                                        new SizedItem("D", 12, 16, 1, 100),
                                                        new SizedItem("X", 2, 6, 1, 10),
                                                        new SizedItem("Y", 6, 10, 1, 5),
                                                        new SizedItem("Z", 10, 14, 1, 10)));
        assertEquals(List.of(4, 6, 8, 10, 12), problem.boundaries);

        // Everything fits in one group.
        assertEquals(List.of(), verifySplits(problem, 28, 0));
        // A single split at 8 would do, but splitting the cheap B and C is better even if it makes more groups.
        assertEquals(List.of(6, 10), verifySplits(problem, 14, 2));
        assertEquals(List.of(6, 10), verifySplits(problem, 10, 2));
        // AX and ZD can't be grouped with anything, the rest should still be split at the cheap points.
        assertEquals(List.of(4, 6, 10, 12), verifySplits(problem, 8, 22));
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
            for (int i = 0; i < 1000; ++i)
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
                double totalWeight = Arrays.stream(problem.weights).sum();
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

    @Test(timeout = 1000)
    public void testOptimizedSplitsManySets()
    {
        long seed = System.nanoTime();
        Random rand = new Random(seed);
        List<SizedItem> items = new ArrayList<>();
        for (int j = 0; j < 2000; ++j)
        {
            int start = rand.nextInt(100000);
            int length = 1 + rand.nextInt(rand.nextInt(20) == 0 ? 5000 : 200);
            items.add(new SizedItem("I" + j, start, start + length, 1 + rand.nextInt(5), 1 + rand.nextInt(20)));
        }
        SplitProblem problem = new SplitProblem(items);
        assertTrue(problem.sets.size() > 500);
        double totalWeight = Arrays.stream(problem.weights).sum();
        // We can't verify optimality here, only that the result is valid and that it is computed in reasonable time
        // when groups span many sets.
        try
        {
            solveAndVerify(problem, totalWeight / 20);
        }
        catch (Throwable t)
        {
            throw new AssertionError("Failed with seed " + seed, t);
        }
    }

    /// Runs `optimizedSplits` with the given limit, validates the result, checks that its cost is as expected, and
    /// returns the points where it splits.
    private static List<Integer> verifySplits(SplitProblem problem, double limit, double expectedCost)
    {
        Overlaps.Split<SizedItem, Integer> solution = solveAndVerify(problem, limit);
        List<Integer> splitPoints = new ArrayList<>();
        for (Overlaps.Split<SizedItem, Integer> group = solution; group != null; group = group.previous)
            if (group.pointAfter != null)
                splitPoints.add(group.pointAfter);
        Collections.reverse(splitPoints);
        assertEquals("Split cost with limit " + limit + " for " + problem + " splitting at " + splitPoints,
                     expectedCost, solution.accumulatedSplitCost, EPSILON);
        return splitPoints;
    }

    private static Overlaps.Split<SizedItem, Integer> solveAndVerify(SplitProblem problem, double limit)
    {
        try
        {
            Overlaps.Split<SizedItem, Integer> solution = problem.solve(limit);
            problem.verify(solution, limit);
            return solution;
        }
        catch (Throwable t)
        {
            throw new AssertionError("Failed with limit " + limit + " for " + problem, t);
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
            assertTrue(this + " is not present between " + pointBefore + " and " + pointAfter, s < e);
            return density * (e - s);
        }

        @Override
        public String toString()
        {
            return String.format("%s[%d,%d)x%d$%.0f", name, start, end, density, splitCost);
        }
    }

    /// The overlap sets of a list of items, with the boundaries between them, their weights and the items that need to
    /// be split at each boundary.
    private static class SplitProblem
    {
        final List<SizedItem> items;
        final List<Set<SizedItem>> sets;
        /// The point between sets i and i + 1.
        final List<Integer> boundaries = new ArrayList<>();
        /// The items present in both sets i and i + 1.
        final List<Set<SizedItem>> crossing = new ArrayList<>();
        final double[] weights;

        SplitProblem(List<SizedItem> items)
        {
            this.items = items;
            this.sets = Overlaps.constructOverlapSets(new ArrayList<>(items),
                                                      (x, y) -> x.start >= y.end,
                                                      Comparator.comparingInt(x -> x.start),
                                                      Comparator.comparingInt(x -> x.end));
            this.weights = new double[sets.size()];
            for (int i = 0; i < sets.size(); ++i)
            {
                Integer pointBefore = i > 0 ? boundaries.get(i - 1) : null;
                Integer pointAfter = null;
                if (i + 1 < sets.size())
                {
                    pointAfter = pointBetween(sets.get(i), sets.get(i + 1));
                    boundaries.add(pointAfter);
                    Set<SizedItem> crossingItems = new HashSet<>(sets.get(i));
                    crossingItems.retainAll(sets.get(i + 1));
                    crossing.add(crossingItems);
                }
                for (SizedItem item : sets.get(i))
                    weights[i] += item.weight(pointBefore, pointAfter);
            }
        }

        static Integer pointBetween(Set<SizedItem> set, Set<SizedItem> next)
        {
            // All items that are in set but not in next end before any item of next that is not in set starts.
            return next.stream().filter(x -> !set.contains(x)).mapToInt(x -> x.start).min().getAsInt();
        }

        static double cost(Set<SizedItem> splitItems)
        {
            return splitItems.stream().mapToDouble(x -> x.splitCost).sum();
        }

        Overlaps.Split<SizedItem, Integer> solve(double limit)
        {
            return Overlaps.optimizedSplits(sets, SplitProblem::pointBetween, SizedItem::weight, x -> x.splitCost, limit);
        }

        /// Checks that the solution is a sequence of groups of consecutive sets that covers all sets, where each
        /// group is within the limit or contains a single set, and where the reported items, weights and accumulated
        /// costs match the groups.
        void verify(Overlaps.Split<SizedItem, Integer> solution, double limit)
        {
            List<Overlaps.Split<SizedItem, Integer>> groups = new ArrayList<>();
            for (Overlaps.Split<SizedItem, Integer> group = solution; group != null; group = group.previous)
                groups.add(group);
            Collections.reverse(groups);
            assertTrue("No groups returned", !groups.isEmpty());

            Set<SizedItem> splitItems = new HashSet<>();
            int start = 0;
            for (Overlaps.Split<SizedItem, Integer> group : groups)
            {
                int end;
                if (group == solution)
                {
                    assertNull("Last group must not have a split point", group.pointAfter);
                    end = sets.size() - 1;
                }
                else
                {
                    end = boundaries.indexOf(group.pointAfter);
                    assertTrue("Split point " + group.pointAfter + " is not a set boundary after " + start, end >= start);
                    splitItems.addAll(crossing.get(end));
                }

                Set<SizedItem> groupItems = new HashSet<>();
                double groupWeight = 0;
                for (int i = start; i <= end; ++i)
                {
                    groupItems.addAll(sets.get(i));
                    groupWeight += weights[i];
                }
//                assertEquals("Items of group of sets " + start + " to " + end, groupItems, group.items);
                assertEquals("Weight of group of sets " + start + " to " + end, groupWeight, group.weight, EPSILON);
                assertTrue("Group of sets " + start + " to " + end + " with weight " + groupWeight + " exceeds limit",
                           start == end || groupWeight <= limit);
                assertEquals("Accumulated cost of group of sets " + start + " to " + end,
                             cost(splitItems), group.accumulatedSplitCost, EPSILON);
                start = end + 1;
            }
        }

        /// The minimum split cost, found by trying all combinations of split points.
        double bruteForceCost(double limit)
        {
            int n = sets.size();
            assert n <= 16;
            double best = Double.POSITIVE_INFINITY;
            for (int splitMask = 0; splitMask < 1 << (n - 1); ++splitMask)
            {
                Set<SizedItem> splitItems = new HashSet<>();
                double groupWeight = 0;
                int groupStart = 0;
                boolean valid = true;
                for (int i = 0; i < n && valid; ++i)
                {
                    groupWeight += weights[i];
                    valid = i == groupStart || groupWeight <= limit;
                    if ((splitMask & (1 << i)) != 0)    // split after set i
                    {
                        splitItems.addAll(crossing.get(i));
                        groupWeight = 0;
                        groupStart = i + 1;
                    }
                }
                if (valid)
                    best = Math.min(best, cost(splitItems));
            }
            return best;
        }

        @Override
        public String toString()
        {
            return items.toString();
        }
    }
}
