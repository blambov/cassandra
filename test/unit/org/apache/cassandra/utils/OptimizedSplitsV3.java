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

import org.apache.cassandra.utils.OptimizedSplits.ItemSpace;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.ToDoubleFunction;

import com.carrotsearch.hppc.DoubleArrayList;
import org.jspecify.annotations.Nullable;

/// Splits a collection of items that span ranges of points into groups whose weight is within a limit, choosing the
/// split points to minimize the cost of splitting the items that span them.
///
/// The items are first arranged into overlap sets (see [Overlaps#constructOverlapSets]). A group can end at the
/// boundary between two sets, or, if it would otherwise go over the limit, inside the set that takes it over, at the
/// point where it reaches the limit. Splitting at a point splits all items that span it, and the cost of splitting an
/// item is charged once, however many times it is split.
public class OptimizedSplitsV3
{
    public static class Split<E, P>
    {
        public final P pointAfter;
        public final double weight;
        public final double accumulatedSplitCost;

        public final Split<E, P> previous;

        Split(P pointAfter, double weight, double accumulatedSplitCost, Split<E, P> previous)
        {
            this.pointAfter = pointAfter;
            this.weight = weight;
            this.accumulatedSplitCost = accumulatedSplitCost;
            this.previous = previous;
        }
    }

    interface Reducer<E, P, R>
    {
        R apply(R state, Collection<E> collection, P pointBefore);
    }

    static <E, P extends Comparable<P>, R> R constructOverlapSets(Collection<E> items,
                                                                  ItemSpace<E, P> space,
                                                                  Reducer<E, P, R> reducer,
                                                                  R initialValue)
    {
        R overlaps = initialValue;
        if (items.isEmpty())
            return overlaps;

        PriorityQueue<E> active = new PriorityQueue<>(Comparator.comparing(space::endOf));
        SortingIterator<E> itemsSorted = SortingIterator.create(Comparator.comparing(space::startOf), items);
        while (itemsSorted.hasNext())
        {
            E item = itemsSorted.next();
            P itemStart = space.startOf(item);

            while (!active.isEmpty())
            {
                P nextEnd = space.endOf(active.peek());
                int cmp = nextEnd.compareTo(itemStart);
                if (cmp > 0)
                    break;

                active.poll();
                while (!active.isEmpty() && space.endOf(active.peek()).compareTo(nextEnd) == 0)
                    active.poll();
                if (!active.isEmpty() && cmp != 0)
                    overlaps = reducer.apply(overlaps, active, nextEnd); // point before the new set is active
            }

            active.add(item);
            while (itemsSorted.hasNext() && space.startOf(itemsSorted.peek()).compareTo(itemStart) == 0)
                active.add(itemsSorted.next());

            overlaps = reducer.apply(overlaps, active, itemStart); // point before the new set is active
        }

        while (!active.isEmpty())
        {
            P nextEnd = space.endOf(active.peek());

            active.poll();
            while (!active.isEmpty() && space.endOf(active.peek()).compareTo(nextEnd) == 0)
                active.poll();
            if (!active.isEmpty())
                overlaps = reducer.apply(overlaps, active, nextEnd);
        }

        return overlaps;
    }

    /// Split the given items into groups with weight at most `pickLimit`, minimizing the total split cost of the items
    /// that need to be split. A group can only be over the limit if it is a single overlap set, or the rest of one,
    /// that the space cannot split further.
    ///
    /// @param items       The items to split.
    /// @param space       Where the items lie, and how to weigh them and split the space.
    /// @param splitCoster The cost of splitting each item, charged once however many times the item is split.
    /// @param pickLimit   The maximum weight of a group.
    /// @return The last group of the solution, with the earlier ones linked through [Split#previous], or null if
    ///         there are no items. The [Split#pointAfter] of each group is the point where it ends, or null for the last
    ///         group.
    public static <E, P extends Comparable<P>> Split<E, P> optimizedSplits(Collection<E> items,
                                                                           ItemSpace<E, P> space,
                                                                           ToDoubleFunction<E> splitCoster,
                                                                           double pickLimit)
    {
        if (items.isEmpty())
            return null;

        return new OptimizedSplitter<>(items, space, splitCoster, pickLimit).evaluate();
    }

    static class OptimizedSplitter<E, P extends Comparable<P>>
    {
        final ItemSpace<E, P> space;
        final ToDoubleFunction<E> splitCoster;
        final double pickLimit;

        /// The start of the first set.
        final P start;
        /// The end of the last set.
        final P end;

        final List<Set<E>> sets;

        /// The boundary after each set, null for the last one.
        List<P> pointAfter;
        /// The items that span the boundary after each set.
        final List<Set<E>> toSplitAfter;

        final double[] weights;

        final List<Split<E, P>> candidates = new ArrayList<>();

        OptimizedSplitter(Collection<E> items, ItemSpace<E, P> space, ToDoubleFunction<E> splitCoster, double pickLimit)
        {
            assert !items.isEmpty();

            this.space = space;
            sets = new ArrayList<>();
            pointAfter = new ArrayList<>();

            this.splitCoster = splitCoster;
            this.pickLimit = pickLimit;

            constructOverlapSets(items,
                                 space,
                                 (s, active, pointBefore) -> {
                                     if (!s.sets.isEmpty())
                                         s.pointAfter.add(pointBefore); // skip the first point-before
                                     s.sets.add(new HashSet<>(active));
                                     return s;
                                 },
                                 this);
            pointAfter.add(null);

            int count = sets.size();
            start = sets.get(0).stream().map(space::startOf).min(Comparator.naturalOrder()).get();
            end = sets.get(count - 1).stream().map(space::endOf).max(Comparator.naturalOrder()).get();

            var ws = new DoubleArrayList(count);
            toSplitAfter = new ArrayList<>(count);
            int i;
            for (i = 0; i < count; ++i)
            {
                Set<E> set = sets.get(i);
                double weight = weight(sets.get(i), startOfSet(i), endOfSet(i));
                while (weight > pickLimit)
                {
                    double splitWeight = trySplit(i, set, weight, ws);
                    if (splitWeight == 0.0)
                        break; // we can't pick a split point; use as is

                    ++i; ++count;
                    weight -= splitWeight;
                }

                ws.add(weight);
                Set<E> spanning = new HashSet<>();
                if (i < count - 1)
                {
                    for (E item : sets.get(i + 1))
                    {
                        if (set.contains(item))
                            spanning.add(item);
                    }
                }
                toSplitAfter.add(spanning);
                candidates.add(null);
            }
            candidates.add(null);
            weights = ws.toArray();
        }

        private double trySplit(int i, Set<E> set, double weight, DoubleArrayList ws)
        {
            // we need to break this group to make it fit into pickLimit sections
            P itemsEnd = endOfSet(i);
            for (E item : set)
                itemsEnd = Comparables.min(itemsEnd, space.endOf(item));
            P pointBefore = startOfSet(i);

            // endWeight - startWeight is the weight of the rest of the set, from pointBefore
            P splitPoint = space.split(pointBefore, itemsEnd, pickLimit / weight);
            if (splitPoint == null || splitPoint.compareTo(pointBefore) <= 0 || splitPoint.compareTo(itemsEnd) >= 0)
                return 0;

            double splitWeight = weight(set, pointBefore, splitPoint);
            if (splitWeight >= weight)
                return 0;

            assert splitWeight <= pickLimit + 0.001;

            // duplicate set
            sets.add(i, set);
            pointAfter.add(i, splitPoint);
            toSplitAfter.add(set);
            candidates.add(null);
            ws.add(splitWeight);
            return splitWeight;
        }

        P startOfSet(int index)
        {
            return index > 0 ? pointAfter.get(index - 1) : start;
        }

        P endOfSet(int index)
        {
            return index + 1 < sets.size() ? pointAfter.get(index) : end;
        }

        /// The weight of the part of the item between the given points.
        double weight(E item, P from, P to)
        {
            P sliceStart = from != null ? Comparables.max(space.startOf(item), from) : space.startOf(item);
            P sliceEnd = to != null ? Comparables.min(space.endOf(item), to) : space.endOf(item);
            return sliceStart.compareTo(sliceEnd) < 0 ? space.weight(item, sliceStart, sliceEnd) : 0;
        }

        double weight(Set<E> items, P from, P to)
        {
            double sum = 0.0;
            for (E item : items)
                sum += weight(item, from, to);
            return sum;
        }

        Split<E, P> evaluate()
        {
            for (int startIndex = 0; startIndex < weights.length; ++startIndex)
            {
                Split<E, P> prev = candidates.get(startIndex);
                if (prev == null && startIndex > 0)
                    continue;

                Set<E> splitsSoFar = startIndex > 0 ? this.toSplitAfter.get(startIndex - 1) : Collections.emptySet();
                double weight = weights[startIndex];
                Split<E, P> next = nextSplit(startIndex, prev, weight, splitsSoFar);
                if (weight > pickLimit)
                {
                    // This set on its own is bigger than the pick limit, and we could not split it in preprocessing.
                    // Add it whole and split after.
                    updateCandidate(startIndex + 1, next);
                    continue;
                }

                int index = startIndex;
                while (++index < weights.length && next != null)
                {
                    Split<E, P> curr = next;
                    weight += weights[index];
                    next = weight <= pickLimit ? nextSplit(index, prev, weight, splitsSoFar) : null;
                    if (next == null || next.accumulatedSplitCost > curr.accumulatedSplitCost)
                        updateCandidate(index, curr);
                    // else we don't need to evaluate this one as it's always cheaper to take next group
                }

                if (next != null)
                    updateCandidate(index, next);
            }
            return candidates.get(weights.length);
        }

        void updateCandidate(int index, Split<E, P> candidate)
        {
            Split<E, P> existing = candidates.get(index);
            if (existing == null || existing.accumulatedSplitCost > candidate.accumulatedSplitCost)
                candidates.set(index, candidate);
        }


        private Split<E, P> nextSplit(int index, Split<E, P> prev, double weight, Set<E> splitsSoFar)
        {
            Set<E> toSplitAfter = this.toSplitAfter.get(index);
            double cost = toSplitAfter.stream().filter(x -> !splitsSoFar.contains(x)).mapToDouble(splitCoster::applyAsDouble).sum();
            Split<E, P> next = new Split<>(pointAfter.get(index), weight, cost + (prev != null ? prev.accumulatedSplitCost : 0.0), prev);
            return next;
        }
    }
}
