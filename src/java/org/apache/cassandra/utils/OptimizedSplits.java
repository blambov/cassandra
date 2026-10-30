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
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.ToDoubleFunction;

/// Splits a collection of items that span ranges of points into groups whose weight is within a limit, choosing the
/// split points to minimize the cost of splitting the items that span them.
///
/// The items are first arranged into overlap sets (see [Overlaps#constructOverlapSets]). A group can end at the
/// boundary between two sets, or, if it would otherwise go over the limit, inside the set that takes it over, at the
/// point where it reaches the limit. Splitting at a point splits all items that span it, and the cost of splitting an
/// item is charged once, however many times it is split.
public class OptimizedSplits
{
    /// Describes where items lie, how much they weigh, and how the space they lie in can be split.
    public interface ItemSpace<E, P extends Comparable<P>>
    {
        /// The point where the item starts.
        P startOf(E item);

        /// The point where the item ends. Items that only touch, i.e. where one ends at the point where the other
        /// starts, do not overlap.
        P endOf(E item);

        /// The weight of the slice of the item between the given points, which are always within the item's span.
        ///
        /// Weights must be additive, i.e. the weights of adjacent slices must add up to the weight of their union,
        /// and uniform with respect to [#split], i.e. the slice of an item between `sliceStart` and
        /// `split(sliceStart, sliceEnd, ratio)` must weigh `ratio` times the slice between `sliceStart` and
        /// `sliceEnd`.
        double weight(E item, P sliceStart, P sliceEnd);

        /// The point at the given ratio of the distance from `start` to `end`, or null if the span between them
        /// cannot be split. If the exact point cannot be represented, this should return the closest point before it,
        /// so that groups do not go over the limit.
        P split(P start, P end, double ratio);
    }

    public static class Split<E, P>
    {
        final P pointAfter;
        final double weight;
        final double accumulatedSplitCost;

        final Set<E> alreadySplit;
        final int groupIndex;
        final double remainingWeight;

        final Split<E, P> previous;

        Split(P pointAfter, double weight, double accumulatedSplitCost, Set<E> alreadySplit, int groupIndex, double remainingWeight, Split<E, P> previous)
        {
            this.pointAfter = pointAfter;
            this.weight = weight;
            this.accumulatedSplitCost = accumulatedSplitCost;
            this.alreadySplit = alreadySplit;
            this.groupIndex = groupIndex;
            this.remainingWeight = remainingWeight;
            this.previous = previous;
        }
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

        List<Set<E>> sets = Overlaps.constructOverlapSets(items,
                                                          (x, y) -> space.startOf(x).compareTo(space.endOf(y)) >= 0,
                                                          Comparator.comparing(space::startOf),
                                                          Comparator.comparing(space::endOf));
        return new OptimizedSplitter<>(sets, space, splitCoster, pickLimit).evaluate();
    }

    static class OptimizedSplitter<E, P extends Comparable<P>>
    {
        final List<Set<E>> sets;
        final ItemSpace<E, P> space;
        final ToDoubleFunction<E> splitCoster;
        final double pickLimit;

        /// The start of the first set.
        final P start;
        /// The end of the last set.
        final P end;
        /// The boundary after each set, null for the last one.
        final List<P> pointAfter;
        /// The items that span the boundary after each set.
        final List<Set<E>> toSplitAfter;
        final double[] weights;
        /// The weight profiles of the sets, built when a set first needs to be split.
        final List<SetProfile> profiles;

        // null must compare greater
        final NavigableMap<P, Split<E, P>> candidates = new TreeMap<>((x, y) -> x == null ? y == null ? 0 : 1
                                                                                          : y == null ? -1 : x.compareTo(y));

        OptimizedSplitter(List<Set<E>> sets, ItemSpace<E, P> space, ToDoubleFunction<E> splitCoster, double pickLimit)
        {
            assert !sets.isEmpty();
            this.sets = sets;
            this.space = space;
            this.splitCoster = splitCoster;
            this.pickLimit = pickLimit;
            int count = sets.size();
            start = sets.get(0).stream().map(space::startOf).min(Comparator.naturalOrder()).get();
            end = sets.get(count - 1).stream().map(space::endOf).max(Comparator.naturalOrder()).get();
            pointAfter = new ArrayList<>(count);
            toSplitAfter = new ArrayList<>(count);
            for (int i = 0; i + 1 < count; ++i)
            {
                Set<E> set = sets.get(i);
                // The items of the next set that are not in this one start after all items of this set that are not
                // in the next one end. Split at the start of the first of them.
                P boundary = null;
                Set<E> spanning = new HashSet<>();
                for (E item : sets.get(i + 1))
                {
                    if (set.contains(item))
                        spanning.add(item);
                    else if (boundary == null || space.startOf(item).compareTo(boundary) < 0)
                        boundary = space.startOf(item);
                }
                pointAfter.add(boundary);
                toSplitAfter.add(spanning);
            }
            pointAfter.add(null);
            toSplitAfter.add(Collections.emptySet());

            weights = new double[count];
            for (int i = 0; i < count; ++i)
                weights[i] = weight(sets.get(i), startOfSet(i), endOfSet(i));
            profiles = new ArrayList<>(Collections.nCopies(count, null));
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
            P sliceStart = Comparables.max(space.startOf(item), from);
            P sliceEnd = Comparables.min(space.endOf(item), to);
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
            Split<E, P> prev = null;
            Set<E> splitsSoFar = Collections.emptySet();
            int startIndex = 0;
            double weight = weights[0];
            P pointBefore = start;
            while (true)
            {
                Split<E, P> next = nextSplitMaybePartial(startIndex, pointBefore, true, prev, 0, weight, splitsSoFar);

                int index = startIndex;
                while (++index < weights.length && weight < pickLimit)
                {
                    Split<E, P> curr = next;
                    double nextWeight = weight + weights[index];
                    pointBefore = curr.pointAfter;
                    next = nextSplitMaybePartial(index, pointBefore, false, prev, weight, nextWeight, splitsSoFar);
                    // Even if next is cheaper, ending the group here may make cheaper splits possible later, because
                    // where a group that ends inside a set ends depends on where it starts.
                    updateCandidate(curr);
                    weight = nextWeight;
                }

                if (next != null)
                    updateCandidate(next);

                prev = candidates.pollFirstEntry().getValue();
                if (prev.pointAfter == null)
                    return prev;
                splitsSoFar = prev.alreadySplit;
                startIndex = prev.groupIndex;
                weight = prev.remainingWeight;
                pointBefore = prev.pointAfter;
            }
        }

        void updateCandidate(Split<E, P> candidate)
        {
            P position = candidate.pointAfter;
            Split<E, P> existing = candidates.get(position);
            if (existing == null || existing.accumulatedSplitCost > candidate.accumulatedSplitCost)
                candidates.put(position, candidate);
        }

        private Split<E, P> nextSplitMaybePartial(int index, P pointBefore, boolean startsGroup, Split<E, P> prev, double startWeight, double endWeight, Set<E> splitsSoFar)
        {
            if (endWeight <= pickLimit)
                return nextSplit(index, prev, endWeight, splitsSoFar);

            if (startWeight < pickLimit)
            {
                Split<E, P> split = splitInside(index, pointBefore, prev, startWeight, endWeight, splitsSoFar);
                if (split != null)
                    return split;
            }
            // The set can't be split to fit. If it starts the group, take all of it, even though it is over the limit.
            return startsGroup ? nextSplit(index, prev, endWeight, splitsSoFar) : null;
        }

        /// Split the set at the given index at the point after `pointBefore` where the group reaches the limit, or
        /// return null if the space can't be split there.
        private Split<E, P> splitInside(int index, P pointBefore, Split<E, P> prev, double startWeight, double endWeight, Set<E> splitsSoFar)
        {
            P splitPoint = pointForWeight(index, pointBefore, pickLimit - startWeight);
            if (splitPoint == null || splitPoint.compareTo(pointBefore) <= 0)
                return null;

            double weight = startWeight;
            Set<E> toSplit = new HashSet<>();
            for (E item : sets.get(index))
            {
                weight += weight(item, pointBefore, splitPoint);
                if (space.startOf(item).compareTo(splitPoint) < 0 && space.endOf(item).compareTo(splitPoint) > 0)
                    toSplit.add(item);
            }
            if (weight == startWeight)
                return null;
            assert weight <= pickLimit + 0.001;

            return nextSplit(index, splitPoint, prev, weight, splitsSoFar, toSplit, endWeight - weight);
        }

        /// The point after `from` in the set at the given index up to which the set's items weigh `weight`, or null if
        /// the space can't be split there. Between the points of the set's profile the set's weight is uniform, so the
        /// point can be found by splitting the span between the two profile points around it.
        private P pointForWeight(int index, P from, double weight)
        {
            SetProfile profile = profiles.get(index);
            if (profile == null)
            {
                profile = new SetProfile(index);
                profiles.set(index, profile);
            }

            double target = weight(sets.get(index), profile.points.get(0), from) + weight;
            // Find the last profile point where the weight from the start of the set does not exceed the target.
            int low = 0;
            int high = profile.points.size() - 1;
            while (low < high)
            {
                int mid = (low + high + 1) >>> 1;
                if (profile.weightTo[mid] <= target)
                    low = mid;
                else
                    high = mid - 1;
            }
            if (low == profile.points.size() - 1)
                return null;    // the rest of the set is within the weight

            double ratio = (target - profile.weightTo[low]) / (profile.weightTo[low + 1] - profile.weightTo[low]);
            return space.split(profile.points.get(low), profile.points.get(low + 1), ratio);
        }

        private Split<E, P> nextSplit(int index, Split<E, P> prev, double weight, Set<E> splitsSoFar)
        {
            return nextSplit(index + 1, pointAfter.get(index), prev, weight, splitsSoFar, toSplitAfter.get(index), index + 1 < weights.length ? weights[index + 1] : 0);
        }

        private Split<E, P> nextSplit(int index, P splitPoint, Split<E, P> prev, double weight, Set<E> splitsSoFar, Set<E> toSplit, double remainingWeight)
        {
            double cost = toSplit.stream().filter(x -> !splitsSoFar.contains(x)).mapToDouble(splitCoster::applyAsDouble).sum();
            return new Split<>(splitPoint,
                               weight,
                               cost + (prev != null ? prev.accumulatedSplitCost : 0.0),
                               toSplit,
                               index,
                               remainingWeight,
                               prev);
        }

        /// The points in a set's span where the set's weight distribution can change, i.e. the ends of the span and
        /// the starts and ends of items inside it, and the weight of the set from the start of the span to each point.
        class SetProfile
        {
            final List<P> points;
            final double[] weightTo;

            SetProfile(int index)
            {
                Set<E> set = sets.get(index);
                P setStart = startOfSet(index);
                P setEnd = endOfSet(index);
                NavigableSet<P> points = new TreeSet<>();
                points.add(setStart);
                points.add(setEnd);
                for (E item : set)
                {
                    for (P point : List.of(space.startOf(item), space.endOf(item)))
                        if (point.compareTo(setStart) > 0 && point.compareTo(setEnd) < 0)
                            points.add(point);
                }
                this.points = new ArrayList<>(points);
                weightTo = new double[this.points.size()];
                for (int i = 1; i < weightTo.length; ++i)
                    weightTo[i] = weightTo[i - 1] + weight(set, this.points.get(i - 1), this.points.get(i));
            }
        }
    }
}
