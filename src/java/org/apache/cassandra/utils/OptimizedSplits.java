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
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
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
        public final P pointAfter;
        public final double weight;
        public final double accumulatedSplitCost;

        final Set<E> alreadySplit;
        final int groupIndex;
        final double remainingWeight;

        public final Split<E, P> previous;

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
        final List<Set<E>> sets;
        final ItemSpace<E, P> space;
        final ToDoubleFunction<E> splitCoster;
        final double pickLimit;

        /// The start of the first set.
        final P start;
        /// The end of the last set.
        final P end;
        /// The boundary after each set, null for the last one.
        List<P> pointAfter;
        /// The items that span the boundary after each set.
        final List<Set<E>> toSplitAfter;
        final double[] weights;

        // null must compare greater
        final NavigableMap<P, Split<E, P>> candidates = new TreeMap<>((x, y) -> x == null ? y == null ? 0 : 1
                                                                                          : y == null ? -1 : x.compareTo(y));

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

            toSplitAfter = new ArrayList<>(count);
            for (int i = 0; i + 1 < count; ++i)
            {
                Set<E> set = sets.get(i);
                // The items of the next set that are not in this one start after all items of this set that are not
                // in the next one end. Split at the start of the first of them.
                Set<E> spanning = new HashSet<>();
                for (E item : sets.get(i + 1))
                {
                    if (set.contains(item))
                        spanning.add(item);
                }
                toSplitAfter.add(spanning);
            }
            toSplitAfter.add(Collections.emptySet());

            weights = new double[count];
            for (int i = 0; i < count; ++i)
                weights[i] = weight(sets.get(i), startOfSet(i), endOfSet(i));
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

                    if (next == null || next.accumulatedSplitCost > curr.accumulatedSplitCost)
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
            var ceiling = candidates.ceilingEntry(position);
            if (ceiling != null && candidate.accumulatedSplitCost >= ceiling.getValue().accumulatedSplitCost)
                return; // Skip, we already have a better candidate.

            candidates.put(position, candidate); // If the position matches ceiling's, this will replace it.

            // Prune all worse candidates.
            // Anything with non-lower cost at an earlier position cannot yield a better result.
            var iter = candidates.headMap(position, false).descendingMap().entrySet().iterator();
            while (iter.hasNext())
            {
                var next = iter.next();
                if (next.getValue().accumulatedSplitCost < candidate.accumulatedSplitCost)
                    break;
                iter.remove();
            }
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
        ///
        /// All items of a set span all of it, except that if a gap follows the set, its items all end where the gap
        /// starts. So the set's weight is spread uniformly up to where its items end, and all of its items span any
        /// point before that. The point where the group reaches the limit is thus as far from `pointBefore` towards
        /// the end of the items as the weight that still fits is of the weight of the rest of the set.
        private Split<E, P> splitInside(int index, P pointBefore, Split<E, P> prev, double startWeight, double endWeight, Set<E> splitsSoFar)
        {
            Set<E> set = sets.get(index);
            P itemsEnd = endOfSet(index);
            for (E item : set)
                itemsEnd = Comparables.min(itemsEnd, space.endOf(item));

            // endWeight - startWeight is the weight of the rest of the set, from pointBefore
            P splitPoint = space.split(pointBefore, itemsEnd, (pickLimit - startWeight) / (endWeight - startWeight));
            if (splitPoint == null || splitPoint.compareTo(pointBefore) <= 0 || splitPoint.compareTo(itemsEnd) >= 0)
                return null;

            double weight = startWeight + weight(set, pointBefore, splitPoint);
            if (weight == startWeight)
                return null;
            assert weight <= pickLimit + 0.001;

            return nextSplit(index, splitPoint, prev, weight, splitsSoFar, set, endWeight - weight);
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
    }
}
