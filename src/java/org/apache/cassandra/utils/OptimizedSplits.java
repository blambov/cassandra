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

import javax.annotation.Nullable;

import com.carrotsearch.hppc.DoubleArrayList;

/// Splits a collection of items that span ranges of points into groups whose weight is within a limit, choosing the
/// split points to minimize the cost of splitting the items that span them, and then the number of groups.
///
/// The span of the items is first cut into sets, with a new set at every point where an item starts or ends, so that
/// the items of a set are present over all of it (see [#constructOverlapSets]). Splitting at a point splits the items
/// present on both sides of it: at the boundary between two sets the items that are in both, and inside a set all of
/// its items. The cost of splitting an item is charged once, however many times it is split.
///
/// A group can end at any set boundary that keeps it within the limit, or, if it would go over the limit, inside the
/// set that takes it over, at the point where it reaches the limit. Groups thus end inside sets that are over the limit
/// on their own, which have to be split, but also inside smaller sets. That never makes the cost lower, as the group
/// could end at the start of the set instead, followed by a group of the set alone, but it can make fewer and fuller
/// groups. This matters because items rarely start and end at exactly the same points, which leaves small sets between
/// their ends that would otherwise make small groups.
///
/// The search goes through the points where groups can end in order, keeping the best way found to split up to each
/// of them: the one that costs least, and of those the one with the fewest groups. It drops a point when a later one
/// has a way that costs less, or that costs as much with no more groups. The later point is never worse: a group that
/// starts from it can end at any boundary beyond it that a group from the earlier point can end at, and where that
/// group ends inside a set, it can end inside the same set too, or at the end of the set, splitting no more items in
/// either case.
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
        @Nullable
        P split(P start, P end, double ratio);
    }

    /// A group of items in a solution, linked to the group before it.
    public static class Split<P>
    {
        /// The point where the group ends, i.e. where it is split from the next group, or null for the last group.
        public final @Nullable P pointAfter;
        /// The weight of the group.
        public final double weight;
        /// The cost of the items split at the ends of this group and all groups before it.
        public final double accumulatedSplitCost;
        /// The number of groups up to and including this one.
        public final int groupCount;
        /// The group before this one, or null for the first group.
        public final @Nullable Split<P> previous;

        Split(@Nullable P pointAfter, double weight, double accumulatedSplitCost, @Nullable Split<P> previous)
        {
            this.pointAfter = pointAfter;
            this.weight = weight;
            this.accumulatedSplitCost = accumulatedSplitCost;
            this.groupCount = previous != null ? previous.groupCount + 1 : 1;
            this.previous = previous;
        }

        /// Whether this is a better way to split up to a point than the other, i.e. it costs less, or as much with fewer
        /// groups.
        boolean isBetterThan(Split<P> other)
        {
            return accumulatedSplitCost < other.accumulatedSplitCost ||
                   accumulatedSplitCost == other.accumulatedSplitCost && groupCount < other.groupCount;
        }
    }

    /// Receives the sets of items made by [#constructOverlapSets].
    interface Reducer<E, P, R>
    {
        /// Process the set of the given items, which starts at the given point. The collection is only valid during
        /// the call.
        R apply(R state, Collection<E> items, P start);
    }

    /// Cut the span of the given items into sets, with a new set at every point where an item starts or ends, and pass
    /// each set and the point where it starts to the reducer, in order. The items of a set are thus present over all
    /// of it, except that a gap where no items are present belongs to the set before it, whose items all end where
    /// the gap starts.
    ///
    /// @return The result of the last call to the reducer, or `initialValue` if there are no items.
    static <E, P extends Comparable<P>, R> R constructOverlapSets(Collection<E> items,
                                                                  ItemSpace<E, P> space,
                                                                  Reducer<E, P, R> reducer,
                                                                  R initialValue)
    {
        R state = initialValue;
        if (items.isEmpty())
            return state;

        PriorityQueue<E> present = new PriorityQueue<>(Comparator.comparing(space::endOf));
        SortingIterator<E> sorted = SortingIterator.create(Comparator.comparing(space::startOf), items);
        while (sorted.hasNext())
        {
            P start = space.startOf(sorted.peek());
            state = removeEndedItems(present, start, space, reducer, state);
            do
                present.add(sorted.next());
            while (sorted.hasNext() && space.startOf(sorted.peek()).compareTo(start) == 0);
            state = reducer.apply(state, present, start);
        }
        return removeEndedItems(present, null, space, reducer, state);
    }

    /// Remove the present items that end at or before `limit`, or all of them if it is null, and pass the items that
    /// remain after each point where some end to the reducer, unless it is `limit`, where new items start.
    private static <E, P extends Comparable<P>, R> R removeEndedItems(PriorityQueue<E> present,
                                                                      @Nullable P limit,
                                                                      ItemSpace<E, P> space,
                                                                      Reducer<E, P, R> reducer,
                                                                      R state)
    {
        while (!present.isEmpty())
        {
            P end = space.endOf(present.peek());
            int cmp = limit != null ? end.compareTo(limit) : -1;
            if (cmp > 0)
                break;

            do
                present.poll();
            while (!present.isEmpty() && space.endOf(present.peek()).compareTo(end) == 0);

            if (cmp < 0 && !present.isEmpty())
                state = reducer.apply(state, present, end);
        }
        return state;
    }

    /// Split the given items into groups with weight at most `pickLimit`, minimizing the total split cost of the items
    /// that need to be split, and then the number of groups. A group can only be over the limit if it is a single set,
    /// or the rest of one, that the space cannot split further.
    ///
    /// @param items       The items to split.
    /// @param space       Where the items lie, and how to weigh them and split the space.
    /// @param splitCoster The cost of splitting each item, charged once however many times the item is split.
    /// @param pickLimit   The maximum weight of a group.
    /// @return The last group of the solution, with the earlier ones linked through [Split#previous], or null if
    ///         there are no items.
    @Nullable
    public static <E, P extends Comparable<P>> Split<P> optimizedSplits(Collection<E> items,
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

        /// The items of each set.
        final List<Set<E>> sets = new ArrayList<>();
        /// The point where each set starts.
        final List<P> starts = new ArrayList<>();
        /// The point where the items of each set end, which is where the next set starts, unless a gap follows it.
        final List<P> itemsEnds = new ArrayList<>();
        /// The weight of each set.
        final DoubleArrayList weights = new DoubleArrayList();
        /// The items split by splitting at the end of each set, i.e. the ones that are in both it and the next set.
        final List<Set<E>> toSplitAfter = new ArrayList<>();

        /// The best way found to split up to each point where a group can end that may still be part of the best
        /// solution. Both their costs and, among points with the same cost, their numbers of groups increase with the
        /// point, as a later point with a way that is no worse makes the earlier one useless.
        final NavigableMap<P, Candidate> frontier = new TreeMap<>();

        OptimizedSplitter(Collection<E> items, ItemSpace<E, P> space, ToDoubleFunction<E> splitCoster, double pickLimit)
        {
            this.space = space;
            this.splitCoster = splitCoster;
            this.pickLimit = pickLimit;

            constructOverlapSets(items,
                                 space,
                                 (unused, present, start) -> {
                                     sets.add(new HashSet<>(present));
                                     starts.add(start);
                                     return null;
                                 },
                                 null);

            for (int i = 0; i < sets.size(); ++i)
            {
                Set<E> set = sets.get(i);
                boolean isLast = i + 1 == sets.size();
                P itemsEnd = isLast ? null : starts.get(i + 1);
                for (E item : set)
                    itemsEnd = itemsEnd == null ? space.endOf(item) : Comparables.min(itemsEnd, space.endOf(item));
                itemsEnds.add(itemsEnd);
                weights.add(weight(set, starts.get(i), itemsEnd));

                Set<E> spanning = Collections.emptySet();
                if (!isLast)
                {
                    spanning = new HashSet<>(set);
                    spanning.retainAll(sets.get(i + 1));
                }
                toSplitAfter.add(spanning);
            }
        }

        /// The weight of the given items between the given points, which must be within the span of all of them.
        private double weight(Set<E> items, P from, P to)
        {
            double sum = 0;
            for (E item : items)
                sum += space.weight(item, from, to);
            return sum;
        }

        /// Go through the points where groups can end in order, starting with the start of the first set and
        /// finishing at the end of the last.
        Split<P> evaluate()
        {
            Candidate current = new Candidate(null, starts.get(0), 0, weights.get(0), Collections.emptySet());
            while (true)
            {
                expand(current);
                current = frontier.pollFirstEntry().getValue();
                if (current.split.pointAfter == null)
                    return current.split;
            }
        }

        /// Offer the groups that start where the given candidate ends: the ones that end at each boundary that keeps
        /// them within the limit, and the one that ends inside the set that takes it over the limit, where it reaches
        /// the limit. If the space can't be split there and that set is the first one in the group, the group takes
        /// all of it, even though it is over the limit.
        private void expand(Candidate from)
        {
            Candidate pending = null;
            double weight = 0;
            for (int set = from.set; set < sets.size() && (set == from.set || weight < pickLimit); ++set)
            {
                P setStart = set == from.set ? from.point : starts.get(set);
                double endWeight = weight + (set == from.set ? from.remainingWeight : weights.get(set));
                Candidate next = endWeight <= pickLimit ? endAfter(set, endWeight, from)
                                                        : endInside(set, setStart, weight, endWeight, from);
                if (next == null && set == from.set)
                    next = endAfter(set, endWeight, from);

                // A group that ends later at no more cost is never worse, so only offer the earlier one if the later one
                // costs more. Groups that start at the same point have the same number of groups before them.
                if (pending != null && (next == null || next.cost() > pending.cost()))
                    offer(pending);
                pending = next;
                if (next == null || endWeight > pickLimit)
                    break;
                weight = endWeight;
            }
            if (pending != null)
                offer(pending);
        }

        /// A group that starts where `from` ends and ends at the end of the given set, with the given weight.
        private Candidate endAfter(int set, double weight, Candidate from)
        {
            boolean isLast = set + 1 == sets.size();
            Set<E> splitHere = toSplitAfter.get(set);
            Split<P> split = new Split<>(isLast ? null : starts.get(set + 1),
                                         weight,
                                         from.cost() + splitCost(splitHere, from.splitHere),
                                         from.split);
            return isLast ? new Candidate(split, itemsEnds.get(set), set + 1, 0, splitHere)
                          : new Candidate(split, starts.get(set + 1), set + 1, weights.get(set + 1), splitHere);
        }

        /// A group that starts where `from` ends, has the weight `weight` before the given set, which starts for it at
        /// `setStart`, and ends inside the set where the group reaches the limit, or null if the space can't be split
        /// there. `endWeight` is the weight the group would have with all of the set.
        ///
        /// The items of a set are present over all of it up to where they end, so its weight is spread uniformly up to
        /// there, and splitting inside it splits all of its items.
        @Nullable
        private Candidate endInside(int set, P setStart, double weight, double endWeight, Candidate from)
        {
            if (weight >= pickLimit)
                return null;

            Set<E> items = sets.get(set);
            P itemsEnd = itemsEnds.get(set);
            P point = space.split(setStart, itemsEnd, (pickLimit - weight) / (endWeight - weight));
            if (point == null || point.compareTo(setStart) <= 0 || point.compareTo(itemsEnd) >= 0)
                return null;
            double pieceWeight = weight(items, setStart, point);
            if (pieceWeight <= 0)
                return null;
            assert weight + pieceWeight <= pickLimit + 0.001;

            Split<P> split = new Split<>(point, weight + pieceWeight, from.cost() + splitCost(items, from.splitHere), from.split);
            return new Candidate(split, point, set, endWeight - weight - pieceWeight, items);
        }

        /// Add the candidate to the frontier, unless the frontier has one that is no worse at the same or a later
        /// point, and remove the ones at earlier points that are no better than it.
        private void offer(Candidate candidate)
        {
            var atOrAfter = frontier.ceilingEntry(candidate.point);
            if (atOrAfter != null && !candidate.split.isBetterThan(atOrAfter.getValue().split))
                return;

            frontier.put(candidate.point, candidate);
            var earlier = frontier.headMap(candidate.point, false).descendingMap().values().iterator();
            while (earlier.hasNext() && !earlier.next().split.isBetterThan(candidate.split))
                earlier.remove();
        }

        /// The cost of splitting the given items, except the ones in `alreadySplit`.
        private double splitCost(Set<E> items, Set<E> alreadySplit)
        {
            double cost = 0;
            for (E item : items)
            {
                if (!alreadySplit.contains(item))
                    cost += splitCoster.applyAsDouble(item);
            }
            return cost;
        }

        /// A way to split up to a point, with what is needed to continue from there.
        private class Candidate
        {
            /// The last group, or null for the start of the first set.
            final @Nullable Split<P> split;
            /// The point where the next group starts, which for the end of the last set is where its items end.
            final P point;
            /// The set the next group starts in.
            final int set;
            /// The weight of what is left of that set after the point.
            final double remainingWeight;
            /// The items split at the point, which are not charged again when split later.
            final Set<E> splitHere;

            Candidate(@Nullable Split<P> split, P point, int set, double remainingWeight, Set<E> splitHere)
            {
                this.split = split;
                this.point = point;
                this.set = set;
                this.remainingWeight = remainingWeight;
                this.splitHere = splitHere;
            }

            double cost()
            {
                return split != null ? split.accumulatedSplitCost : 0;
            }
        }
    }
}
