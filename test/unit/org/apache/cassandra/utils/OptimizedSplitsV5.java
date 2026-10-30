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

import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.cassandra.utils.OptimizedSplits.ItemSpace;
import org.apache.cassandra.utils.OptimizedSplits.Split;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.ToDoubleFunction;

import javax.annotation.Nullable;

import com.carrotsearch.hppc.DoubleArrayList;

/// Splits a collection of items that span ranges of points into groups whose weight is within a limit, choosing the
/// split points to minimize the cost of splitting the items that span them.
///
/// The span of the items is first cut into sets, with a new set at every point where an item starts or ends, so that
/// the items of a set are present over all of it (see [#constructOverlapSets]). Splitting at the boundary between two
/// sets splits the items present on both sides of it, and the cost of splitting an item is charged once, however
/// many times it is split. Sets that are over the limit on their own are cut into pieces within it, which splits all
/// of their items, and the groups are then formed by choosing which boundaries to split at.
///
/// Nothing is lost by only splitting inside sets that are over the limit. A split inside a set splits all of its
/// items, and a split at either end of the set only some of them, so a group that ends inside a set that is within
/// the limit could instead end at the start of the set, followed by a group of the set alone. That splits no more
/// items, and the group after it starts further on, which is never worse. Inside a set over the limit, where the cuts
/// go does not matter either, as all of its items are split anyway.
public class OptimizedSplitsV5
{
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
    /// that need to be split. A group can only be over the limit if it is a single set, or the rest of one, that the
    /// space cannot split further.
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

        /// The weight of each set, with the sets that are over the limit cut into pieces.
        final DoubleArrayList weights = new DoubleArrayList();
        /// The point where each set ends, or null for the last one.
        final List<P> pointAfter = new ArrayList<>();
        /// The items split by splitting at the end of each set.
        final List<Set<E>> toSplitAfter = new ArrayList<>();

        OptimizedSplitter(Collection<E> items, ItemSpace<E, P> space, ToDoubleFunction<E> splitCoster, double pickLimit)
        {
            this.space = space;
            this.splitCoster = splitCoster;
            this.pickLimit = pickLimit;

            List<Set<E>> sets = new ArrayList<>();
            List<P> starts = new ArrayList<>();
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
                // All items of the set span it from its start to the end of the items, which is the start of the next
                // set, unless there is a gap before it.
                P start = starts.get(i);
                P itemsEnd = isLast ? null : starts.get(i + 1);
                for (E item : set)
                    itemsEnd = itemsEnd == null ? space.endOf(item) : Comparables.min(itemsEnd, space.endOf(item));

                // If the set is over the limit, cut pieces within the limit off its start, as long as the space can
                // be split. All of its items span the cuts.
                double weight = weight(set, start, itemsEnd);
                while (weight > pickLimit)
                {
                    P cut = space.split(start, itemsEnd, pickLimit / weight);
                    if (cut == null || cut.compareTo(start) <= 0 || cut.compareTo(itemsEnd) >= 0)
                        break;
                    double pieceWeight = weight(set, start, cut);
                    if (pieceWeight <= 0 || pieceWeight >= weight)
                        break;
                    assert pieceWeight <= pickLimit + 0.001;

                    addSet(pieceWeight, cut, set);
                    start = cut;
                    weight -= pieceWeight;
                }

                Set<E> spanning = Collections.emptySet();
                if (!isLast)
                {
                    spanning = new HashSet<>(set);
                    spanning.retainAll(sets.get(i + 1));
                }
                addSet(weight, isLast ? null : starts.get(i + 1), spanning);
            }
        }

        private void addSet(double weight, @Nullable P end, Set<E> splitAtEnd)
        {
            weights.add(weight);
            pointAfter.add(end);
            toSplitAfter.add(splitAtEnd);
        }

        /// The weight of the given items between the given points, which must be within the span of all of them.
        private double weight(Set<E> items, P from, P to)
        {
            double sum = 0;
            for (E item : items)
                sum += space.weight(item, from, to);
            return sum;
        }

        /// Choose where to split with dynamic programming over the boundaries between the sets, keeping the cheapest
        /// way found to split up to each of them.
        Split<P> evaluate()
        {
            int count = weights.size();
            // The cheapest way found to split the sets before each one, i.e. with a group that ends where it starts,
            // and at the end the cheapest way to split all of them.
            List<Split<P>> best = new ArrayList<>(Collections.nCopies(count + 1, null));
            NavigableMap<Integer, Double> frontier = new TreeMap<>();
            for (int first = 0; first < count; ++first)
            {
                Split<P> prev = best.get(first);
                if (first > 0 && frontier.remove(first) == null)
                    continue;   // no group ends here, or a later position costs no more
                double costBefore = prev != null ? prev.accumulatedSplitCost : 0;
                Set<E> alreadySplit = first > 0 ? toSplitAfter.get(first - 1) : Collections.emptySet();

                // Try the groups from this set to each later one that are within the limit. A single set may be over
                // the limit if it could not be cut into pieces.
                int pending = -1;
                double pendingWeight = 0;
                double pendingCost = 0;
                double weight = 0;
                for (int last = first; last < count; ++last)
                {
                    weight += weights.get(last);
                    if (last > first && weight > pickLimit)
                        break;
                    double cost = costBefore + splitCost(toSplitAfter.get(last), alreadySplit);
                    // Ending the group a set later at no more cost is never worse: the group after it has less to
                    // cover, and if it splits an item that was split here, the item spans the later end too, so it is
                    // not charged again either way.
                    if (pending >= 0 && cost > pendingCost)
                        offer(best, frontier, pending + 1, pendingWeight, pendingCost, prev);
                    pending = last;
                    pendingWeight = weight;
                    pendingCost = cost;
                }
                offer(best, frontier, pending + 1, pendingWeight, pendingCost, prev);
            }
            return best.get(count);
        }

        /// Record a group that ends where the set at `index` starts, if it is the cheapest way found to split up to
        /// there.
        private void offer(List<Split<P>> best, NavigableMap<Integer, Double> frontier, int index, double weight, double cost, @Nullable Split<P> prev)
        {
            var ceiling = frontier.ceilingEntry(index);
            if (ceiling != null && ceiling.getValue() <= cost)
                return;
            best.set(index, new Split<>(pointAfter.get(index - 1), weight, cost, prev));
            frontier.put(index, cost);
            var dominated = frontier.headMap(index, false).descendingMap().values().iterator();
            while (dominated.hasNext() && dominated.next() >= cost)
                dominated.remove();
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
    }
}
