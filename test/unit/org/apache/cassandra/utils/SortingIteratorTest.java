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

import org.junit.Test;
import static org.junit.Assert.*;

import java.lang.reflect.Array;
import java.util.*;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

import org.apache.cassandra.index.sai.utils.PriorityQueueIterator;

public class SortingIteratorTest {

    static <T> SortingIterator<T> makeIterator(Class<T> itemClass, Comparator<T> comparator, List<T> data)
    {
//        return new SortingIterator<T>(itemClass, comparator, data, x -> x);
        return new SortingIterator<T>(itemClass, comparator, data, x -> x);

//        return new LucenePriorityQueue.SortingIterator<>(comparator, data);

//        var pq = new PriorityQueue<>(data.size(), comparator);
//        for (T item : data)
//            if (item != null)
//                pq.add(item);
//        return new PriorityQueueIterator<>(pq);

//        var al = new ArrayList<T>(data.size());
//        for (T item : data)
//            if (item != null)
//                al.add(item);
//        al.sort(comparator);
//        return al.iterator();

//        T[] al = (T[]) Array.newInstance(itemClass, (data.size()));
//        int sz = 0;
//        for (T item : data)
//            if (item != null)
//                al[sz++] = item;
//        Arrays.sort(al, 0, sz, comparator);
//        return Iterators.forArray(al);

//        return data.stream().filter(Predicates.notNull()).sorted(comparator).iterator();
    }

    @Test
    public void testSortingIterator_withFixedData() {
        List<Integer> data = Arrays.asList(4, 1, 3, 2);
        Iterator<Integer> iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);

        List<Integer> sorted = new ArrayList<>();
        while (iterator.hasNext()) {
            sorted.add(iterator.next());
        }

        assertEquals(Arrays.asList(1, 2, 3, 4), sorted);
    }

    @Test
    public void testSortingIterator_withEmptyData() {
        List<Integer> data = Collections.emptyList();
        Iterator<Integer> iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testNextWithoutHasNext() {
        List<String> data = Arrays.asList("apple", "orange", "banana");
        Iterator<String> iterator = makeIterator(String.class, Comparator.naturalOrder(), data);

        assertEquals("apple", iterator.next());
        assertEquals("banana", iterator.next());
        assertEquals("orange", iterator.next());
        assertFalse(iterator.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void testNoSuchElementException() {
        List<Integer> data = Arrays.asList(1, 2, 3);
        Iterator<Integer> iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);

        iterator.next();
        iterator.next();
        iterator.next();
        iterator.next(); // Should throw NoSuchElementException
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedOperationException() {
        List<Integer> data = Arrays.asList(1, 2, 3);
        Iterator<Integer> iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);

        iterator.remove(); // Should throw UnsupportedOperationException
    }


    @Test
    public void testSkipTo_existingKey() {
        List<Integer> data = Arrays.asList(1, 3, 5, 7, 9);
        var iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);

        iterator.skipTo(5);

        assertTrue(iterator.hasNext());
        assertEquals((Integer) 5, iterator.next());
    }

    @Test
    public void testSkipTo_nonExistingKey() {
        List<Integer> data = Arrays.asList(1, 3, 5, 7, 9);
        var iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);

        iterator.skipTo(6);

        assertTrue(iterator.hasNext());
        assertEquals((Integer) 7, iterator.next());
    }

    @Test
    public void testSkipTo_beyondLastKey() {
        List<Integer> data = Arrays.asList(1, 3, 5, 7, 9);
        var iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);

        iterator.skipTo(10);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testSkipTo_firstKey() {
        List<Integer> data = Arrays.asList(1, 3, 5, 7, 9);
        var iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);

        iterator.skipTo(1);

        assertTrue(iterator.hasNext());
        assertEquals((Integer) 1, iterator.next());
    }

    @Test
    public void testSkipTo_beforeFirstKey() {
        List<Integer> data = Arrays.asList(1, 3, 5, 7, 9);
        var iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);

        iterator.skipTo(0);

        assertTrue(iterator.hasNext());
        assertEquals((Integer) 1, iterator.next());
    }

    @Test
    public void testSkipTo_onEmptyCollection() {
        List<Integer> data = Collections.emptyList();
        var iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);

        iterator.skipTo(5);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testRandomizedSkipTo() {
        Random random = new Random();
        int size = random.nextInt(100) + 50; // List size between 50 and 150
        List<Integer> data = new ArrayList<>();

        // Generate random data
        for (int i = 0; i < size; i++) {
            data.add(random.nextInt(200)); // Values between 0 and 199
        }

        // Sort the list to simulate typical usage
        Collections.sort(data);

        // Create the iterator
        var iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);

        // Track the current index
        int currentIndex = 0;

        // Perform random skipTo operations
        for (int i = 0; i < 10; i++) { // Perform 10 random skipTo operations
            int targetKey = random.nextInt(200); // Random target key between 0 and 199
            iterator.skipTo(targetKey);

            // Find the expected position, considering the current position
            int expectedIndex = currentIndex;
            for (int j = currentIndex; j < data.size(); j++) {
                if (data.get(j) >= targetKey) {
                    expectedIndex = j;
                    break;
                }
            }

            if (expectedIndex == data.size()) {
                // If no element is greater than or equal to targetKey, iterator should be exhausted
                assertFalse(iterator.hasNext());
            } else {
                // Otherwise, the next element should be the expected one
                assertTrue(iterator.hasNext());
                assertEquals(data.get(expectedIndex), iterator.next());
            }
            currentIndex = expectedIndex + 1;
        }
    }


    @Test
    public void testSortingIterator_withRandomData() {
        Random random = new Random();
        int size = 10000;
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            data.add(random.nextInt(size));
        }

        Iterator<Integer> iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);
        List<Integer> sorted = new ArrayList<>();
        while (iterator.hasNext()) {
            sorted.add(iterator.next());
        }

        List<Integer> expected = new ArrayList<>(data);
        Collections.sort(expected);

        assertEquals(expected, sorted);
    }

    @Test
    public void testSortingIterator_skipToRandomData() {
        Random random = new Random();
        int size = 10000;
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            data.add(random.nextInt(size));
        }

        List<Integer> sorted = new ArrayList<>(data);
        Collections.sort(sorted);
        List<Integer> expected = new ArrayList<>();

        var iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);
        List<Integer> iterated = new ArrayList<>();

        int skipDistanceMax = 200;
        for (int i = 0; i < size; i += random.nextInt(skipDistanceMax))
        {
            int targetMax = sorted.get(i);
            int targetMin = i > 0 ? sorted.get(i - 1) : 0;
            if (targetMin == targetMax)
                continue;
            int target = random.nextInt(targetMax - targetMin) + targetMin + 1; // (targetMin + 1; targetMax]
            iterator.skipTo(target);
            for (int c = random.nextInt(5); c >= 0; --c)
            {
                if (i >= size)
                {
                    assert !iterator.hasNext();
                    break;
                }
                iterated.add(iterator.next());
                expected.add(sorted.get(i++));
            }
        }

        assertEquals(expected, iterated);
    }

    @Test
    public void testSortingIterator_randomWithNulls() {
        Random random = new Random();
        int size = 10000;
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            data.add(random.nextInt(10) != 0 ? random.nextInt(size) : null);
        }

        Iterator<Integer> iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);
        List<Integer> sorted = new ArrayList<>();
        while (iterator.hasNext()) {
            sorted.add(iterator.next());
        }

        List<Integer> expected = new ArrayList<>(data);
        expected.removeIf(Predicates.isNull());
        Collections.sort(expected);

        assertEquals(expected, sorted);
    }

    @Test
    public void testSortingIterator_skipToRandomDataWithNulls() {
        Random random = new Random();
        int size = 10000;
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            data.add(random.nextInt(10) != 0 ? random.nextInt(size) : null);
        }

        List<Integer> sorted = new ArrayList<>(data);
        sorted.removeIf(Predicates.isNull());
        Collections.sort(sorted);
        size = sorted.size();
        List<Integer> expected = new ArrayList<>();

        var iterator = makeIterator(Integer.class, Comparator.naturalOrder(), data);
        List<Integer> iterated = new ArrayList<>();

        int skipDistanceMax = 200;
        for (int i = 0; i < size; i += random.nextInt(skipDistanceMax))
        {
            int targetMax = sorted.get(i);
            int targetMin = i > 0 ? sorted.get(i - 1) : 0;
            if (targetMin == targetMax)
                continue;
            int target = random.nextInt(targetMax - targetMin) + targetMin + 1; // (targetMin + 1; targetMax]
            iterator.skipTo(target);
            for (int c = random.nextInt(5); c >= 0; --c)
            {
                if (i >= size)
                {
                    assert !iterator.hasNext();
                    break;
                }
                iterated.add(iterator.next());
                expected.add(sorted.get(i++));
            }
        }

        assertEquals(expected, iterated);
    }
}
