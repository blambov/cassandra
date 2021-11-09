package org.apache.cassandra.utils;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.nio.ByteBuffer;

import org.github.jamm.MemoryLayoutSpecification;
import org.github.jamm.MemoryMeter;

/**
 * A convenience class for wrapping access to MemoryMeter
 */
public class ObjectSizes
{
    private static final MemoryMeter meter = new MemoryMeter()
                                             .withGuessing(MemoryMeter.Guess.FALLBACK_UNSAFE)
                                             .ignoreKnownSingletons();
    private static final MemoryMeter omitSharedMeter = meter.omitSharedBufferOverhead();

    private static final long BUFFER_EMPTY_SIZE = measure(ByteBufferUtil.EMPTY_BYTE_BUFFER);
    private static final long DIRECT_BUFFER_EMPTY_SIZE = measure(ByteBuffer.allocateDirect(0));
    private static final long BYTE_ARRAY_EMPTY_SIZE = measure(new byte[0]);
    private static final long STRING_EMPTY_SIZE = measure("");

    /**
     * Memory a byte array consumes
     * @param bytes byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(byte[] bytes)
    {
        return sizeOfArray(bytes.length, 1);
    }

    /**
     * Memory a long array consumes
     * @param longs byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(long[] longs)
    {
        return sizeOfArray(longs.length, 8);
    }

    /**
     * Memory an int array consumes
     * @param ints byte array to get memory size
     * @return heap-size of the array
     */
    public static long sizeOfArray(int[] ints)
    {
        return sizeOfArray(ints.length, 4);
    }

    /**
     * Memory a reference array consumes
     * @param length the length of the reference array
     * @return heap-size of the array
     */
    public static long sizeOfReferenceArray(int length)
    {
        return sizeOfArray(length, MemoryLayoutSpecification.SPEC.getReferenceSize());
    }

    /**
     * Memory a reference array consumes itself only
     * @param objects the array to size
     * @return heap-size of the array (excluding memory retained by referenced objects)
     */
    public static long sizeOfArray(Object[] objects)
    {
        return sizeOfReferenceArray(objects.length);
    }

    private static long sizeOfArray(int length, long elementSize)
    {
        return MemoryLayoutSpecification.sizeOfArray(length, elementSize);
    }

    /**
     * Memory a ByteBuffer array consumes.
     */
    public static long sizeOnHeapOf(ByteBuffer[] array)
    {
        long allElementsSize = 0;
        for (int i = 0; i < array.length; i++)
            if (array[i] != null)
                allElementsSize += sizeOnHeapOf(array[i]);

        return allElementsSize + sizeOfArray(array);
    }

    public static long sizeOnHeapExcludingData(ByteBuffer[] array)
    {
        long sum = sizeOfArray(array);
        for (ByteBuffer b : array)
            sum += sizeOnHeapExcludingData(b);
        return sum;
    }

    /**
     * Memory a byte buffer consumes
     * @param buffer ByteBuffer to calculate in memory size
     * @return Total in-memory size of the byte buffer
     */
    public static long sizeOnHeapOf(ByteBuffer buffer)
    {
        if (buffer.isDirect())
            return DIRECT_BUFFER_EMPTY_SIZE;
        // if we're only referencing a sub-portion of the ByteBuffer, don't count the array overhead (assume it's slab
        // allocated, so amortized over all the allocations the overhead is negligible and better to undercount than over)
        int capacity = buffer.capacity();
        if (capacity > buffer.remaining())
            return BUFFER_EMPTY_SIZE + buffer.remaining();
        return BUFFER_EMPTY_SIZE + sizeOfArray(capacity, 1);
    }

    public static long sizeOfEmptyHeapByteBuffer()
    {
        return BUFFER_EMPTY_SIZE;
    }

    public static long sizeOfEmptyByteArray()
    {
        return BYTE_ARRAY_EMPTY_SIZE;
    }

    public static long sizeOnHeapExcludingData(ByteBuffer buffer)
    {
        if (buffer.isDirect())
            return DIRECT_BUFFER_EMPTY_SIZE;
        int capacity = buffer.capacity();
        // if we're only referencing a sub-portion of the ByteBuffer, don't count the array overhead (assume it's slab
        // allocated, so amortized over all the allocations the overhead is negligible and better to undercount than over)
        if (capacity > buffer.remaining())
            return BUFFER_EMPTY_SIZE;
        // If buffers are dedicated, account for byte array size and any padding overhead
        return BUFFER_EMPTY_SIZE + sizeOfArray(capacity, 1) - capacity;
    }

    /**
     * Memory a String consumes
     * @param str String to calculate memory size of
     * @return Total in-memory size of the String
     */
    //@TODO hard coding this to 2 isn't necessarily correct in Java 11
    public static long sizeOf(String str)
    {
        return STRING_EMPTY_SIZE + sizeOfArray(str.length(), 2);
    }

    /**
     * @param pojo the object to measure
     * @return The size on the heap of the instance and all retained heap referenced by it. Where memory is distributed
     * to heap byte buffers from a slab, this will include the size of the whole slab.
     */
    public static long measureDeep(Object pojo)
    {
        return meter.measureDeep(pojo);
    }

    /**
     * @param pojo the object to measure
     * @return The size on the heap of the instance and all retained heap referenced by it, excluding portions of
     * ByteBuffer that are not directly referenced by it but including any other referenced that may also be retained
     * by other objects. This also includes bytes referenced in direct byte buffers, and may double-count memory if
     * it is referenced by multiple ByteBuffer copies.
     */
    public static long measureDeepOmitShared(Object pojo)
    {
        return omitSharedMeter.measureDeep(pojo);
    }

    /**
     * @param pojo the object to measure
     * @return the size on the heap of the instance only, excluding any referenced objects
     */
    public static long measure(Object pojo)
    {
        return meter.measure(pojo);
    }
}
