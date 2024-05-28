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

package org.apache.cassandra.db.tries;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import sun.misc.Unsafe;

/**
 * Like AtomicReferenceArray, but with an option to change the size (e.g. expand) of the underlying array.
 * Separate implementation to avoid second indirection when we need to pass a reference to array, and the expanding of
 * that array needs to be reflected in the state of the receiving object.
 * <p>
 * (For example, derivative MemtableReadTrie objects need to have up-to-date references to the content and metadata
 * arrays of the source MemtableTrie, because modification of the trie can cause the arrays to expand leaving them
 * with indexes of content/metadata they can't find the entries for.)
 * <p>
 * Only useful for single-writer scenarios, as any updates concurrent with {@link #resize(int)} may fail to be reflected
 * in the expanded array.
 */
public class ExpandableAtomicReferenceArray<E>
{
    private volatile Object[] array;
    private static VarHandle arrayAccess = MethodHandles.arrayElementVarHandle(Object[].class);

    public ExpandableAtomicReferenceArray(int length)
    {
        array = new Object[length];
    }

    public final int length()
    {
        return array.length;
    }

    public final E getVolatile(int i)
    {
        return (E) arrayAccess.getVolatile(array, i);
    }

    public final E getOrdered(int i)
    {
        return (E) arrayAccess.getAcquire(array, i);
    }

    public final E getPlain(int i)
    {
        return (E) arrayAccess.get(array, i);
    }

    public final void setVolatile(int i, E newValue)
    {
        arrayAccess.setVolatile(array, i, newValue);
    }

    public final void setOrdered(int i, E newValue)
    {
        arrayAccess.setRelease(array, i, newValue);
    }

    public final void setPlain(int i, E newValue)
    {
        arrayAccess.set(array, i, newValue);
    }

    public final E getAndSet(int i, E newValue)
    {
        return (E) arrayAccess.getAndSet(array, i, newValue);
    }

    public final boolean compareAndSet(int i, E oldValue, E newValue)
    {
        return arrayAccess.compareAndSet(array, i, oldValue, newValue);
    }

    public String toString()
    {
        return Arrays.toString(array);
    }

    /**
     * Expand/change the size of the array.
     * <p>
     * Note that writes performed concurrently with this operation may or may not be reflected in the expanded array.
     */
    void resize(int newSize)
    {
        if (newSize != array.length)
        {
            // volatile set, all writes done during copyOf become visible
            // to anyone that reads array
            array = Arrays.copyOf(array, newSize);
        }
    }
}
