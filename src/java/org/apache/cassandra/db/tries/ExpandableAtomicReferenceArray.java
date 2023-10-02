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

/**
 * Like AtomicReferenceArray, but with an option to change the size (e.g. expand) of the underlying array.
 * Separate implementation to avoid second indirection when we need to pass a reference to array, and the expanding of
 * that array needs to be reflected in the state of the receiving object.
 *
 * (For example, derivative MemtableReadTrie objects need to have up-to-date references to the content and metadata
 * arrays of the source MemtableTrie, because modification of the the trie can cause the arrays to expand leaving then
 * with indexes of content/metadata they can't find the entries for.)
 *
 * Only useful for single-writer scenarios, as any updates concurrent with {@link #expand(int)} may fail to be reflected
 * in the expanded array.
 */
public class ExpandableAtomicReferenceArray<E> {
    private static final VarHandle ARRAY_ACCESS = MethodHandles.arrayElementVarHandle(Object[].class);

    private volatile Object[] array;

    public ExpandableAtomicReferenceArray(int length) {
        array = new Object[length];
    }

    public final int length() {
        return array.length;
    }

    public final E get(int i) {
        return (E) ARRAY_ACCESS.getVolatile(array, i);
    }

    public final void set(int i, E newValue) {
        ARRAY_ACCESS.setVolatile(array, i, newValue);
    }

    public final E getAndSet(int i, E newValue) {
        return (E) ARRAY_ACCESS.getAndSet(array, i, newValue);
    }

    public final void lazySet(int i, E newValue) {
        ARRAY_ACCESS.setRelease(array, i, newValue);
    }

    public final boolean compareAndSet(int i, E oldValue, E newValue)
    {
        return ARRAY_ACCESS.compareAndSet(array, i, oldValue, newValue);
    }

    public String toString() {
        return Arrays.toString(array);
    }

    /**
     * Expand/change the size of the array.
     *
     * Note that writes performed concurrently with this operation may or may not be reflected in the expanded array.
     */
    public void expand(int newSize)
    {
        if (newSize != array.length)
            array = Arrays.copyOf(array, newSize);  // volatile set, all writes done during copyOf become visible
                                                    // to anyone that reads array
    }

}
