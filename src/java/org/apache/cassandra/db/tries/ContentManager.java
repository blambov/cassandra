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

public interface ContentManager<T>
{
    /// Get the content for the given content pointer.
    ///
    /// @param id content pointer, encoded as ~index where index is the position in the content array.
    /// @return the current content value.
    T getContent(int id);

    boolean shouldPresentAfterBranch(int contentId);

    /// This is called when content is left without children and is used to remove dangling metadata
    /// or markers for branches (e.g. rows) that have become empty.
    boolean shouldPreserveWithoutChildren(int contentId);

    /// Add a new content value.
    ///
    /// @param value The value to add.
    /// @param contentAfterBranch Whether the content should be understood to reside after the branch, i.e. it is to be
    ///                           returned on the ascent path of the cursor walk.
    /// @return A content id that can be used to reference the content, a negative number where
    ///         `id & CONTENT_INDEX_MASK` encodes the position of the value in the content array.
    int addContent(T value, boolean contentAfterBranch) throws TrieSpaceExhaustedException;

    /// Change the content associated with a given content id.
    ///
    /// @param id Encoded content id, where `id & CONTENT_INDEX_MASK` is the position in the content array.
    /// @param value New content value to store.
    /// @return The id to use for the modified content; an attempt will be made to make this the same as id, but not
    ///         all content managers will be able to freely modify the data for a given id.
    ///         Implementations must ensure that if the id changes, the previous id is released.
    int setContent(int id, T value) throws TrieSpaceExhaustedException;

    void releaseContent(int id);

    void completeMutation();
    void abortMutation();


    /// Make a textual representation of the id for debugging.
    String dumpContentId(int id);

    long usedSizeOnHeap();
    long usedSizeOffHeap();

    /// Returns the amount of memory that has been allocated for various buffers but isn't currently in use.
    /// The total on-heap space used by the trie is `usedSizeOnHeap() + unusedReservedOnHeapMemory()`.
    long unusedReservedOnHeapMemory();

    /// Release all recycled content references, including the ones waiting in still incomplete recycling lists.
    /// This is a test method and can cause null pointer exceptions if used on a live trie.
    ///
    /// If similar functionality is required for non-test purposes, a version of this should be developed that only
    /// releases references on barrier-complete lists.
    void releaseReferencesUnsafe();

    /// Returns the number of values in the trie
    int valuesCount();
}
