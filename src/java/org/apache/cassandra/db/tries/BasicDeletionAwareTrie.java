/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

/// A deletion-aware trie that can only be walked in order, i.e. a source of [BasicDeletionAwareCursor]s, which present
/// the live content and the deletions in one walk. See [BasicTrie].
///
/// @param <T> The content type for live data in the trie
/// @param <D> The deletion marker type
public interface BasicDeletionAwareTrie<T, D extends RangeState<D>>
{
    /// Returns a cursor positioned at the root of the trie that walks it in the given direction.
    BasicDeletionAwareCursor<T, D> basicCursor(Direction direction);
}
