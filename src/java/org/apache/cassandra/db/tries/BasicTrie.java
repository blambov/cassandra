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

/// A trie that can only be walked in order, i.e. a source of [BasicCursor]s.
///
/// This is what operations that walk the whole of a trie accept, e.g. the mutation of in-memory tries. All [Trie]s are
/// basic tries; other data can implement this interface to be given to such operations without having to support all
/// the features of a trie.
///
/// @param <T> The content type of the trie.
public interface BasicTrie<T>
{
    /// Returns a cursor positioned at the root of the trie that walks it in the given direction.
    BasicCursor<T> basicCursor(Direction direction);
}
