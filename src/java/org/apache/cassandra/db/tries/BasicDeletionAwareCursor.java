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

import javax.annotation.Nullable;

/// A deletion-aware cursor that can only walk the trie in order. See [BasicCursor].
///
/// [DeletionAwareCursor] keeps the deletions in separate branches so that a consumer can skip to the closest live
/// content or the closest deletion to a given point. A basic cursor cannot skip, and has no need for the separation:
/// it presents live content and deletions in one walk, where a position can have live content ([#content]), a deletion
/// boundary ([#deletion]) or both. As in [BasicRangeCursor], only the boundaries of the deletion ranges are presented,
/// and positions on the return path can only have deletions.
///
/// Consumers that have to recover the shape of the full cursor, e.g. to apply the trie to an in-memory trie or write
/// it to disk, need to know where the deletion branches are rooted. These positions are marked with the
/// [Cursor#MAY_HAVE_DELETION_BRANCH_BIT] flag: all deletions are at or below a marked position, and no marked position
/// is below another.
///
/// The requirements of [DeletionAwareCursor] apply: every deletion range opened under a deletion branch root must be
/// closed under it, and the live content must not be deleted by the deletions of the same cursor.
///
/// @param <T> The content type for live data in the trie
/// @param <D> The deletion marker type
public interface BasicDeletionAwareCursor<T, D extends RangeState<D>> extends BasicCursor<T>
{
    /// Returns the deletion boundary at the current position, or null if there is none.
    ///
    /// It is preferable to call this via the static [#deletion(BasicDeletionAwareCursor, long)] to take advantage of
    /// the [Cursor#MAY_HAVE_DELETION_BIT] flag.
    @Nullable
    D deletion();

    /// Returns the deletion boundary at the cursor's current position, or null if the position does not have the
    /// [Cursor#MAY_HAVE_DELETION_BIT] flag set.
    static <T, D extends RangeState<D>> D deletion(BasicDeletionAwareCursor<T, D> cursor, long cursorPosition)
    {
        return (cursorPosition & Cursor.MAY_HAVE_DELETION_BIT) != 0 ? cursor.deletion() : null;
    }

    /// Returns true if the given position is marked as the root of a deletion branch.
    static boolean isDeletionBranchRoot(long encodedPosition)
    {
        return (encodedPosition & Cursor.MAY_HAVE_DELETION_BRANCH_BIT) != 0;
    }
}
