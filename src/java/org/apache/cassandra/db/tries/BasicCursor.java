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

import org.agrona.DirectBuffer;
import org.apache.cassandra.utils.Closeable;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/// A trie cursor that can only walk the trie in order.
///
/// This is the part of the [Cursor] interface that does not need skipping or taking tails: the position (encoded as
/// described in [Cursor]), the content, and the methods that advance to the next position. It is sufficient for
/// consumers that walk the whole trie, such as mutations of in-memory tries and writing tries to disk, and is much
/// easier to implement as a view of other data.
///
/// All trie cursors are basic cursors. The basic versions of range and deletion-aware cursors are
/// [BasicRangeCursor] and [BasicDeletionAwareCursor].
///
/// @param <T> The content type of the trie.
public interface BasicCursor<T> extends Closeable
{
    /// Returns the cursor's current position encoded as a long. This combines the depth and incoming transition as well
    /// as other flags that are part of the state. Use the static methods in [Cursor] to compare, manipulate, decode or
    /// encode cursor positions.
    long encodedPosition();

    /// Returns the content associated with the cursor's current node, or null if the position does not have the
    /// [Cursor#MAY_HAVE_CONTENT_BIT] flag set. This avoids redundant content lookups when the flag indicates no content.
    static <T> T content(BasicCursor<T> cursor, long cursorPosition)
    {
        return (cursorPosition & Cursor.MAY_HAVE_CONTENT_BIT) != 0 ? cursor.content() : null;
    }

    /// @return the content associated with the current node. This may be non-null for any presented node, including
    ///         the root.
    ///         It is preferable to retrieve this via the static method [#content(BasicCursor, long)] to take
    ///         advantage of the [Cursor#MAY_HAVE_CONTENT_BIT] flag and avoid megamorphic calls to this method unless
    ///         content is actually present.
    @Nullable
    T content();

    /// Returns the direction in which this cursor is progressing.
    default Direction direction()
    {
        return Cursor.direction(encodedPosition());
    }

    /// Returns the byte-comparable version that this trie uses.
    ByteComparable.Version byteComparableVersion();

    /// Advance one position to the node whose associated path is next lexicographically.
    /// This can be either:
    ///   - descending one level to the first child of the current node,
    ///   - ascending to the closest parent that has remaining children, and then descending one level to its next
    ///     child.
    ///
    /// It is an error to call this after the trie has already been exhausted (i.e. when `depth() == -1`);
    /// for performance reasons we won't always check this.
    ///
    /// @return encoded position after the advance (see [#encodedPosition()]).
    long advance();

    /// Advance, descending multiple levels if the cursor can do this for the current position without extra work
    /// (e.g. when positioned on a chain node in a memtable trie). If the current node does not have children this
    /// is exactly the same as advance(), otherwise it may take multiple steps down (but will not necessarily, even
    /// if they exist).
    ///
    /// Note that if any positions are skipped, they must have no content (of any kind, e.g. deletions for
    /// [BasicDeletionAwareCursor]) or feature flags, and cannot be on the return path.
    ///
    /// This is an optional optimization; the default implementation falls back to calling advance.
    ///
    /// It is an error to call this after the trie has already been exhausted (i.e. when `depth() == -1`);
    /// for performance reasons we won't always check this.
    ///
    /// @param receiver object that will receive all transitions taken except the last;
    ///                                 on ascend, or if only one step down was taken, it will not receive any
    /// @return encoded position after the advance (see [#encodedPosition()]).
    default long advanceMultiple(TransitionsReceiver receiver)
    {
        return advance();
    }

    /// Advance all the way to the next node with non-null content.
    ///
    /// It is an error to call this after the trie has already been exhausted (i.e. when `depth() == -1`);
    /// for performance reasons we won't always check this.
    ///
    /// @param receiver object that will receive all taken transitions
    /// @return the content, null if the trie is exhausted
    default T advanceToContent(ResettingTransitionsReceiver receiver)
    {
        return receiver != null ? advanceToContentWithReceiver(receiver) : advanceToContent();
    }

    private T advanceToContentWithReceiver(ResettingTransitionsReceiver receiver)
    {
        long prevPosition = encodedPosition();
        while (true)
        {
            long currPosition = advanceMultiple(receiver);
            if (Cursor.isExhausted(currPosition))
                return null;

            if (Cursor.ascended(currPosition, prevPosition))
            {
                int depth = Cursor.depth(currPosition);
                if (depth > 0)
                {
                    receiver.resetPathLength(depth - 1);
                    receiver.addPathByte(Cursor.incomingTransition(currPosition));
                }
                else
                {
                    receiver.resetPathLength(0);
                }
            }
            else
                receiver.addPathByte(Cursor.incomingTransition(currPosition));

            if (Cursor.isOnReturnPath(currPosition))
                receiver.onReturnPath();

            T content = content(this, currPosition);
            if (content != null)
                return content;
            prevPosition = currPosition;
        }
    }

    private T advanceToContent()
    {
        while (true)
        {
            long currPosition = advanceMultiple(null);
            if (Cursor.isExhausted(currPosition))
                return null;

            T content = content(this, currPosition);
            if (content != null)
                return content;
        }
    }

    /// Release whatever the cursor holds. A cursor over a trie in memory holds nothing; a cursor over a file holds a
    /// buffer for as long as it lives, and only gives it back here. Cursors that wrap others must pass this on to all
    /// of them. Closing twice must be harmless -- a cursor can be shared between two wrappers that each close it.
    ///
    /// After this call the cursor is no longer positioned anywhere; advancing it or asking for its content or
    /// position is an error. See [Cursor#close] for what remains possible with full cursors.
    @Override
    void close();

    /// Used by [#advanceMultiple] to feed the transitions taken.
    interface TransitionsReceiver
    {
        /// Add a single byte to the path.
        void addPathByte(int nextByte);
        /// Add the count bytes from position pos in the given buffer.
        void addPathBytes(DirectBuffer buffer, int pos, int count);

        /// Called when the current position is on the return path. Sets and ranges use these positions to return
        /// end-inclusive ranges.
        default void onReturnPath()
        {
            // nothing by default
        }
    }

    /// Used by [#advanceToContent] to track the transitions and backtracking taken.
    interface ResettingTransitionsReceiver extends TransitionsReceiver
    {
        /// Delete all bytes beyond the given length.
        void resetPathLength(int newLength);
    }

    /// A push interface for walking over a trie. Builds upon [TransitionsReceiver] to be given the bytes of the
    /// path, and adds methods called on encountering content and completion.
    /// See [TrieDumper] for an example of how this can be used, and [TrieEntriesWalker] as a base class
    /// for other common usages.
    interface Walker<T, R> extends ResettingTransitionsReceiver
    {
        /// Called when content is found.
        void content(T content);

        /// Called at the completion of the walk.
        R complete();
    }

    /// Process the trie using the given [Walker].
    /// This method must only be called on a freshly constructed cursor.
    default <R> R process(Walker<? super T, R> walker)
    {
        long currentPosition = getPositionAndAssertFresh();
        T content = content(this, currentPosition);
        if (content == null)
            content = advanceToContent(walker);

        while (content != null)
        {
            walker.content(content);
            content = advanceToContent(walker);
        }
        return walker.complete();
    }

    default void assertFresh()
    {
        getPositionAndAssertFresh();
    }

    default long getPositionAndAssertFresh()
    {
        long pos = encodedPosition();
        assert Cursor.depth(pos) == 0 : "The provided cursor has already been advanced.";
        return pos;
    }
}
