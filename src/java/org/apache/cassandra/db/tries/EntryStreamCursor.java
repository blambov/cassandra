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

import java.util.Arrays;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/// A [BasicDeletionAwareCursor] that presents a stream of entries, each of which places live content and/or a
/// deletion marker at a given path. This makes it easy to present other data structures as tries: a subclass only
/// needs to list the entries in the order of the walk, and this class takes care of presenting the trie positions,
/// including the intermediate nodes on the paths to the entries.
///
/// Subclasses implement [#nextEntry], which places the path of the next entry in `path[0..pathLength)` and sets the
/// entry's content, deletion, and flags. The entries must satisfy the following:
/// - They must be listed in the order of the walk in the cursor's direction, i.e. a node's descent-path entry must come
///   before the entries of its descendants, and its return-path entry after them; children are listed in the order of
///   their transition bytes in the walk's direction.
/// - Each entry must have some content, a deletion, or be marked as the deletion branch root (the latter only for
///   descent-path entries). Entries on the return path can only have a deletion.
/// - There can be at most one descent-path and one return-path entry for any path.
///
/// Subclasses must call [#start] at the end of their construction.
public abstract class EntryStreamCursor<T, D extends RangeState<D>> implements BasicDeletionAwareCursor<T, D>
{
    private static final int INITIAL_PATH_SIZE = 64;

    private final Direction direction;
    private final ByteComparable.Version byteComparableVersion;

    /// The path of the entry. Set by subclasses in [#nextEntry]; use [#ensurePathCapacity] or [#appendToPath] to make
    /// sure the array can hold the path.
    protected byte[] path = new byte[INITIAL_PATH_SIZE];
    /// The length of the entry's path.
    protected int pathLength;
    /// The length of a prefix of `path` that is known to be unchanged since the previous entry. Subclasses may set this
    /// in [#nextEntry] to avoid comparing the unchanged part of the path; it is reset to 0 after each entry.
    protected int unchangedPathPrefix;
    /// The live content of the entry, can be null.
    protected T entryContent;
    /// The deletion marker of the entry, can be null.
    protected D entryDeletion;
    /// Whether the entry is on the return path of its node, i.e. presented after its node's descendants.
    protected boolean entryOnReturnPath;
    /// Whether the entry's node is the root of the source's deletion branch.
    protected boolean entryIsDeletionBranchRoot;

    /// The path to the current position. Only the bytes up to `currentDepth` are valid.
    private byte[] currentPath = new byte[INITIAL_PATH_SIZE];
    private int currentDepth;
    private long currentPosition;
    /// The length of the common prefix of the current position's path and the next entry's path.
    private int matchedDepth;
    /// True if the current position is the position of the entry.
    private boolean atEntry;
    /// False if there are no more entries.
    private boolean hasEntry;

    protected EntryStreamCursor(Direction direction, ByteComparable.Version byteComparableVersion)
    {
        this.direction = direction;
        this.byteComparableVersion = byteComparableVersion;
    }

    /// Prepare the next entry. Implementations should set `path`, `pathLength` and the `entry...` fields, and return
    /// true, or return false if there are no more entries.
    protected abstract boolean nextEntry();

    /// Must be called by subclasses at the end of their construction to prepare the root position.
    protected void start()
    {
        currentDepth = 0;
        currentPosition = Cursor.rootPosition(direction);
        fetchEntry();
        if (hasEntry && pathLength == 0 && !entryOnReturnPath)
            setAtEntry(currentPosition);
    }

    /// Make sure `path` can hold at least the given number of bytes, preserving its current content.
    protected void ensurePathCapacity(int length)
    {
        if (length > path.length)
            path = Arrays.copyOf(path, Math.max(length, path.length * 2));
    }

    /// Append the given bytes to the path.
    protected void appendToPath(ByteSource bytes)
    {
        int next;
        while ((next = bytes.next()) != ByteSource.END_OF_STREAM)
        {
            if (pathLength == path.length)
                path = Arrays.copyOf(path, path.length * 2);
            path[pathLength++] = (byte) next;
        }
    }

    private void fetchEntry()
    {
        entryContent = null;
        entryDeletion = null;
        entryOnReturnPath = false;
        entryIsDeletionBranchRoot = false;
        hasEntry = nextEntry();
        int unchanged = unchangedPathPrefix;
        unchangedPathPrefix = 0;
        if (!hasEntry)
            return;

        if (currentPath.length < pathLength)
            currentPath = Arrays.copyOf(currentPath, Math.max(pathLength, currentPath.length * 2));
        // Find the common prefix of the current position and the new entry.
        int limit = Math.min(currentDepth, pathLength);
        int matched = Math.min(unchanged, limit);
        while (matched < limit && currentPath[matched] == path[matched])
            ++matched;
        matchedDepth = matched;
        assert entryOnReturnPath || matchedDepth < pathLength || pathLength == 0 && currentDepth == 0
            : "Entries must be presented in walk order.";
    }

    private long setAtEntry(long position)
    {
        atEntry = true;
        if (entryContent != null && !entryOnReturnPath)
            position |= Cursor.MAY_HAVE_CONTENT_BIT;
        if (entryDeletion != null)
            position |= Cursor.MAY_HAVE_DELETION_BIT;
        if (entryIsDeletionBranchRoot && !entryOnReturnPath)
            position |= Cursor.MAY_HAVE_DELETION_BRANCH_BIT;
        return currentPosition = position;
    }

    @Override
    public long advance()
    {
        if (atEntry)
        {
            atEntry = false;
            fetchEntry();
        }
        if (!hasEntry)
            return currentPosition = Cursor.exhaustedPosition(direction);

        if (matchedDepth < descentTarget())
        {
            // Descend one step towards the entry's node.
            int transition = path[matchedDepth] & 0xFF;
            currentPath[matchedDepth] = (byte) transition;
            currentDepth = ++matchedDepth;
            long position = Cursor.encode(currentDepth, transition, direction);
            if (currentDepth == pathLength && !entryOnReturnPath)
                return setAtEntry(position);
            return currentPosition = position;
        }

        // We are at the parent of the node of a return-path entry, or inside its branch. Move to its return path.
        if (pathLength > 0)
            currentPath[pathLength - 1] = path[pathLength - 1];
        currentDepth = matchedDepth = pathLength;
        long position = pathLength == 0
                        ? Cursor.rootReturnPosition(currentPosition)
                        : Cursor.encode(pathLength, path[pathLength - 1] & 0xFF, direction) | Cursor.ON_RETURN_PATH_BIT;
        return setAtEntry(position);
    }

    @Override
    public long advanceMultiple(TransitionsReceiver receiver)
    {
        if (atEntry)
        {
            atEntry = false;
            fetchEntry();
        }
        // We can only take multiple steps if we are descending from the current position.
        if (hasEntry && matchedDepth == currentDepth && matchedDepth + 1 < descentTarget())
        {
            int last = descentTarget() - 1;
            for (int i = matchedDepth; i < last; ++i)
            {
                currentPath[i] = path[i];
                if (receiver != null)
                    receiver.addPathByte(path[i] & 0xFF);
            }
            currentDepth = matchedDepth = last;
        }
        return advance();
    }

    /// The depth to which we need to descend to reach the entry. For return-path entries we only descend to the
    /// parent of the node and present the return-path position directly when the node's descent position has not
    /// been presented.
    private int descentTarget()
    {
        return entryOnReturnPath ? pathLength - 1 : pathLength;
    }

    @Override
    public long encodedPosition()
    {
        return currentPosition;
    }

    @Override
    public T content()
    {
        return atEntry && !entryOnReturnPath ? entryContent : null;
    }

    @Override
    public D deletion()
    {
        return atEntry ? entryDeletion : null;
    }

    @Override
    public Direction direction()
    {
        return direction;
    }

    @Override
    public ByteComparable.Version byteComparableVersion()
    {
        return byteComparableVersion;
    }

    /// Does nothing by default. Subclasses that hold resources must release them here.
    @Override
    public void close()
    {
    }
}
