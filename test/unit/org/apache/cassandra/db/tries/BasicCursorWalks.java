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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.agrona.DirectBuffer;
import org.apache.cassandra.utils.Hex;

/// Test utility that lists the positions of basic deletion-aware cursors, also for use outside the tries package.
public class BasicCursorWalks
{
    /// A position of a basic deletion-aware cursor with something to show, the path leading to it and what it has.
    public static class Step
    {
        public final byte[] path;
        public final boolean onReturnPath;
        public final boolean isDeletionBranchRoot;
        public final Object content;
        public final Object deletion;

        Step(byte[] path, boolean onReturnPath, boolean isDeletionBranchRoot, Object content, Object deletion)
        {
            this.path = path;
            this.onReturnPath = onReturnPath;
            this.isDeletionBranchRoot = isDeletionBranchRoot;
            this.content = content;
            this.deletion = deletion;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof Step))
                return false;
            Step step = (Step) o;
            return onReturnPath == step.onReturnPath &&
                   isDeletionBranchRoot == step.isDeletionBranchRoot &&
                   Arrays.equals(path, step.path) &&
                   Objects.equals(content, step.content) &&
                   Objects.equals(deletion, step.deletion);
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode(path);
        }

        @Override
        public String toString()
        {
            return Hex.bytesToHex(path) + (onReturnPath ? "↑" : "") + (isDeletionBranchRoot ? " BRANCH" : "") +
                   (content != null ? " " + content : "") + (deletion != null ? " DEL[" + deletion + "]" : "");
        }
    }

    /// Walks the given cursor to its end using `advance` and closes it. Returns the positions that have content, a
    /// deletion or mark a deletion branch root.
    public static List<Step> walk(BasicDeletionAwareCursor<?, ?> cursor)
    {
        return walk(cursor, false);
    }

    /// As [#walk(BasicDeletionAwareCursor)], using `advanceMultiple`.
    public static List<Step> walkMultiple(BasicDeletionAwareCursor<?, ?> cursor)
    {
        return walk(cursor, true);
    }

    private static List<Step> walk(BasicDeletionAwareCursor<?, ?> cursor, boolean multiple)
    {
        List<Step> steps = new ArrayList<>();
        PathTracker path = new PathTracker();
        try (cursor)
        {
            long position = cursor.encodedPosition();
            while (!Cursor.isExhausted(position))
            {
                path.moveTo(position);
                Object content = BasicCursor.content(cursor, position);
                Object deletion = BasicDeletionAwareCursor.deletion(cursor, position);
                boolean isDeletionBranchRoot = BasicDeletionAwareCursor.isDeletionBranchRoot(position);
                if (content != null || deletion != null || isDeletionBranchRoot)
                    steps.add(new Step(Arrays.copyOf(path.bytes, path.length),
                                       Cursor.isOnReturnPath(position),
                                       isDeletionBranchRoot,
                                       content,
                                       deletion));
                position = multiple ? cursor.advanceMultiple(path) : cursor.advance();
            }
        }
        return steps;
    }

    /// Tracks the path of the cursor from the positions it reports and the bytes `advanceMultiple` gives.
    private static class PathTracker implements BasicCursor.TransitionsReceiver
    {
        byte[] bytes = new byte[256];
        int length = 0;

        void moveTo(long position)
        {
            int depth = Cursor.depth(position);
            // The bytes given by advanceMultiple, if any, lead to the parent of the new position.
            assert length <= Math.max(depth - 1, 0) || length >= depth : "Unexpected path length " + length + " at " + Cursor.toString(position);
            length = Math.max(depth - 1, 0);
            if (depth > 0)
                addPathByte(Cursor.incomingTransition(position));
        }

        @Override
        public void addPathByte(int nextByte)
        {
            if (length == bytes.length)
                bytes = Arrays.copyOf(bytes, length * 2);
            bytes[length++] = (byte) nextByte;
        }

        @Override
        public void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            for (int i = 0; i < count; ++i)
                addPathByte(buffer.getByte(pos + i) & 0xFF);
        }
    }
}
