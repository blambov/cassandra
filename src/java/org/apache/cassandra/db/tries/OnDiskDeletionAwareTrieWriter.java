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

import java.io.IOException;

import org.agrona.DirectBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;

/// Serializes a [DeletionAwareTrie] using the [OnDiskTrieWriter] on-disk trie format.
///
/// ## Where the deletion branch pointer lives
///
/// [OnDiskWriteNodeType] has no room for a deletion branch: the node type is given by
/// `nodeCode >> 3`, all 32 values of which are assigned (LEAF 0-7, CHAIN 8-15, SPARSE 16-27,
/// BITMAP 28, DENSE 29, PREFIX 30, RELAY 31), and PREFIX's three flag bits are all in
/// use (`HAS_CHILD`, `HAS_ASCENT_CONTENT`, `HAS_DESCENT_CONTENT`).
///
/// A node does, however, have a spare payload slot. A generic-content node carries content on both the
/// descent and the ascent (return) path, and the live trie of a deletion-aware trie only ever presents
/// content on descent (see the `presentContentOnDescentPath` argument
/// [InMemoryDeletionAwareTrie] passes to its base) — so nothing ever fills its ascent-side slot. The
/// deletion branch's root goes there, as a delta subtracted from the position where the slot starts, stored
/// as a minimal-width reversed integer whose length the node's own length prefix carries (see
/// [OnDiskWriteNodeType#writePayload]). That is how every
/// other pointer in the format is encoded, and for the same reasons: a branch written just before its node is
/// a byte or two away where an absolute position in a large trie takes four or five, and a subtree with only
/// backward deltas inside is self-contained, so it can be relocated, or copied unmodified in compaction, which
/// a single absolute pointer would prevent. A node without children can have its branch immediately before it; the
/// delta is then 0 and takes no bytes, leaving the slot empty. This is the same reuse the in-memory trie makes of
/// a prefix node's secondary pointer, which holds return-path content in a range trie and the alternate branch in
/// a deletion-aware one (see `PREFIX_ALTERNATE_OFFSET` and the prefix node section of `InMemoryTrie.md`).
///
/// A payload with no deletion branch is therefore byte-for-byte what a plain trie would write for the
/// same content, and the ascent-content bit of the node code is all the reader needs to flag the position
/// with [Cursor#MAY_HAVE_DELETION_BRANCH_BIT]: the pointer itself is only decoded when the branch is asked
/// for (see [OnDiskCursor#alternateBranch]).
///
/// ## Live content is descent-side only
///
/// It follows that the live data can only be presented on the descent path, before the node's children in both
/// directions, as every [InMemoryDeletionAwareTrie] presents it. A trie that presents live content on the return
/// path -- for example an ordered plain trie passed through [DeletionAwareTrie#wrap], whose content comes after its
/// branch when walked in reverse, as this writer walks -- has no slot to put that content in, and [#write] rejects it
/// with an [IllegalArgumentException] rather than write it where the reader expects a branch pointer.
///
/// ## Writers
///
/// The trie is taken as a [BasicDeletionAwareCursor], which presents the live content and the deletions in one walk
/// and marks the roots of the deletion branches. The live trie and the deletion branches are separate tries with
/// different types of content: this is the writer of the live trie, and holds an [OnDiskTrieWriter] for the deletion
/// branch in progress, which it reuses for each branch in turn (branches do not nest). While the walk is under a
/// deletion branch root, every step is given to both writers, with the positions that the branch writer sees made
/// relative to the root; each of the two only creates nodes for the content it is given.
///
/// The two are connected through their node writers. The node writer of the branches is
/// [nested][OnDiskNodeWriter#nested] in the one of the live trie, and a branch is attached to its node of the live
/// trie when the walk leaves the node ([OnDiskBaseTrieWriter#alternateBranch]). Where the branch and the descendants
/// of the node are placed is up to the node writer: [OnDiskNodeWriterPageAware] packs them in pages together with the
/// node as far as possible, and [OnDiskNodeWriterImmediate] writes the branch first, then the descendants (which it
/// has to hold back until the branch is complete) and the node. In both cases the branch is written before the node
/// that refers to it, and the pointer stored for its root is a non-negative backward delta like every other.
public class OnDiskDeletionAwareTrieWriter<T, D extends RangeState<D>> extends OnDiskBaseTrieWriter<T>
{
    final OnDiskTrieWriter.DataSerializer<D> deletionSerializer;

    /// The writer for each deletion branch in turn. Null until the first deletion branch, so that a trie without
    /// any does not allocate it.
    OnDiskTrieWriter<D> branchWriter;
    /// The position of the root of the deletion branch that the walk is in, and its depth, -1 if it is not in one.
    long branchRootPosition;
    int branchDepth = -1;
    /// The last position given to the branch writer.
    long branchPosition;

    /// Gives the transitions that the cursor takes inside a deletion branch to both writers.
    private final BasicCursor.TransitionsReceiver bothWriters = new BasicCursor.TransitionsReceiver()
    {
        @Override
        public void addPathByte(int nextByte)
        {
            OnDiskDeletionAwareTrieWriter.this.addPathByte(nextByte);
            branchWriter.addPathByte(nextByte);
        }

        @Override
        public void addPathBytes(DirectBuffer buffer, int pos, int count)
        {
            OnDiskDeletionAwareTrieWriter.this.addPathBytes(buffer, pos, count);
            branchWriter.addPathBytes(buffer, pos, count);
        }
    };

    /// Create a writer that gives the nodes of the live trie to the given node writer, and the ones of the deletion
    /// branches to a node writer nested in it.
    OnDiskDeletionAwareTrieWriter(OnDiskNodeWriter<T> nodeWriter,
                                  OnDiskTrieWriter.DataSerializer<D> deletionSerializer)
    {
        // As OnDiskDeletionAwareTrie reads it: live content in the descent-side slot, never swapped, and the branch
        // pointer in the ascent-side one.
        super(nodeWriter, OnDiskCursor.ContentStyle.ALTERNATE_BRANCH);
        this.deletionSerializer = deletionSerializer;
    }

    /// Serialize a deletion-aware trie, writing each node as soon as it is complete.
    ///
    /// @return the position of the root, which is the position of `out` when this returns, or 0 if the trie is empty,
    ///         as [OnDiskTrieWriter#write] returns it.
    public static <T, D extends RangeState<D>> long write(BasicDeletionAwareTrie<T, D> trie,
                                                          OnDiskTrieWriter.DataSerializer<T> contentSerializer,
                                                          OnDiskTrieWriter.DataSerializer<D> deletionSerializer,
                                                          DataOutputPlus out) throws IOException
    {
        return write(trie, contentSerializer, deletionSerializer, out, OnDiskNodeWriter.Type.IMMEDIATE);
    }

    /// Serialize a deletion-aware trie with the layout the given type of node writer produces.
    ///
    /// @return the position of the root, which is the position of `out` when this returns, or 0 if the trie is empty,
    ///         as [OnDiskTrieWriter#write] returns it.
    public static <T, D extends RangeState<D>> long write(BasicDeletionAwareTrie<T, D> trie,
                                                          OnDiskTrieWriter.DataSerializer<T> contentSerializer,
                                                          OnDiskTrieWriter.DataSerializer<D> deletionSerializer,
                                                          DataOutputPlus out,
                                                          OnDiskNodeWriter.Type nodeWriterType) throws IOException
    {
        return new OnDiskDeletionAwareTrieWriter<>(nodeWriterType.create(out, contentSerializer), deletionSerializer).writeFrom(trie);
    }

    /// Serialize the given trie. A writer can write several tries this way, one after the other.
    ///
    /// Writes in [Direction#REVERSE] like [OnDiskTrieWriter#write], so the root ends up last and the trie
    /// is read from the end of the stream.
    ///
    /// @return the position of the root, which is where the output is left, or 0 if the trie is empty.
    long writeFrom(BasicDeletionAwareTrie<T, D> trie) throws IOException
    {
        start();
        branchDepth = -1;
        try (BasicDeletionAwareCursor<T, D> c = trie.basicCursor(Direction.REVERSE))
        {
            long position = c.getPositionAndAssertFresh();
            do
            {
                emitAt(c, position);
                position = step(c, position);
            }
            while (!Cursor.isExhausted(position));
        }
        // An empty trie leaves the stream untouched and has no root.
        return rootPosition;
    }

    /// Report what the cursor has at its position: live content to this writer, and a deletion to the branch writer.
    private void emitAt(BasicDeletionAwareCursor<T, D> c, long position)
    {
        if (branchDepth < 0 && BasicDeletionAwareCursor.isDeletionBranchRoot(position))
            startBranch(position);

        T content = BasicCursor.content(c, position);
        if (content != null)
        {
            // The ascent-side slot is the branch pointer's (see the class documentation); live content on the return path
            // would be written into it and read back as a pointer.
            if (Cursor.isOnReturnPath(position))
                throw new IllegalArgumentException("Deletion-aware tries can only be serialized with live content on the descent " +
                                                   "path; content " + content + " is presented on the return path at " +
                                                   Cursor.toString(position));

            descentContent(content);
        }

        D deletion = BasicDeletionAwareCursor.deletion(c, position);
        if (deletion != null)
        {
            if (branchDepth < 0)
                throw new IllegalArgumentException("Deletion " + deletion + " is presented outside a deletion branch at " +
                                                   Cursor.toString(position));
            branchWriter.content(deletion, position);
        }
    }

    private void startBranch(long position)
    {
        // Always written as a range trie, independently of the live trie: a deletion branch is one, and it is read
        // back through OnDiskRangeCursor, which reads the RANGE content style.
        if (branchWriter == null)
            branchWriter = new OnDiskTrieWriter<>(nodeWriter.nested(deletionSerializer), OnDiskCursor.ContentStyle.RANGE);
        branchWriter.start();
        startAlternateBranch();
        branchRootPosition = position;
        branchDepth = Cursor.depth(position);
        branchPosition = Cursor.rootPosition(Cursor.direction(position));
    }

    @Override
    long step(BasicCursor<?> c, long prevPosition) throws IOException
    {
        if (branchDepth < 0)
            return super.step(c, prevPosition);

        long position = c.advanceMultiple(bothWriters);
        // The position as the branch sees it, which is exhausted if the walk has left the branch.
        branchPosition = branchWriter.moveTo(DepthAdjustedCursor.fromAdjustedDepth(position,
                                                                                   branchRootPosition,
                                                                                   Cursor.depthCorrectionValue(branchRootPosition)),
                                             branchPosition);
        if (Cursor.isExhausted(branchPosition))
        {
            // A branch can be entered and produce nothing, e.g. when an intersection leaves none of its content. It
            // writes nothing and has no root, and the node must not be given a branch then.
            if (branchWriter.hasRoot)
                alternateBranch(branchWriter, branchDepth);
            branchDepth = -1;
        }
        return moveTo(position, prevPosition);
    }
}
