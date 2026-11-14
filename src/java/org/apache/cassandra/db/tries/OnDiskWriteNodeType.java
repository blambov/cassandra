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

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.vint.VIntCoding;

/// The node encodings of the on-disk trie format, as used when writing. See [OnDiskTrie.md](./OnDiskTrie.md).
///
/// The methods here only encode: they are given the positions of a node's children and the place the node is written
/// at, and are shared by all [OnDiskNodeWriter] implementations. Deciding when a node is written and where its children
/// end up is the job of the node writer.
///
/// A node's children are passed as parallel arrays of positions (the end of each child's serialization) and transition
/// bytes, ordered by descending transition, i.e. in the order a reverse walk of the trie completes them. The last child
/// in the arrays is thus the first child of the node, the one the sparse, bitmap and chain types do not store a pointer
/// for: it, or a relay to it (see [#writeRelay]), must end exactly where the node begins.
enum OnDiskWriteNodeType
{
    // Note: Any changes to bits must be reflected in OnDiskReadNodeType.IMPLEMENTATIONS.

    LEAF(0b00000000),

    CHAIN(0b01000000)
    {
        @Override
        int sizeChildren(int count, int bytesPerPointer)
        {
            return 2;
        }

        @Override
        void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos, int bytesPerPointer) throws IOException
        {
            assert count == 1;
            out.writeByte(transitions[0]);
            out.writeByte(bits);    // length 1
        }
    },

    PREFIX(0b11110000),

    DENSE(0b11101000)
    {
        @Override
        int sizeChildren(int count, int bytesPerPointer)
        {
            // first pointer is not implicit here
            return 256 * bytesPerPointer + 1;
        }

        @Override
        void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos, int bytesPerPointer) throws IOException
        {
            // bytesPerPointer is sized for one more than the furthest delta, so that no real delta is all-ones, which
            // marks an absent child (see OnDiskReadNodeType.DENSE.notPresent).
            // first pointer is not implicit here
            int index = 0;
            for (int i = 255; i >= 0; --i)
            {
                if (index < count && i == (transitions[index] & 0xFF))
                {
                    long delta = basePos - positions[index++];
                    assert delta >= 0 && OnDiskBaseTrieWriter.bytesFor(delta + 1) <= bytesPerPointer;
                    OnDiskBaseTrieWriter.writeReversedSized(out, delta, bytesPerPointer);
                }
                else
                    OnDiskBaseTrieWriter.writeReversedSized(out, -1L, bytesPerPointer);
            }
            assert index == count;
            out.writeByte(bits | (bytesPerPointer - 1));
        }
    },

    BITMAP(0b11100000)
    {
        @Override
        int sizeChildren(int count, int bytesPerPointer)
        {
            return (count - 1) * bytesPerPointer + 32 + 1;
        }

        @Override
        void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos, int bytesPerPointer) throws IOException
        {
            writePointers(out, positions, count, basePos, bytesPerPointer);
            // Bit t of the bitmap is bit t % 64 of word t / 64, the layout of BitSet.toLongArray.
            long[] bitsAsLong = new long[4];
            for (int i = 0; i < count; ++i)
            {
                int transition = transitions[i] & 0xFF;
                bitsAsLong[Integer.divideUnsigned(transition, 64)] |= 1L << Integer.remainderUnsigned(transition, 64);
            }
            for (int i = 3; i >= 0; --i)
                out.writeLong(bitsAsLong[i]);   // lowest-order bytes ends up last
            out.writeByte(this.bits | (bytesPerPointer - 1));
        }
    },

    SPARSE(0b10000000)
    {
        @Override
        int sizeChildren(int count, int bytesPerPointer)
        {
            return (count - 1) * bytesPerPointer + count + 1;
        }

        @Override
        void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos, int bytesPerPointer) throws IOException
        {
            writePointers(out, positions, count, basePos, bytesPerPointer);
            for (int i = 0; i < count; ++i)
                out.writeByte(transitions[i]);
            out.writeByte(bits | ((count - 2) << SHIFT_SPARSE_LENGTH) | (bytesPerPointer - 1));
        }
    },

    /// An epsilon transition to a node placed elsewhere. Written immediately before a node whose first child could not
    /// be placed there, see [#writeRelay].
    RELAY(0b11111000);

    final int bits;

    static final int MAX_LEAF_LENGTH_INCLUSIVE = 63;
    static final int MAX_CHAIN_LENGTH_INCLUSIVE = 64;
    // sparse: 0x80 - 0xDF (96 positions) 1lllllbb, lllll is count - 2 and must be < 24, bb is bytes per pointer
    static final int MAX_SPARSE_LENGTH_INCLUSIVE = 25;
    static final int MAX_SPARSE_BYTES = 4;
    static final int SHIFT_SPARSE_LENGTH = 2;

    static final int PREFIX_HAS_CHILD = 0b00000001;
    static final int PREFIX_HAS_ASCENT_CONTENT = 0b00000010;
    static final int PREFIX_HAS_DESCENT_CONTENT = 0b00000100;

    OnDiskWriteNodeType(int bits)
    {
        this.bits = bits;
    }

    /// The size of the payload node [#writePayload] writes for content of the given serialized sizes, where -1 stands
    /// for no content on that side, or 0 if there is nothing to write.
    ///
    /// If the node has an alternate branch, the size does not include the bytes of the pointer to it, whose number
    /// depends on where the branch and the node are placed; the caller must add [#sizeAlternateBranchPointer].
    static int sizePayload(int descentDataSize, int ascentDataSize, boolean hasAlternateBranch, boolean hasChild)
    {
        assert ascentDataSize < 0 || !hasAlternateBranch;
        boolean hasAscentSlot = ascentDataSize >= 0 || hasAlternateBranch;
        if (descentDataSize < 0 && !hasAscentSlot)
            return 0;

        int size = 1;   // the node code
        if (ascentDataSize >= 0)
            size += ascentDataSize + VIntCoding.computeUnsignedVIntSize(ascentDataSize);
        else if (hasAlternateBranch)
            size += 1;  // the length of the pointer, which is at most 8
        if (descentDataSize >= 0)
        {
            size += descentDataSize;
            if (hasChild || hasAscentSlot || descentDataSize > MAX_LEAF_LENGTH_INCLUSIVE)
                size += VIntCoding.computeUnsignedVIntSize(descentDataSize);
        }
        return size;
    }

    /// The number of bytes [#writePayload] uses for the pointer to an alternate branch at the given distance from the
    /// start of the payload. A branch that ends where the payload starts needs none.
    static int sizeAlternateBranchPointer(long delta)
    {
        return delta == 0 ? 0 : OnDiskBaseTrieWriter.bytesFor(delta);
    }

    /// Write the payload of a node: its descent-side content and what goes in its ascent-side slot, which is either
    /// ascent-side content or the pointer to the node's alternate branch.
    ///
    /// The latter is given as `alternateBranch`, the position of the root of the branch, or -1 if there is none. It is
    /// stored like every other pointer in the format, as a delta subtracted from the position where the slot starts
    /// (which is where the payload starts, as the ascent-side slot comes first), in the smallest number of bytes that
    /// can hold it. A branch that ends just where the slot starts has the delta 0, which takes no bytes and leaves
    /// the slot empty. This can only be the case for a node without children: the children of a node (or a relay to
    /// them) lie between its branch and the node.
    static <T> void writePayload(DataOutputPlus out, OnDiskTrieWriter.DataSerializer<T> serializer, T descentData, T ascentData, long alternateBranch, boolean hasChild) throws IOException
    {
        assert ascentData == null || alternateBranch < 0;
        boolean hasAscentSlot = ascentData != null || alternateBranch >= 0;
        if (descentData == null && !hasAscentSlot)
            return;

        // A node is read backwards from its code byte, and the reader takes the content block
        // adjacent to the code as the descent side (see OnDiskReadNodeType.PREFIX and the layout in
        // OnDiskTrie.md), so the ascent block has to be emitted first.
        int code = PREFIX.bits;
        if (ascentData != null)
        {
            long ascentStart = out.position();
            serializer.serialize(out, ascentData);
            OnDiskBaseTrieWriter.writeReversedVint(out, out.position() - ascentStart);
            code |= PREFIX_HAS_ASCENT_CONTENT;
        }
        else if (alternateBranch >= 0)
        {
            long delta = out.position() - alternateBranch;
            assert delta >= 0 : "Alternate branch root " + alternateBranch + " is after its node at " + out.position();
            assert delta > 0 || !hasChild : "Alternate branch root " + alternateBranch + " is where the children of its node should be";
            int pointerBytes = sizeAlternateBranchPointer(delta);
            if (pointerBytes > 0)
                OnDiskBaseTrieWriter.writeReversedSized(out, delta, pointerBytes);
            OnDiskBaseTrieWriter.writeReversedVint(out, pointerBytes);
            code |= PREFIX_HAS_ASCENT_CONTENT;
        }

        long descentDataSize = -1;
        if (descentData != null)
        {
            long descentStart = out.position();
            serializer.serialize(out, descentData);
            descentDataSize = out.position() - descentStart;
        }

        if (hasChild || hasAscentSlot || descentDataSize > MAX_LEAF_LENGTH_INCLUSIVE)
        {
            if (descentDataSize >= 0)
            {
                OnDiskBaseTrieWriter.writeReversedVint(out, descentDataSize);
                code |= PREFIX_HAS_DESCENT_CONTENT;
            }
            if (hasChild)
                code |= PREFIX_HAS_CHILD;

            out.writeByte(code);
        }
        else
            out.writeByte(LEAF.bits | (int) descentDataSize);
    }

    /// Choose the node type for the given number of children. A bitmap node takes the 32-byte bitmap and a pointer for
    /// every child but the first, which is implicit; a dense node takes a pointer for each of the 256 transitions, sized
    /// by `denseBytesPerPointer`, which can be one byte wider than `bytesPerPointer` (see [#DENSE]). The bitmap is used
    /// when it saves at least one byte.
    ///
    /// `bytesPerPointer` is the size needed for the pointer to the furthest of the children a sparse or bitmap node
    /// stores a pointer for, i.e. all but the first, and `denseBytesPerPointer` the one for one more than the delta
    /// to the furthest of all the children.
    static OnDiskWriteNodeType selectChildrenType(int bytesPerPointer, int denseBytesPerPointer, int pointerCount)
    {
        if (pointerCount == 1)
            return CHAIN;
        else if (pointerCount <= MAX_SPARSE_LENGTH_INCLUSIVE && bytesPerPointer <= MAX_SPARSE_BYTES)
            return SPARSE;
        else if (32 + (pointerCount - 1) * bytesPerPointer < 256 * denseBytesPerPointer)
            return BITMAP;
        else
            return DENSE;
    }

    /// The size of the node [#writeChildren] writes for the given number of children and bytes per pointer.
    int sizeChildren(int count, int bytesPerPointer)
    {
        // Throw by default, only applies to CHAIN, SPARSE, BITMAP and DENSE
        throw new AssertionError();
    }

    /// Write a node of this type listing the given children. `basePos` is the position the node is written at, from
    /// which all pointers are taken, and `bytesPerPointer` the size [#selectChildrenType] was given for this type.
    ///
    /// All types but [#DENSE] leave the pointer to the last child in the arrays implicit: that child, or a relay to it
    /// (see [#writeRelay]), must end exactly at `basePos`.
    void writeChildren(DataOutputPlus out, long[] positions, byte[] transitions, int count, long basePos, int bytesPerPointer) throws IOException
    {
        // Throw by default, only applies to CHAIN, SPARSE, BITMAP and DENSE
        throw new AssertionError();
    }

    /// Write the explicit pointers of a sparse or bitmap node: every child but the last in the arrays, whose delta is 0
    /// and is left implicit.
    static void writePointers(DataOutputPlus out, long[] positions, int count, long basePos, int bytesPerPointer) throws IOException
    {
        for (int i = 0; i < count - 1; ++i)
        {
            long delta = basePos - positions[i];
            assert delta >= 0 && OnDiskBaseTrieWriter.bytesFor(delta) <= bytesPerPointer;
            OnDiskBaseTrieWriter.writeReversedSized(out, delta, bytesPerPointer);
        }
    }

    /// The size of the chain nodes [#writeChain] writes for the given number of transitions.
    static int sizeChain(int length)
    {
        return length + (length + MAX_CHAIN_LENGTH_INCLUSIVE - 1) / MAX_CHAIN_LENGTH_INCLUSIVE;
    }

    /// Write the transitions `bytes[start, end)` as chain nodes, deepest transition first, in pieces of at most
    /// [#MAX_CHAIN_LENGTH_INCLUSIVE] with the deepest piece the full one. The node the chain leads to must end at the
    /// current position.
    static void writeChain(DataOutputPlus out, byte[] bytes, int start, int end) throws IOException
    {
        int pos = end;
        while (pos > start)
        {
            int written = 0;
            while (pos > start && written < MAX_CHAIN_LENGTH_INCLUSIVE)
            {
                out.writeByte(bytes[--pos]);
                ++written;
            }
            out.writeByte(CHAIN.bits | (written - 1));
        }
    }

    /// The size of the relay [#writeRelay] writes for a target at the given distance.
    static int sizeRelay(long delta)
    {
        return OnDiskBaseTrieWriter.bytesFor(delta) + 1;
    }

    /// Write a relay to a node that ends `delta` bytes before the current position. A node written directly after the
    /// relay sees the relay's target as its first child.
    static void writeRelay(DataOutputPlus out, long delta) throws IOException
    {
        assert delta > 0;
        int bytesPerPointer = OnDiskBaseTrieWriter.bytesFor(delta);
        OnDiskBaseTrieWriter.writeReversedSized(out, delta, bytesPerPointer);
        out.writeByte(RELAY.bits | (bytesPerPointer - 1));
    }
}
