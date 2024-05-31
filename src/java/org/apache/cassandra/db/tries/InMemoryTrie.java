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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.github.jamm.MemoryMeterStrategy;

import static org.apache.cassandra.db.tries.CursorWalkable.BYTE_COMPARABLE_VERSION;

/**
 * In-memory trie built for fast modification and reads executing concurrently with writes from a single mutator thread.
 * <p>
 * The main method for performing writes is {@link #apply(Trie, UpsertTransformer, Predicate)} which takes a trie as
 * an argument and merges it into the current trie using the methods supplied by the given {@link UpsertTransformer},
 * force copying anything below the points where the third argument returns true.
 * </p><p>
 * The predicate can be used to implement several forms of atomicity and consistency guarantees:
 * <list>
 * <li> if the predicate is {@code nf -> false}, neither atomicity nor sequential consistency is guaranteed - readers
 *      can see any mixture of old and modified content
 * <li> if the predicate is {@code nf -> true}, full sequential consistency will be provided, i.e. if a reader sees any
 *      part of a modification, it will see all of it, and all the results of all previous modifications
 * <li> if the predicate is {@code nf -> nf.isBranching()} the write will be atomic, i.e. either none or all of the
 *      content of the merged trie will be visible by concurrent readers, but not sequentially consistent, i.e. there
 *      may be writes that are not visible to a reader even when they precede writes that are visible.
 * <li> if the predicate is {@code nf -> <some_test>(nf.content())} the write will be consistent below the identified
 *      point (used e.g. by Memtable to ensure partition-level consistency)
 * </list>
 * </p><p>
 * Additionally, the class provides several simpler write methods for efficiency and convenience:
 * <list>
 * <li> {@link #putRecursive(ByteComparable, Object, UpsertTransformer)} inserts a single value using a recursive walk.
 *      It cannot provide consistency (single-path writes are always atomic). This is more efficient as it stores the
 *      walk state in the stack rather than on the heap but can cause a {@code StackOverflowException}.
 * <li> {@link #putSingleton(ByteComparable, Object, UpsertTransformer)} is a non-recursive version of the above, using
 *      the {@code apply} machinery.
 * <li> {@link #putSingleton(ByteComparable, Object, UpsertTransformer, boolean)} uses the fourth argument to choose
 *      between the two methods above, where some external property can be used to decide if the keys are short enough
 *      to permit recursive execution.
 * </list>
 * </p><p>
 * Because it uses 32-bit pointers in byte buffers, this trie has a fixed size limit of 2GB.
 */
class InMemoryTrie<T> extends InMemoryReadTrie<T>
{
    // See the trie format description in InMemoryReadTrie.

    /**
     * Trie size limit. This is not enforced, but users must check from time to time that it is not exceeded (using
     * {@link #reachedAllocatedSizeThreshold()}) and start switching to a new trie if it is.
     * This must be done to avoid tries growing beyond their hard 2GB size limit (due to the 32-bit pointers).
     */
    @VisibleForTesting
    static final int ALLOCATED_SIZE_THRESHOLD;
    static
    {
        // Default threshold + 10% == 2 GB. This should give the owner enough time to react to the
        // {@link #reachedAllocatedSizeThreshold()} signal and switch this trie out before it fills up.
        int limitInMB = CassandraRelevantProperties.MEMTABLE_OVERHEAD_SIZE.getInt(2048 * 10 / 11);
        if (limitInMB < 1 || limitInMB > 2047)
            throw new AssertionError(CassandraRelevantProperties.MEMTABLE_OVERHEAD_SIZE.getKey() +
                                     " must be within 1 and 2047");
        ALLOCATED_SIZE_THRESHOLD = 1024 * 1024 * limitInMB;
    }

    private int allocatedPos = 0;
    private int contentCount = 0;

    private final BufferType bufferType;    // on or off heap

    // constants for space calculations
    private static final long EMPTY_SIZE_ON_HEAP;
    private static final long EMPTY_SIZE_OFF_HEAP;
    private static final long REFERENCE_ARRAY_ON_HEAP_SIZE = ObjectSizes.measureDeep(new AtomicReferenceArray<>(0));

    static
    {
        InMemoryTrie<Object> empty = new InMemoryTrie<>(BufferType.ON_HEAP);
        EMPTY_SIZE_ON_HEAP = ObjectSizes.measureDeep(empty);
        empty = new InMemoryTrie<>(BufferType.OFF_HEAP);
        EMPTY_SIZE_OFF_HEAP = ObjectSizes.measureDeep(empty);
    }

    public InMemoryTrie(BufferType bufferType)
    {
        super(new UnsafeBuffer[31 - BUF_START_SHIFT],  // last one is 1G for a total of ~2G bytes
              new AtomicReferenceArray[29 - CONTENTS_START_SHIFT],  // takes at least 4 bytes to write pointer to one content -> 4 times smaller than buffers
              NONE);
        this.bufferType = bufferType;
    }

    // Buffer, content list and block management

    /**
     * Because we use buffers and 32-bit pointers, the trie cannot grow over 2GB of size. This exception is thrown if
     * a trie operation needs it to grow over that limit.
     * <p>
     * To avoid this problem, users should query {@link #reachedAllocatedSizeThreshold} from time to time. If the call
     * returns true, they should switch to a new trie (e.g. by flushing a memtable) as soon as possible. The threshold
     * is configurable, and is set by default to 10% under the 2GB limit to give ample time for the switch to happen.
     */
    public static class SpaceExhaustedException extends Exception
    {
        public SpaceExhaustedException()
        {
            super("The hard 2GB limit on trie size has been exceeded");
        }
    }

    final void putInt(int pos, int value)
    {
        getChunk(pos).putInt(inChunkPointer(pos), value);
    }

    final void putIntVolatile(int pos, int value)
    {
        getChunk(pos).putIntVolatile(inChunkPointer(pos), value);
    }

    final void putShort(int pos, short value)
    {
        getChunk(pos).putShort(inChunkPointer(pos), value);
    }

    final void putShortVolatile(int pos, short value)
    {
        getChunk(pos).putShort(inChunkPointer(pos), value);
    }

    final void putByte(int pos, byte value)
    {
        getChunk(pos).putByte(inChunkPointer(pos), value);
    }


    private int allocateBlock() throws SpaceExhaustedException
    {
        // Note: If this method is modified, please run InMemoryTrieTest.testOver1GSize to verify it acts correctly
        // close to the 2G limit.
        int v = allocatedPos;
        if (inChunkPointer(v) == 0)
        {
            int leadBit = getChunkIdx(v, BUF_START_SHIFT, BUF_START_SIZE);
            if (leadBit + BUF_START_SHIFT == 31)
                throw new SpaceExhaustedException();

            ByteBuffer newBuffer = bufferType.allocate(BUF_START_SIZE << leadBit);
            buffers[leadBit] = new UnsafeBuffer(newBuffer);
            // Note: Since we are not moving existing data to a new buffer, we are okay with no happens-before enforcing
            // writes. Any reader that sees a pointer in the new buffer may only do so after reading the volatile write
            // that attached the new path.
        }

        allocatedPos += BLOCK_SIZE;
        return v;
    }

    private int allocateCleanNode() throws SpaceExhaustedException
    {
        int pos = allocateBlock();
        // bytebuffers start zeroed out
        assert NONE == 0;
        return pos;
    }

    /**
     * Add a new content value.
     *
     * @return A content pointer that can be used to reference the content, encoded as ~index where index is the
     *         position of the value in the content array.
     */
    int addContent(T value)
    {
        int index = contentCount++;
        int leadBit = getChunkIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inChunkPointer(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        if (array == null)
        {
            assert ofs == 0 : "Error in content arrays configuration.";
            contentArrays[leadBit] = array = new AtomicReferenceArray<>(CONTENTS_START_SIZE << leadBit);
        }
        array.lazySet(ofs, value); // no need for a volatile set here; at this point the item is not referenced
                                   // by any node in the trie, and a volatile set will be made to reference it.
        return ~index; // encode content index as a negative number, so that it is directly useable as a pointer
    }

    /**
     * Change the content associated with a given content pointer.
     *
     * @param id content pointer, encoded as ~index where index is the position in the content array
     * @param value new content value to store
     */
    void setContent(int id, T value)
    {
        int leadBit = getChunkIdx(~id, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inChunkPointer(~id, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> array = contentArrays[leadBit];
        array.set(ofs, value);
    }

    public void discardBuffers()
    {
        if (bufferType == BufferType.ON_HEAP)
            return; // no cleaning needed

        for (UnsafeBuffer b : buffers)
        {
            if (b != null)
                FileUtils.clean(b.byteBuffer());
        }
    }

    // Write methods

    // Write visibility model: writes are not volatile, with the exception of the final write before a call returns
    // the same value that was present before (e.g. content was updated in-place / existing node got a new child or had
    // a child pointer updated); if the whole path including the root node changed, the root itself gets a volatile
    // write.
    // This final write is the point where any new cells created during the write become visible for readers for the
    // first time, and such readers must pass through reading that pointer, which forces a happens-before relationship
    // that extends to all values written by this thread before it.

    /**
     * Attach a child to the given non-content node. This may be an update for an existing branch, or a new child for
     * the node. An update _is_ required (i.e. this is only called when the newChild pointer is not the same as the
     * existing value).
     * This method is called when the original node content must be preserved for concurrent readers (i.e. any block to
     * be modified needs to be copied first.)
     *
     * @param node pointer to the node to update or copy
     * @param originalNode pointer to the node as it was before any updates in the current modification (i.e. apply
     *                     call) were started. In other words, the node that is currently reachable by readers if they
     *                     follow the same key, and which will become unreachable for new readers after this update
     *                     completes. Used to avoid copying again if already done -- if node is already != originalNode
     *                     (which is the case when a second or further child of a node is changed by an update),
     *                     then node is currently not reachable and can be safely modified or completely overwritten.
     * @param trans transition to modify/add
     * @param newChild new child pointer
     * @return pointer to the updated node
     */
    private int attachChildCopying(int node, int originalNode, int trans, int newChild) throws SpaceExhaustedException
    {
        if (node < 0)
            throw new AssertionError("Content in intermediate nodes is not supported.");

        switch (offset(node))
        {
            case SPARSE_OFFSET:
                // If the node is already copied (e.g. this is not the first child being modified), there's no need to copy
                // it again.
                return attachChildToSparse(node, trans, newChild, node == originalNode);
            case SPLIT_OFFSET:
                // This call will copy the split node itself and any intermediate blocks as necessary to make sure blocks
                // reachable from the original node are not modified.
                return attachChildToSplitCopying(node, originalNode, trans, newChild);
            default:
                // multi nodes
                return attachChildToChain(node, trans, newChild); // always copies
        }
    }

    /**
     * Attach a child to the given node. This may be an update for an existing branch, or a new child for the node.
     * An update _is_ required (i.e. this is only called when the newChild pointer is not the same as the existing value).
     *
     * @param node pointer to the node to update or copy
     * @param trans transition to modify/add
     * @param newChild new child pointer
     * @return pointer to the updated node; same as node if update was in-place
     */
    private int attachChild(int node, int trans, int newChild) throws SpaceExhaustedException
    {
        assert !isLeaf(node) : "attachChild cannot be used on content nodes.";

        switch (offset(node))
        {
            case PREFIX_OFFSET:
                assert false : "attachChild cannot be used on content nodes.";
            case SPARSE_OFFSET:
                return attachChildToSparse(node, trans, newChild, false);
            case SPLIT_OFFSET:
                return attachChildToSplit(node, trans, newChild);
            case LAST_POINTER_OFFSET - 1:
                // If this is the last character in a Chain block, we can modify the child in-place
                if (trans == getUnsignedByte(node))
                {
                    putIntVolatile(node + 1, newChild);
                    return node;
                }
                // else pass through
            default:
                return attachChildToChain(node, trans, newChild);
        }
    }

    /**
     * Attach a child to the given split node. This may be an update for an existing branch, or a new child for the node.
     */
    private int attachChildToSplit(int node, int trans, int newChild) throws SpaceExhaustedException
    {
        int midPos = splitBlockPointerAddress(node, splitNodeMidIndex(trans), SPLIT_START_LEVEL_LIMIT);
        int mid = getInt(midPos);
        if (isNull(mid))
        {
            mid = createEmptySplitNode();
            int tailPos = splitBlockPointerAddress(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
            int tail = createEmptySplitNode();
            int childPos = splitBlockPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
            putInt(childPos, newChild);
            putInt(tailPos, tail);
            putIntVolatile(midPos, mid);
            return node;
        }

        int tailPos = splitBlockPointerAddress(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        int tail = getInt(tailPos);
        if (isNull(tail))
        {
            tail = createEmptySplitNode();
            int childPos = splitBlockPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
            putInt(childPos, newChild);
            putIntVolatile(tailPos, tail);
            return node;
        }

        int childPos = splitBlockPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        putIntVolatile(childPos, newChild);
        return node;
    }

    /**
     * Attach a child to the given split node, copying all modified content to enable atomic visibility
     * of modification.
     * This may be an update for an existing branch, or a new child for the node.
     */
    private int attachChildToSplitCopying(int node, int originalNode, int trans, int newChild) throws SpaceExhaustedException
    {
        if (offset(originalNode) != SPLIT_OFFSET)
            originalNode = NONE;

        if (node == originalNode)
            node = copyBlock(node);

        int midPos = splitBlockPointerAddress(0, splitNodeMidIndex(trans), SPLIT_START_LEVEL_LIMIT);
        int mid = getInt(midPos + node);
        int midOriginal = originalNode != NONE ? getInt(midPos + originalNode) : NONE;
        if (mid == NONE)
        {
            mid = createEmptySplitNode();
            putInt(node + midPos, mid);  // volatile writes not needed because this branch is not attached yet
        }
        else if (mid == midOriginal)
        {
            mid = copyBlock(mid);
            putInt(node + midPos, mid);
        }

        int tailPos = splitBlockPointerAddress(0, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        int tail = getInt(tailPos + mid);
        int tailOriginal = midOriginal != NONE ? getInt(tailPos + midOriginal) : NONE;
        if (tail == NONE)
        {
            tail = createEmptySplitNode();
            putInt(mid + tailPos, tail);
        }
        else if (tailOriginal == tail)
        {
            tail = copyBlock(tail);
            putInt(mid + tailPos, tail);
        }

        int childPos = splitBlockPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        putIntVolatile(childPos, newChild);
        return node;
    }

    /**
     * Attach a child to the given sparse node. This may be an update for an existing branch, or a new child for the node.
     */
    private int attachChildToSparse(int node, int trans, int newChild, boolean forcedCopy) throws SpaceExhaustedException
    {
        int index;
        int smallerCount = 0;
        // first check if this is an update and modify in-place if so
        for (index = 0; index < SPARSE_CHILD_COUNT; ++index)
        {
            if (isNull(getInt(node + SPARSE_CHILDREN_OFFSET + index * 4)))
                break;
            final int existing = getUnsignedByte(node + SPARSE_BYTES_OFFSET + index);
            if (existing == trans)
            {
                if (forcedCopy)
                    node = copyBlock(node);

                putIntVolatile(node + SPARSE_CHILDREN_OFFSET + index * 4, newChild);
                return node;
            }
            else if (existing < trans)
                ++smallerCount;
        }
        int childCount = index;

        if (childCount == SPARSE_CHILD_COUNT)
        {
            // Node is full. Switch to split
            int split = createEmptySplitNode();
            for (int i = 0; i < SPARSE_CHILD_COUNT; ++i)
            {
                int t = getUnsignedByte(node + SPARSE_BYTES_OFFSET + i);
                int p = getInt(node + SPARSE_CHILDREN_OFFSET + i * 4);
                attachChildToSplitNonVolatile(split, t, p);
            }
            attachChildToSplitNonVolatile(split, trans, newChild);
            return split;
        }

        if (forcedCopy)
            node = copyBlock(node);

        // Add a new transition. They are not kept in order, so append it at the first free position.
        putByte(node + SPARSE_BYTES_OFFSET + childCount, (byte) trans);

        // Update order word.
        int order = getUnsignedShort(node + SPARSE_ORDER_OFFSET);
        int newOrder = insertInOrderWord(order, childCount, smallerCount);

        // Sparse nodes have two access modes: via the order word, when listing transitions, or directly to characters
        // and addresses.
        // To support the former, we volatile write to the order word last, and everything is correctly set up.
        // The latter does not touch the order word. To support that too, we volatile write the address, as the reader
        // can't determine if the position is in use based on the character byte alone (00 is also a valid transition).
        // Note that this means that reader must check the transition byte AFTER the address, to ensure they get the
        // correct value (see getSparseChild).

        // setting child enables reads to start seeing the new branch
        putIntVolatile(node + SPARSE_CHILDREN_OFFSET + childCount * 4, newChild);

        // some readers will decide whether to check the pointer based on the order word
        // write that volatile to make sure they see the new change too
        putShortVolatile(node + SPARSE_ORDER_OFFSET,  (short) newOrder);
        return node;
    }

    private int copyBlock(int block) throws SpaceExhaustedException
    {
        int copy = allocateBlock();
        getChunk(copy).putBytes(inChunkPointer(copy), getChunk(block), inChunkPointer(block & -BLOCK_SIZE), BLOCK_SIZE);
        return copy | offset(block);
    }

    /**
     * Insert the given newIndex in the base-6 encoded order word in the correct position with respect to the ordering.
     * <p>
     * E.g.
     *   - insertOrderWord(120, 3, 0) must return 1203 (decimal 48*6 + 3)
     *   - insertOrderWord(120, 3, 1, ptr) must return 1230 (decimal 8*36 + 3*6 + 0)
     *   - insertOrderWord(120, 3, 2, ptr) must return 1320 (decimal 1*216 + 3*36 + 12)
     *   - insertOrderWord(120, 3, 3, ptr) must return 3120 (decimal 3*216 + 48)
     */
    private static int insertInOrderWord(int order, int newIndex, int smallerCount)
    {
        int r = 1;
        for (int i = 0; i < smallerCount; ++i)
            r *= 6;
        int head = order / r;
        int tail = order % r;
        // insert newIndex after the ones we have passed (order % r) and before the remaining (order / r)
        return tail + (head * 6 + newIndex) * r;
    }

    /**
     * Non-volatile version of attachChildToSplit. Used when the split node is not reachable yet (during the conversion
     * from sparse).
     */
    private void attachChildToSplitNonVolatile(int node, int trans, int newChild) throws SpaceExhaustedException
    {
        assert offset(node) == SPLIT_OFFSET : "Invalid split node in trie";
        int midPos = splitBlockPointerAddress(node, splitNodeMidIndex(trans), SPLIT_START_LEVEL_LIMIT);
        int mid = getInt(midPos);
        if (isNull(mid))
        {
            mid = createEmptySplitNode();
            putInt(midPos, mid);
        }

        assert offset(mid) == SPLIT_OFFSET : "Invalid split node in trie";
        int tailPos = splitBlockPointerAddress(mid, splitNodeTailIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        int tail = getInt(tailPos);
        if (isNull(tail))
        {
            tail = createEmptySplitNode();
            putInt(tailPos, tail);
        }

        assert offset(tail) == SPLIT_OFFSET : "Invalid split node in trie";
        int childPos = splitBlockPointerAddress(tail, splitNodeChildIndex(trans), SPLIT_OTHER_LEVEL_LIMIT);
        putInt(childPos, newChild);
    }

    /**
     * Attach a child to the given chain node. This may be an update for an existing branch with different target
     * address, or a second child for the node.
     * This method always copies the node -- with the exception of updates that change the child of the last node in a
     * chain block with matching transition byte (which this method is not used for, see attachChild), modifications to
     * chain nodes cannot be done in place, either because we introduce a new transition byte and have to convert from
     * the single-transition chain type to sparse, or because we have to remap the child from the implicit node + 1 to
     * something else.
     */
    private int attachChildToChain(int node, int transitionByte, int newChild) throws SpaceExhaustedException
    {
        int existingByte = getUnsignedByte(node);
        if (transitionByte == existingByte)
        {
            // This will only be called if new child is different from old, and the update is not on the final child
            // where we can change it in place (see attachChild). We must always create something new.
            // If the child is a chain, we can expand it (since it's a different value, its branch must be new and
            // nothing can already reside in the rest of the block).
            return expandOrCreateChainNode(transitionByte, newChild);
        }

        // The new transition is different, so we no longer have only one transition. Change type.
        int existingChild = node + 1;
        if (offset(existingChild) == LAST_POINTER_OFFSET)
        {
            existingChild = getInt(existingChild);
        }
        return createSparseNode(existingByte, existingChild, transitionByte, newChild);
    }

    private boolean isExpandableChain(int newChild)
    {
        int newOffset = offset(newChild);
        return newChild > 0 && newChild - 1 > NONE && newOffset > CHAIN_MIN_OFFSET && newOffset <= CHAIN_MAX_OFFSET;
    }

    /**
     * Create a sparse node with two children.
     */
    private int createSparseNode(int byte1, int child1, int byte2, int child2) throws SpaceExhaustedException
    {
        assert byte1 != byte2 : "Attempted to create a sparse node with two of the same transition";
        if (byte1 > byte2)
        {
            // swap them so the smaller is byte1, i.e. there's always something bigger than child 0 so 0 never is
            // at the end of the order
            int t = byte1; byte1 = byte2; byte2 = t;
            t = child1; child1 = child2; child2 = t;
        }

        int node = allocateBlock() + SPARSE_OFFSET;
        putByte(node + SPARSE_BYTES_OFFSET + 0,  (byte) byte1);
        putByte(node + SPARSE_BYTES_OFFSET + 1,  (byte) byte2);
        putInt(node + SPARSE_CHILDREN_OFFSET + 0 * 4, child1);
        putInt(node + SPARSE_CHILDREN_OFFSET + 1 * 4, child2);
        putShort(node + SPARSE_ORDER_OFFSET,  (short) (1 * 6 + 0));
        // Note: this does not need a volatile write as it is a new node, returning a new pointer, which needs to be
        // put in an existing node or the root. That action ends in a happens-before enforcing write.
        return node;
    }

    /**
     * Creates a chain node with the single provided transition (pointing to the provided child).
     * Note that to avoid creating inefficient tries with under-utilized chain nodes, this should only be called from
     * {@link #expandOrCreateChainNode} and other call-sites should call {@link #expandOrCreateChainNode}.
     */
    private int createNewChainNode(int transitionByte, int newChild) throws SpaceExhaustedException
    {
        int newNode = allocateBlock() + LAST_POINTER_OFFSET - 1;
        putByte(newNode, (byte) transitionByte);
        putInt(newNode + 1, newChild);
        // Note: this does not need a volatile write as it is a new node, returning a new pointer, which needs to be
        // put in an existing node or the root. That action ends in a happens-before enforcing write.
        return newNode;
    }

    /** Like {@link #createNewChainNode}, but if the new child is already a chain node and has room, expand
     * it instead of creating a brand new node. */
    private int expandOrCreateChainNode(int transitionByte, int newChild) throws SpaceExhaustedException
    {
        if (isExpandableChain(newChild))
        {
            // attach as a new character in child node
            int newNode = newChild - 1;
            putByte(newNode, (byte) transitionByte);
            return newNode;
        }

        return createNewChainNode(transitionByte, newChild);
    }

    private int createEmptySplitNode() throws SpaceExhaustedException
    {
        return allocateCleanNode() + SPLIT_OFFSET;
    }

    int createContentPrefixNode(int contentId, int child, boolean isSafeChain) throws SpaceExhaustedException
    {
        assert !isNullOrLeaf(child) : "Content prefix node cannot reference a childless node.";
        return createPrefixNode(contentId, child, isSafeChain, 0);
    }

    int createPrefixNode(int contentId, int child, boolean isSafeChain, int additionalFlags) throws SpaceExhaustedException
    {
        int offset = offset(child);
        int node;
        if (!isNullOrLeaf(child) && (offset == SPLIT_OFFSET || isSafeChain && offset > (PREFIX_FLAGS_OFFSET + PREFIX_OFFSET) && offset <= CHAIN_MAX_OFFSET))
        {
            // We can do an embedded prefix node
            // Note: for chain nodes we have a risk that the node continues beyond the current point, in which case
            // creating the embedded node may overwrite information that is still needed by concurrent readers or the
            // mutation process itself.
            node = (child & -BLOCK_SIZE) | PREFIX_OFFSET;
            putByte(node + PREFIX_FLAGS_OFFSET, (byte) (offset | PREFIX_EMBEDDED_FLAG | additionalFlags));
        }
        else
        {
            // Full prefix node
            node = allocateBlock() + PREFIX_OFFSET;
            putByte(node + PREFIX_FLAGS_OFFSET, (byte) additionalFlags);
            putInt(node + PREFIX_POINTER_OFFSET, child);
        }

        putInt(node + PREFIX_CONTENT_OFFSET, contentId);
        return node;
    }

    int updatePrefixNodeChild(int node, int child, boolean forcedCopy) throws SpaceExhaustedException
    {
        assert offset(node) == PREFIX_OFFSET : "updatePrefix called on non-prefix node";
        assert !isNullOrLeaf(child) : "Prefix node cannot reference a childless node.";

        // We can only update in-place if we have a full prefix node
        if (!forcedCopy && !isEmbeddedPrefixNode(node))
        {
            // This attaches the child branch and makes it reachable -- the write must be volatile.
            putIntVolatile(node + PREFIX_POINTER_OFFSET, child);
            return node;
        }
        else
        {
            int contentId = getInt(node + PREFIX_CONTENT_OFFSET);
            return createContentPrefixNode(contentId, child, true);
        }
    }

    private boolean isEmbeddedPrefixNode(int node)
    {
        return (getUnsignedByte(node + PREFIX_FLAGS_OFFSET) & PREFIX_EMBEDDED_FLAG) != 0;
    }

    /**
     * Copy the content from an existing node, if it has any, to a newly-prepared update for its child.
     *
     * @param existingFullNode pointer to the existing node before skipping over content nodes, i.e. this is
     *                               either the same as existingPostContentNode or a pointer to a prefix or leaf node
     *                               whose child is existingPostContentNode
     * @param existingPostContentNode pointer to the existing node being updated, after any content nodes have been
     *                                skipped and before any modification have been applied; always a non-content node
     * @param updatedPostContentNode is the updated node, i.e. the node to which all relevant modifications have been
     *                               applied; if the modifications were applied in-place, this will be the same as
     *                               existingPostContentNode, otherwise a completely different pointer; always a non-
     *                               content node
     * @param forcedCopy whether or not we need to preserve all pre-existing data for concurrent readers
     * @return a node which has the children of updatedPostContentNode combined with the content of
     *         existingFullNode
     */
    private int preservePrefix(int existingFullNode,
                               int existingPostContentNode,
                               int updatedPostContentNode,
                               boolean forcedCopy)
    throws SpaceExhaustedException
    {
        if (existingFullNode == existingPostContentNode)
            return updatedPostContentNode;     // no content to preserve

        if (existingPostContentNode == updatedPostContentNode)
        {
            assert !forcedCopy;
            return existingFullNode;     // child didn't change, no update necessary
        }

        // else we have existing prefix node, and we need to reference a new child
        if (isLeaf(existingFullNode))
        {
            return createContentPrefixNode(existingFullNode, updatedPostContentNode, true);
        }

        assert offset(existingFullNode) == PREFIX_OFFSET : "Unexpected content in non-prefix and non-leaf node.";
        return updatePrefixNodeChild(existingFullNode, updatedPostContentNode, forcedCopy);
    }

    final ApplyState applyState = new ApplyState();

    /**
     * Represents the state for an {@link #apply} operation. Contains a stack of all nodes we descended through
     * and used to update the nodes with any new data during ascent.
     * <p>
     * To make this as efficient and GC-friendly as possible, we use an integer array (instead of is an object stack)
     * and we reuse the same object. The latter is safe because memtable tries cannot be mutated in parallel by multiple
     * writers.
     */
    class ApplyState
    {
        int[] data = new int[16 * 5];
        int stackDepth = -1;
        int currentDepth = -1;

        /**
         * Pointer to the existing node before skipping over content nodes, i.e. this is either the same as
         * existingPostContentNode or a pointer to a prefix or leaf node whose child is existingPostContentNode.
         */
        int existingFullNode()
        {
            return data[stackDepth * 5 + 0];
        }
        void setExistingFullNode(int value)
        {
            data[stackDepth * 5 + 0] = value;
        }
        int existingFullNodeAtDepth(int stackDepth)
        {
            return data[stackDepth * 5 + 0];
        }

        /**
         * Pointer to the existing node being updated, after any content nodes have been skipped and before any
         * modification have been applied. Always a non-content node.
         */
        int existingPostContentNode()
        {
            return followPrefixTransition(existingFullNode());
        }

        /**
         * Pointer to the existing node after any alternative prefix. May be a content-carrying prefix, a leaf or a
         * normal node, but cannot be an alternative prefix.
         */
        int existingPostAlternateNode()
        {
            final int node = existingFullNode();
            if (getAlternateBranch(node) != NONE)
                return getPrefixChild(node, getUnsignedByte(node + PREFIX_FLAGS_OFFSET));
            else
                return node;
        }

        /**
         * The updated node, i.e. the node to which the relevant modifications are being applied. This will change as
         * children are processed and attached to the node. After all children have been processed, this will contain
         * the fully updated node (i.e. the union of existingPostContentNode and mutationNode) without any content,
         * which will be processed separately and, if necessary, attached ahead of this. If the modifications were
         * applied in-place, this will be the same as existingPostContentNode, otherwise a completely different
         * pointer. Always a non-content node.
         */
        int updatedPostContentNode()
        {
            return data[stackDepth * 5 + 1];
        }
        void setUpdatedPostContentNode(int value)
        {
            data[stackDepth * 5 + 1] = value;
        }

        /**
         * The transition we took on the way down.
         */
        int transition()
        {
            return data[stackDepth * 5 + 2];
        }
        void setTransition(int transition)
        {
            data[stackDepth * 5 + 2] = transition;
        }
        int transitionAtDepth(int stackDepth)
        {
            return data[stackDepth * 5 + 2];
        }

        /**
         * The compiled content index. Needed because we can only access a cursor's content on the way down but we can't
         * attach it until we ascend from the node.
         */
        int contentId()
        {
            return data[stackDepth * 5 + 3];
        }
        void setContentId(int value)
        {
            data[stackDepth * 5 + 3] = value;
        }

        /**
         * The alternate branch of this node. Updated by alternate branch processing (descendTo/attachAlternate).
         */
        int alternateBranch()
        {
            return data[stackDepth * 5 + 4];
        }
        void setAlternateBranch(int value)
        {
            data[stackDepth * 5 + 4] = value;
        }

        int ascendLimit = -1;

        int setAscendLimit(int newLimit)
        {
            int prev = ascendLimit;
            ascendLimit = newLimit;
            return prev;
        }


        ApplyState start()
        {
            int existingFullNode = root;
            stackDepth = -1;
            currentDepth = 0;
            ascendLimit = 0;

            descendInto(existingFullNode);
            return this;
        }

        /**
         * Returns true if the depth signals mutation cursor is exhausted.
         */
        boolean advanceTo(int depth, int transition, int forcedCopyDepth) throws SpaceExhaustedException
        {
            while (currentDepth > Math.max(ascendLimit, depth - 1))
            {
                // There are no more children. Ascend to the parent state to continue walk.
                attachAndMoveToParentState(forcedCopyDepth);
            }
            if (depth == -1)
                return true;

            // We have a transition, get child to descend into
            descend(transition);
            return false;
        }

        boolean advanceToNextExistingOr(int depth, int transition, int forcedCopyDepth) throws SpaceExhaustedException
        {
            setTransition(-1); // we have newly descended to a node, start with its first child
            while (true)
            {
                int currentTransition = transition();
                int nextTransition = getNextTransition(existingFullNode(), currentTransition + 1);
                if (currentDepth + 1 == depth && nextTransition >= transition)
                {
                    descend(transition);
                    return true;
                }
                if (nextTransition <= 0xFF)
                {
                    descend(nextTransition);
                    return false;
                }
                if (currentDepth <= ascendLimit)
                    break;

                attachAndMoveToParentState(forcedCopyDepth);
            }
            assert depth == -1;
            return true;
        }

        /**
         * Descend to a child node. Prepares a new entry in the stack for the node.
         */
        void descend(int transition)
        {
            setTransition(transition);
            ++currentDepth;
            int existingFullNode = getChild(existingFullNode(), transition);

            descendInto(existingFullNode);
        }

        /**
         * Descend to the alternate (ie. 𝜀-transition) path of this node. Adds an entry to the stack but does not
         * increase the descent depth.
         */
        void descendToAlternate()
        {
            // We are positioned on the corresponding normal node. Create a stack entry but do not increase
            // currentDepth.
            int existingFullNode = getAlternateBranch(existingFullNode());
            assert stackDepth >= 0;
            descendInto(existingFullNode);
        }

        private void descendInto(int existingFullNode)
        {
            ++stackDepth;
            if (stackDepth * 5 >= data.length)
                data = Arrays.copyOf(data, stackDepth * 5 * 2);
            setExistingFullNode(existingFullNode);
            int node = existingFullNode;

            int existingAlternateBranch = getAlternateBranch(node);
            setAlternateBranch(existingAlternateBranch);
            if (existingAlternateBranch != NONE)
                node = getPrefixChild(node, getUnsignedByte(node + PREFIX_FLAGS_OFFSET));

            int existingContentId = NONE;
            int existingPostContentNode;
            if (isLeaf(node))
            {
                existingContentId = node;
                existingPostContentNode = NONE;
            }
            else if (offset(node) == PREFIX_OFFSET)
            {
                existingContentId = getInt(node + PREFIX_CONTENT_OFFSET);
                existingPostContentNode = followPrefixTransition(node);
            }
            else
                existingPostContentNode = node;
            setUpdatedPostContentNode(existingPostContentNode);
            setContentId(existingContentId);
        }

        T getContent()
        {
            int contentId = contentId();
            if (contentId == NONE)
                return null;
            return InMemoryTrie.this.getContent(contentId);
        }

        void setContent(T content, boolean forcedCopy)
        {
            int contentId = contentId();
            if (contentId == NONE || forcedCopy)
            {
                if (content != null)
                    setContentId(InMemoryTrie.this.addContent(content));

                // TODO: If forcedCopy is true, the old content is left referenced for concurrent readers, i.e. stale
                // data will remain in the heap, not collectible by the GC. This should be addressed by cell reuse.
                // TODO: It may be worthwhile to check if combined matches existing
            }
            else
            {
                InMemoryTrie.this.setContent(contentId, content);
                // TODO: Delete on the way up
                // TODO: setContentId(NONE);
                //  and release contentId
            }
        }

        T getNearestContent()
        {
            // Assume any dead branch is deleted, thus: go upstack until first node for which we have a higher transition
            // and then repeatedly descend into first child until content.
            int stackPos = stackDepth;
            int node = NONE;
            setTransition(-1);      // In the node we have just descended to, start with its first child
            for (; stackPos >= 0 && node == NONE; --stackPos)
            {
                // TODO: accelerate this, especially going through prefix nodes
                node = getNextChild(existingFullNodeAtDepth(stackPos), transitionAtDepth(stackPos) + 1);
            }

            while (node != NONE)
            {
                T content = InMemoryTrie.this.getNodeContent(node);
                if (content != null)
                    return content;
                node = getNextChild(node, 0);
            }
            return null;
        }

        /**
         * Attach a child to the current node.
         */
        private void attachChild(int transition, int child, boolean forcedCopy) throws SpaceExhaustedException
        {
            int updatedPostContentNode = updatedPostContentNode();
            if (isNull(updatedPostContentNode))
                setUpdatedPostContentNode(expandOrCreateChainNode(transition, child));
            else if (forcedCopy)
                setUpdatedPostContentNode(attachChildCopying(updatedPostContentNode,
                                                             existingPostContentNode(),
                                                             transition,
                                                             child));
            else
                setUpdatedPostContentNode(InMemoryTrie.this.attachChild(updatedPostContentNode,
                                                                        transition,
                                                                        child));
        }

        /**
         * Apply the collected content to a node. Converts NONE to a leaf node, and adds or updates a prefix for all
         * others.
         */
        private int applyContent(int updatedPostContentNode, boolean forcedCopy) throws SpaceExhaustedException
        {
            int contentId = contentId();
            if (contentId == NONE)
                return updatedPostContentNode;

            if (isNull(updatedPostContentNode))
                return contentId;

            // applyPrefixChange does not understand leaf nodes, handle upgrade from one explicitly.
            final int existingPreContentNode = existingPostAlternateNode();
            if (isLeaf(existingPreContentNode))
                return createPrefixNode(contentId, updatedPostContentNode, true, 0);

            return applyPrefixChange(updatedPostContentNode,
                                     existingPreContentNode,
                                     existingPostContentNode(),
                                     contentId,
                                     0,
                                     forcedCopy);
        }

        /**
         * Apply any updates to the alternate branch of the node.
         */
        private int applyAlternateBranch(int updatedPostAlternateNode, boolean forcedCopy) throws SpaceExhaustedException
        {
            int alternateBranch = alternateBranch();
            if (alternateBranch == NONE)
                return updatedPostAlternateNode;

            return applyPrefixChange(updatedPostAlternateNode,
                                     existingFullNode(),
                                     existingPostAlternateNode(),
                                     alternateBranch,
                                     PREFIX_ALTERNATE_PATH_FLAG,
                                     forcedCopy);
        }

        private int applyPrefixChange(int updatedPostPrefixNode,
                                      int existingPrePrefixNode,
                                      int existingPostPrefixNode,
                                      int prefixData,
                                      int flagBits,
                                      boolean forcedCopy)
        throws SpaceExhaustedException
        {
            // assumes prefixData is not empty
            boolean childChanged = updatedPostPrefixNode != existingPostPrefixNode;
            boolean prefixWasPresent = existingPrePrefixNode != existingPostPrefixNode;
            boolean dataChanged = !prefixWasPresent || prefixData != getInt(existingPrePrefixNode + PREFIX_CONTENT_OFFSET);
            if (!childChanged && !dataChanged)
                return existingPrePrefixNode;

            // In addition to forced copying, we can't update in-place if there was no preexisting prefix, or if the
            // prefix was embedded and the target node must change.
            if (forcedCopy ||
                !prefixWasPresent ||
                isEmbeddedPrefixNode(existingPrePrefixNode) && childChanged)
            {
                if (forcedCopy && !childChanged && isEmbeddedPrefixNode(existingPrePrefixNode))
                {
                    // If we directly create in this case, we will find embedding is possible and will overwrite the
                    // previous value.
                    // We could create a separate metadata node referencing the child, but in that case we'll
                    // use two nodes while one suffices. Instead, copy the child and embed the new metadata.
                    updatedPostPrefixNode = copyBlock(existingPostPrefixNode);
                }
                return createPrefixNode(prefixData, updatedPostPrefixNode, isNull(existingPostPrefixNode), flagBits);
            }

            // Otherwise modify in place
            assert (getUnsignedByte(existingPrePrefixNode + PREFIX_FLAGS_OFFSET) & PREFIX_ALTERNATE_PATH_FLAG) == flagBits;
            if (childChanged) // to use volatile write but also ensure we don't corrupt embedded nodes
                putIntVolatile(existingPrePrefixNode + PREFIX_POINTER_OFFSET, updatedPostPrefixNode);
            if (dataChanged)
                putIntVolatile(existingPrePrefixNode + PREFIX_CONTENT_OFFSET, prefixData);
            return existingPrePrefixNode;
        }

        private int applyPrefixes(boolean forcedCopy) throws SpaceExhaustedException
        {
            return applyAlternateBranch(applyContent(updatedPostContentNode(), forcedCopy), forcedCopy);
        }

        /**
         * After a node's children are processed, this is called to ascend from it. This means applying the collected
         * content to the compiled updatedPostContentNode and creating a mapping in the parent to it (or updating if
         * one already exists).
         */
        void attachAndMoveToParentState(int forcedCopyDepth) throws SpaceExhaustedException
        {
            int updatedFullNode = applyPrefixes(currentDepth >= forcedCopyDepth);
            int existingFullNode = existingFullNode();
            --currentDepth;
            --stackDepth;
            assert stackDepth >= 0;

            if (updatedFullNode != existingFullNode)
                attachChild(transition(), updatedFullNode, currentDepth >= forcedCopyDepth);
        }

        /**
         * Ascend and update the root at the end of processing.
         */
        void attachRoot(int forcedCopyDepth) throws SpaceExhaustedException
        {
            int updatedFullNode = applyPrefixes(0 >= forcedCopyDepth);
            int existingFullNode = existingFullNode();
            assert root == existingFullNode : "Unexpected change to root. Concurrent trie modification?";
            if (updatedFullNode != existingFullNode)
            {
                // Only write to root if they are different (value doesn't change, but
                // we don't want to invalidate the value in other cores' caches unnecessarily).
                root = updatedFullNode;
            }
        }

        /**
         * Ascend and update the alternate link at the end of alternate branch processing.
         */
        void attachAlternate(boolean forcedCopy) throws SpaceExhaustedException
        {
            int updatedFullNode = applyPrefixes(forcedCopy);
            int existingFullNode = existingFullNode();
            // currentDepth does not change
            --stackDepth;
            assert stackDepth >= 0;
            assert existingFullNode == alternateBranch();
            setAlternateBranch(updatedFullNode);
        }
    }

    /**
     * Somewhat similar to {@link Trie.MergeResolver}, this encapsulates logic to be applied whenever new content is
     * being upserted into a {@link InMemoryTrie}. Unlike {@link Trie.MergeResolver}, {@link UpsertTransformer} will be
     * applied no matter if there's pre-existing content for that trie key/path or not.
     *
     * @param <T> The content type for this {@link InMemoryTrie}.
     * @param <U> The type of the new content being applied to this {@link InMemoryTrie}.
     */
    public interface UpsertTransformer<T, U>
    {
        /**
         * Called when there's content in the updating trie.
         *
         * @param existing Existing content for this key, or null if there isn't any.
         * @param update   The update, always non-null.
         * @return The combined value to use. Cannot be null.
         */
        T apply(T existing, U update);
    }

    /**
     * Interface providing features of the mutating node during mutation done using {@link #apply}.
     * Effectively a subset of the {@link TrieImpl.Cursor} interface which only permits operations that are safe to
     * perform before iterating the children of the mutation node to apply the branch mutation.
     *
     * This is mainly used as an argument to predicates that decide when to copy substructure when modifying tries,
     * which enables different kinds of atomicity and consistency guarantees.
     *
     * See the InMemoryTrie javadoc or InMemoryTrieThreadedTest for demonstration of the typical usages and what they
     * achieve.
     */
    public interface NodeFeatures<T>
    {
        /**
         * Whether or not the node has more than one descendant. If a checker needs mutations to be atomic, they can
         * return true when this becomes true.
         */
        boolean isBranching();

        /**
         * The metadata associated with the node. If readers need to see a consistent view (i.e. where older updates
         * cannot be missed if a new one is presented) below some specified point (e.g. within a partition), the checker
         * should return true when it identifies that point.
         */
        T content();
    }

    static class Mutation<T, U> implements NodeFeatures<U>
    {
        final UpsertTransformer<T, U> transformer;
        final Predicate<NodeFeatures<U>> needsForcedCopy;
        final TrieImpl.Cursor<U> mutationCursor;
        final InMemoryTrie<T>.ApplyState state;
        int forcedCopyDepth;

        Mutation(UpsertTransformer<T, U> transformer,
                 Predicate<NodeFeatures<U>> needsForcedCopy,
                 TrieImpl.Cursor<U> mutationCursor,
                 InMemoryTrie<T>.ApplyState state)
        {
            assert mutationCursor.depth() == 0 : "Unexpected non-fresh cursor.";
            assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
            this.transformer = transformer;
            this.needsForcedCopy = needsForcedCopy;
            this.mutationCursor = mutationCursor;
            this.state = state;
        }

        void apply() throws SpaceExhaustedException
        {
            int depth = state.currentDepth;
            int prevAscendLimit = state.setAscendLimit(state.currentDepth);
            while (true)
            {
                if (depth <= forcedCopyDepth)
                    forcedCopyDepth = needsForcedCopy.test(this) ? depth : Integer.MAX_VALUE;

                applyContent();

                depth = mutationCursor.advance();
                if (state.advanceTo(depth, mutationCursor.incomingTransition(), forcedCopyDepth))
                    break;
                assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
            }
            state.setAscendLimit(prevAscendLimit);
        }

        void applyContent()
        {
            U content = mutationCursor.content();
            if (content != null)
            {
                T existingContent = state.getContent();
                T combinedContent = transformer.apply(existingContent, content);
                state.setContent(combinedContent, // can be null
                                 state.currentDepth >= forcedCopyDepth); // this is called at the start of processing
            }
        }


        void complete() throws SpaceExhaustedException
        {
            assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
            state.attachRoot(forcedCopyDepth);
        }

        @Override
        public boolean isBranching()
        {
            // TODO: consider implementing this in the Trie interface so that e.g. InMemoryTrie can optimize it
            TrieImpl.Cursor<U> dupe = mutationCursor.duplicate();
            int depth = dupe.depth();
            if (depth < 0)
                return false;
            int childDepth = dupe.advance();
            return childDepth > depth &&
                   dupe.skipTo(childDepth, dupe.incomingTransition() + 1) == childDepth;
        }

        @Override
        public U content()
        {
            return mutationCursor.content();
        }
    }

    /**
     * Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
     * with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
     * @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
     * different than the element type for this memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value. Applied even if there's no pre-existing value in the memtable trie.
     * @param needsForcedCopy a predicate which decides when to fully copy a branch to provide atomicity guarantees to
     * concurrent readers. See NodeFeatures for details.
     */
    public <U> void apply(Trie<U> mutation,
                          final UpsertTransformer<T, U> transformer,
                          final Predicate<NodeFeatures<U>> needsForcedCopy)
    throws SpaceExhaustedException
    {
        Mutation<T, U> m = new Mutation<>(transformer,
                                          needsForcedCopy,
                                          TrieImpl.impl(mutation).cursor(Direction.FORWARD),
                                          applyState.start());
        m.apply();
        m.complete();
    }

    // TODO: Create class that implements NodeFeatures and holds cursor, transformer, needsForcedCopy
    // TODO: Query and track forcedCopy depth, pass to advanceTo
    static <T, U> void applyContent(InMemoryTrie<T>.ApplyState state, TrieImpl.Cursor<U> mutationCursor, UpsertTransformer<T, U> transformer)
    {
        U content = mutationCursor.content();
        if (content != null)
        {
            T existingContent = state.getContent();
            T combinedContent = transformer.apply(existingContent, content);
            // TODO
            state.setContent(combinedContent, false); // can be null
        }
    }

    private static <T, U>
    void deleteContent(InMemoryTrie<T>.ApplyState state, UpsertTransformer<T, U> transformer, U mutationContent)
    {
        T existingContent = state.getContent();
        if (existingContent != null)
        {
            T combinedContent = transformer.apply(existingContent, mutationContent);
            // TODO
            state.setContent(combinedContent, false); // can be null
        }
    }

    // TODO: pass and use forcedCopy control in all apply/delete variations
    public void delete(TrieSet set) throws SpaceExhaustedException
    {
        TrieSetImpl.Cursor cursor = TrieSetImpl.impl(set).cursor(Direction.FORWARD);
        ApplyState state = applyState.start();
        assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        delete(state, cursor);
        assert state.currentDepth == 0 : "Unexpected change to applyState. Concurrent trie modification?";
        // TODO
        state.attachRoot(Integer.MAX_VALUE);
    }

    private static <T> T deleteEntry(T entry, TrieSetImpl.RangeState state)
    {
        return state.applicableBefore ? null : entry;
    }

    // TODO: Use mutation class, implement consistency protections
    void delete(InMemoryTrie<T>.ApplyState state, TrieSetImpl.Cursor mutationCursor)
    throws SpaceExhaustedException
    {
        int prevAscendDepth = state.setAscendLimit(state.currentDepth);
        UpsertTransformer<T, TrieSetImpl.RangeState> transformer = InMemoryTrie::deleteEntry;
        TrieSetImpl.RangeState content = mutationCursor.state();
        // The set may start already in a deleted range. If so, pretend there's a START at the initial position.
        if (content.asContent == null && content.applicableBefore)
            content = TrieSetImpl.RangeState.START;
        else
            content = content.asContent;

        while (!deleteStep(state, mutationCursor, transformer, content))
            content = mutationCursor.content();
        state.setAscendLimit(prevAscendDepth);
    }

    static <T, M extends RangeTrie.RangeMarker<M>>
    void delete(InMemoryTrie<T>.ApplyState state, RangeTrieImpl.Cursor<M> mutationCursor, UpsertTransformer<T, M> transformer)
    throws SpaceExhaustedException
    {
        // While activeDeletion is not set, follow the mutation trie.
        // When a deletion is found, get existing covering state, combine and apply/store.
        // Get rightSideAsCovering and walk the full existing trie to apply, advancing mutation cursor in parallel
        // until we see another entry in mutation trie.
        // Repeat until mutation trie is exhausted.
        int prevAscendDepth = state.setAscendLimit(state.currentDepth);
        while (!deleteStep(state, mutationCursor, transformer, mutationCursor.content())) {}
        state.setAscendLimit(prevAscendDepth);
    }

    private static <T, M extends RangeTrie.RangeMarker<M>>
    boolean deleteStep(InMemoryTrie<T>.ApplyState state, RangeTrieImpl.Cursor<M> mutationCursor, UpsertTransformer<T, M> transformer, M content)
    throws SpaceExhaustedException
    {
        if (content != null)
        {
            deleteContent(state, transformer, content);
            M mutationCoveringState = content.asCoveringState(Direction.REVERSE); // Use the right side of the deletion
            // Several cases:
            // - New deletion is point deletion: Apply it and move on to next mutation branch.
            // - New deletion starts range and there is no existing or it beats the existing: Walk both tries in
            //   parallel to apply deletion and adjust on any change.
            // - New deletion starts range and existing beats it: We still have to walk both tries in parallel,
            //   because existing deletion may end before the newly introduced one, and we want to apply that when
            //   it does.
            if (mutationCoveringState != null)
            {
                boolean done = deleteRange(state, mutationCursor, transformer, mutationCoveringState);
                if (done)
                    return true;
            }
        }

        int depth = mutationCursor.advance();
        // Descend but do not modify anything yet.
        // TODO
        if (state.advanceTo(depth, mutationCursor.incomingTransition(), Integer.MAX_VALUE))
            return true;
        assert state.currentDepth == depth : "Unexpected change to applyState. Concurrent trie modification?";
        return false;
    }

    static <T, M extends RangeTrie.RangeMarker<M>>
    boolean deleteRange(InMemoryTrie<T>.ApplyState state,
                        RangeTrieImpl.Cursor<M> mutationCursor,
                        UpsertTransformer<T, M> transformer,
                        M mutationCoveringState)
    throws SpaceExhaustedException
    {
        boolean atMutation = true;
        int depth = mutationCursor.depth();
        int transition = mutationCursor.incomingTransition();
        // We are walking both tries in parallel.
        while (true)
        {
            if (atMutation)
            {
                depth = mutationCursor.advance();
                transition = mutationCursor.incomingTransition();
            }
            // TODO
            atMutation = state.advanceToNextExistingOr(depth, transition, Integer.MAX_VALUE);
            if (atMutation && depth == -1)
                return true;

            T existingContent = state.getContent();
            M mutationContent = atMutation ? mutationCursor.content() : null;
            if (mutationContent != null)
            {
                // TODO: maybe assert correct closing of ranges
                deleteContent(state, transformer, mutationContent);
                mutationCoveringState = mutationContent.asCoveringState(Direction.REVERSE);
                if (mutationCoveringState == null || !mutationCoveringState.precedingIncluded(Direction.REVERSE))
                    return false; // mutation deletion range was closed, we can continue normal mutation cursor iteration
            }
            else if (existingContent != null)
                deleteContent(state, transformer, mutationCoveringState);
        }
    }

    /**
     * Map-like put method, using the apply machinery above which cannot run into stack overflow. When the correct
     * position in the trie has been reached, the value will be resolved with the given function before being placed in
     * the trie (even if there's no pre-existing content in this trie).
     * @param key the trie path/key for the given value.
     * @param value the value being put in the memtable trie. Note that it can be of type different than the element
     * type for this memtable trie. It's up to the {@code transformer} to return the final value that will stay in
     * the memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value (of a potentially different type), returning the final value that will stay in the memtable trie. Applied
     * even if there's no pre-existing value in the memtable trie.
     */
    public <R> void putSingleton(ByteComparable key,
                                 R value,
                                 UpsertTransformer<T, ? super R> transformer) throws SpaceExhaustedException
    {
        apply(Trie.singleton(key, value), transformer, Predicates.alwaysFalse());
    }

    /**
     * A version of putSingleton which uses recursive put if the last argument is true.
     */
    public <R> void putSingleton(ByteComparable key,
                                 R value,
                                 UpsertTransformer<T, ? super R> transformer,
                                 boolean useRecursive) throws SpaceExhaustedException
    {
        if (useRecursive)
            putRecursive(key, value, transformer);
        else
            putSingleton(key, value, transformer);
    }

    /**
     * Map-like put method, using a fast recursive implementation through the key bytes. May run into stack overflow if
     * the trie becomes too deep. When the correct position in the trie has been reached, the value will be resolved
     * with the given function before being placed in the trie (even if there's no pre-existing content in this trie).
     * @param key the trie path/key for the given value.
     * @param value the value being put in the memtable trie. Note that it can be of type different than the element
     * type for this memtable trie. It's up to the {@code transformer} to return the final value that will stay in
     * the memtable trie.
     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
     * value (of a potentially different type), returning the final value that will stay in the memtable trie. Applied
     * even if there's no pre-existing value in the memtable trie.
     */
    public <R> void putRecursive(ByteComparable key, R value, final UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        int newRoot = putRecursive(root, key.asComparableBytes(BYTE_COMPARABLE_VERSION), value, transformer);
        if (newRoot != root)
            root = newRoot;
    }

    <R> int putRecursive(int node, ByteSource key, R value, final UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        int transition = key.next();
        if (transition == ByteSource.END_OF_STREAM)
            return applyContent(node, value, transformer);

        int child = getChild(node, transition);
        int newChild = putRecursive(child, key, value, transformer);
        return completeUpdate(node, transition, child, newChild);
    }

    public <R> void putAlternativeRecursive(ByteComparable key, R value, final UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        final ByteSource keyComparableBytes = key.asComparableBytes(BYTE_COMPARABLE_VERSION);
        if (isNull(root)) // special case for empty trie
        {
            int newAlternateBranch = putRecursive(NONE, keyComparableBytes, value, transformer);
            root = createPrefixNode(newAlternateBranch, NONE, true, PREFIX_ALTERNATE_PATH_FLAG);
        }
        else
        {
            int newRoot = putAlternativeRecursive(root, keyComparableBytes, value, transformer);
            if (newRoot != root)
                root = newRoot;
        }
    }

    private <R> int putAlternativeRecursive(int node, ByteSource key, R value, final UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        int alternateBranch = getAlternateBranch(node);
        if (alternateBranch != NONE)
        {
            int newAlternateBranch = putRecursive(alternateBranch, key, value, transformer);
            if (newAlternateBranch == alternateBranch)
                return node;
            assert !isNullOrLeaf(node) && offset(node) == PREFIX_OFFSET;
            putInt(node + PREFIX_CONTENT_OFFSET, newAlternateBranch);
            return node;
        }

        int transition = key.next();
        if (transition == ByteSource.END_OF_STREAM)
        {
            // Reached a match for the alternative in the normal path. Create an alternative here.
            int alternate = addContent(transformer.apply(null, value));
            return createPrefixNode(alternate, node, false, PREFIX_ALTERNATE_PATH_FLAG);
        }

        int child = getChild(node, transition);
        if (isNull(child))
        {
            int newChild = putRecursive(NONE, key, value, transformer);
            int newAlternateBranch = expandOrCreateChainNode(transition, newChild);
            return createPrefixNode(newAlternateBranch, node, false, PREFIX_ALTERNATE_PATH_FLAG);
        }

        int newChild = putAlternativeRecursive(child, key, value, transformer);
        return completeUpdate(node, transition, child, newChild);
    }


    // TODO: remove or change to be atomic
    public <R> void putAlternativeRangeRecursive(ByteComparable start, R startValue, ByteComparable end, R endValue, final UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        final ByteSource startComparableBytes = start.asComparableBytes(BYTE_COMPARABLE_VERSION);
        final ByteSource endComparableBytes = end.asComparableBytes(BYTE_COMPARABLE_VERSION);
        if (isNull(root)) // special case for empty trie
        {
            int newAlternateBranch = putRangeRecursive(NONE, startComparableBytes, startValue, endComparableBytes, endValue, transformer);
            root = createPrefixNode(newAlternateBranch, NONE, true, PREFIX_ALTERNATE_PATH_FLAG);
        }
        else
        {
            int newRoot = putAlternativeRangeRecursive(root, startComparableBytes, startValue, endComparableBytes, endValue, transformer);
            if (newRoot != root)
                root = newRoot;
        }
    }

    // Testing only
    <R> void putRangeRecursive(ByteComparable start, R startValue, ByteComparable end, R endValue, final UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        final ByteSource startComparableBytes = start.asComparableBytes(BYTE_COMPARABLE_VERSION);
        final ByteSource endComparableBytes = end.asComparableBytes(BYTE_COMPARABLE_VERSION);
        int newRoot = putRangeRecursive(root, startComparableBytes, startValue, endComparableBytes, endValue, transformer);
        if (newRoot != root)
            root = newRoot;
    }

    private <R> int putRangeRecursive(int node, ByteSource start, R startValue, ByteSource end, R endValue, final UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        int stransition = start.next();
        int etransition = end.next();
        if (stransition != etransition || stransition == ByteSource.END_OF_STREAM)
        {
            node = putRecursive(node, stransition, start, startValue, transformer);
            node = putRecursive(node, etransition, end, endValue, transformer);
            return node;
        }
        else
        {
            int oldChild = getChild(node, stransition);
            int newChild = putRangeRecursive(oldChild, start, startValue, end, endValue, transformer);
            return completeUpdate(node, stransition, oldChild, newChild);
        }
    }

    private <R> int putAlternativeRangeRecursive(int node, ByteSource start, R startValue, ByteSource end, R endValue, final UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        int alternateBranch = getAlternateBranch(node);
        if (alternateBranch != NONE)
        {
            int newAlternateBranch = putRangeRecursive(alternateBranch, start, startValue, end, endValue, transformer);
            if (newAlternateBranch == alternateBranch)
                return node;
            assert !isNullOrLeaf(node) && offset(node) == PREFIX_OFFSET;
            putInt(node + PREFIX_CONTENT_OFFSET, newAlternateBranch);
            return node;
        }

        int stransition = start.next();
        int etransition = end.next();
        if (stransition != etransition || stransition == ByteSource.END_OF_STREAM)
        {
            // Reached the lowest common ancestor. Time to go into alternate branch.
            int alternate = NONE;
            alternate = putRecursive(alternate, stransition, start, startValue, transformer);
            alternate = putRecursive(alternate, etransition, end, endValue, transformer);
            return createPrefixNode(alternate, node, false, PREFIX_ALTERNATE_PATH_FLAG);
        }

        int oldChild = getChild(node, stransition);
        if (isNull(oldChild))
        {
            int newChild = putRangeRecursive(NONE, start, startValue, end, endValue, transformer);
            int newAlternateBranch = expandOrCreateChainNode(stransition, newChild);
            return createPrefixNode(newAlternateBranch, node, false, PREFIX_ALTERNATE_PATH_FLAG);
        }

        int newChild = putAlternativeRangeRecursive(oldChild, start, startValue, end, endValue, transformer);
        return completeUpdate(node, stransition, oldChild, newChild);
    }

    private <R> int putRecursive(int node, int keyHead, ByteSource keyTail, R value, UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        if (keyHead == ByteSource.END_OF_STREAM)
            return applyContent(node, value, transformer);

        int oldChild = getChild(node, keyHead);
        int newChild = putRecursive(oldChild, keyTail, value, transformer);
        return completeUpdate(node, keyHead, getChild(node, keyHead), newChild);
    }

    // TODO: do we need forcedCopy here?
    int completeUpdate(int node, int transition, int oldChild, int newChild) throws SpaceExhaustedException
    {
        if (newChild == oldChild)
            return node;

        int skippedContent = followPrefixTransition(node);
        int attachedChild = !isNull(skippedContent)
                            ? attachChild(skippedContent, transition, newChild)  // Single path, no copying required
                            : expandOrCreateChainNode(transition, newChild);

        return preservePrefix(node, skippedContent, attachedChild, false);
    }

    private <R> int applyContent(int node, R value, UpsertTransformer<T, R> transformer) throws SpaceExhaustedException
    {
        if (isNull(node))
            return addContent(transformer.apply(null, value));

        if (isLeaf(node))
        {
            int contentId = node;
            setContent(contentId, transformer.apply(getContent(contentId), value));
            return node;
        }

        // TODO: alternative branch handling?
        if (offset(node) == PREFIX_OFFSET)
        {
            int contentId = getInt(node + PREFIX_CONTENT_OFFSET);
            setContent(contentId, transformer.apply(getContent(contentId), value));
            return node;
        }
        else
            return createContentPrefixNode(addContent(transformer.apply(null, value)), node, false);
    }

    /**
     * Returns true if the allocation threshold has been reached. To be called by the the writing thread (ideally, just
     * after the write completes). When this returns true, the user should switch to a new trie as soon as feasible.
     * <p>
     * The trie expects up to 10% growth above this threshold. Any growth beyond that may be done inefficiently, and
     * the trie will fail altogether when the size grows beyond 2G - 256 bytes.
     */
    public boolean reachedAllocatedSizeThreshold()
    {
        return allocatedPos >= ALLOCATED_SIZE_THRESHOLD;
    }

    /**
     * For tests only! Advance the allocation pointer (and allocate space) by this much to test behaviour close to
     * full.
     */
    @VisibleForTesting
    int advanceAllocatedPos(int wantedPos) throws SpaceExhaustedException
    {
        while (allocatedPos < wantedPos)
            allocateBlock();
        return allocatedPos;
    }

    /** Returns the off heap size of the memtable trie itself, not counting any space taken by referenced content. */
    public long sizeOffHeap()
    {
        return bufferType == BufferType.ON_HEAP ? 0 : allocatedPos;
    }

    /** Returns the on heap size of the memtable trie itself, not counting any space taken by referenced content. */
    public long sizeOnHeap()
    {
        return contentCount * MemoryMeterStrategy.MEMORY_LAYOUT.getReferenceSize() +
               REFERENCE_ARRAY_ON_HEAP_SIZE * getChunkIdx(contentCount, CONTENTS_START_SHIFT, CONTENTS_START_SIZE) +
               (bufferType == BufferType.ON_HEAP ? allocatedPos + EMPTY_SIZE_ON_HEAP : EMPTY_SIZE_OFF_HEAP) +
               REFERENCE_ARRAY_ON_HEAP_SIZE * getChunkIdx(allocatedPos, BUF_START_SHIFT, BUF_START_SIZE);
    }

    public long unusedReservedMemory()
    {
        int bufferOverhead = 0;
        if (bufferType == BufferType.ON_HEAP)
        {
            int pos = this.allocatedPos;
            UnsafeBuffer buffer = getChunk(pos);
            if (buffer != null)
                bufferOverhead = buffer.capacity() - inChunkPointer(pos);
        }

        int index = contentCount;
        int leadBit = getChunkIdx(index, CONTENTS_START_SHIFT, CONTENTS_START_SIZE);
        int ofs = inChunkPointer(index, leadBit, CONTENTS_START_SIZE);
        AtomicReferenceArray<T> contentArray = contentArrays[leadBit];
        int contentOverhead = ((contentArray != null ? contentArray.length() : 0) - ofs) * MemoryMeterStrategy.MEMORY_LAYOUT.getReferenceSize();

        return bufferOverhead + contentOverhead;
    }
}
