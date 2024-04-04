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

import java.util.function.Function;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

/**
 *
 * @param <U> Common superclass of the deletable and the deletion marker.
 * @param <T>
 * @param <D> Must be a subtype of U.
 */
public class InMemoryDeletionAwareTrie<U extends DeletionAwareTrie.Deletable, T extends U, D extends DeletionAwareTrie.DeletionMarker<T, D>>
extends InMemoryTrie<U> implements DeletionAwareTrieWithImpl<T, D>
{
    public InMemoryDeletionAwareTrie(BufferType bufferType)
    {
        super(bufferType);
    }

    static class DeletionAwareCursor<U extends DeletionAwareTrie.Deletable, T extends U, D extends DeletionAwareTrie.DeletionMarker<T, D>>
    extends MemtableCursor<U> implements Cursor<T, D>
    {
        DeletionAwareCursor(InMemoryReadTrie<U> trie, int root, int depth, int incomingTransition)
        {
            super(trie, root, depth, incomingTransition);
        }

        DeletionAwareCursor(DeletionAwareCursor<U, T, D> copyFrom)
        {
            super(copyFrom);
        }

        @SuppressWarnings("unchecked")
        @Override
        public T content()
        {
            return (T) content;
        }

        @Override
        public RangeTrieImpl.Cursor<D> deletionBranch()
        {
            return isNull(alternateBranch)
                   ? null
                   : new InMemoryRangeTrie.RangeCursor<>(trie, alternateBranch, depth() - 1, incomingTransition());
        }

        @Override
        public DeletionAwareCursor<U, T, D> duplicate()
        {
            return new DeletionAwareCursor<>(this);
        }
    }

    @Override
    public Cursor<T, D> makeCursor()
    {
        return new DeletionAwareCursor<>(this, root, -1, -1);
    }

    private interface DeletionControl<T, D>
    {
        boolean isDeletionAttachmentPoint(T content);   // could generalise using sub-interfaces
        D resolve(D existingApplicableMarker, D newMarker);
        T delete(T content, D deletion);
    }



//    /**
//     * Modify this trie to apply the mutation given in the form of a trie. Any content in the mutation will be resolved
//     * with the given function before being placed in this trie (even if there's no pre-existing content in this trie).
//     * @param mutation the mutation to be applied, given in the form of a trie. Note that its content can be of type
//     * different than the element type for this memtable trie.
//     * @param transformer a function applied to the potentially pre-existing value for the given key, and the new
//     * value. Applied even if there's no pre-existing value in the memtable trie.
//     */
//    public <V extends Deletable, W extends V, E extends DeletionMarker<W, E>>
//    void apply(DeletionAwareTrie<W, E> mutation, final UpsertTransformer<U, V> transformer) throws SpaceExhaustedException
//    {
//    }
//
//    private <V extends Deletable, W extends V, E extends DeletionMarker<W, E>>
//    void apply(ApplyState state, DeletionAwareTrieImpl.Cursor<W, E> mutationCursor, final UpsertTransformer<U, V> transformer) throws SpaceExhaustedException
//    {
//    }


    public void deleteRecursive(ByteComparable key, D deletion, DeletionControl<T, D> control) throws SpaceExhaustedException
    {
        // TODO
        //   descend along key until isDeletionAttachmentPoint
        //   dupe key and switch to deletion branch
        //   descend along until end of key
        //   construct entry from new key and applicable content or coveringState
        //   put and walk back
        //   descend along main trie until end of key
        //   if entry found, delete and apply change back (removing nodes, returning NONE as necessary; no node type scale back but careful with two-child sparse)
        DeletionState deleter = new DeletionState(key, deletion, control);

        int newRoot = deleter.deleteRecursive(root);
        if (root != newRoot)
            root = newRoot;
    }

    class DeletionState
    {
        final D deletion;
        final DeletionControl<T, D> control;
        ByteSource key;
        // TODO: Could just as well put stack here, avoiding recursion stack overflow

        int deletionIndex;

        DeletionState(ByteComparable key, D deletion, DeletionControl<T, D> control)
        {
            this.key = key.asComparableBytes(TrieImpl.BYTE_COMPARABLE_VERSION);
            this.deletion = deletion;
            this.control = control;
        }


        int deleteRecursive(int node) throws SpaceExhaustedException
        {
            if (control.isDeletionAttachmentPoint((T) getContent(node)))
                return processAndAttachDeletion(node);

            int next = key.next();
            if (next == ByteSource.END_OF_STREAM)
                throw new IllegalArgumentException("Key exhausted without finding deletion attachment point");

            int child = getChild(node, next);
            int newChild = deleteRecursive(child);
            return completeUpdate(node, next, child, newChild);
        }

        private int processAndAttachDeletion(int node) throws SpaceExhaustedException
        {
            int mainBranch = node;
            int newMainBranch = node;
            int delBranch = NONE;
            if (offset(node) == PREFIX_OFFSET)
            {
                final int flags = getUnsignedByte(node + PREFIX_FLAGS_OFFSET);
                if ((flags & PREFIX_ALTERNATE_PATH_FLAG) != 0)
                {
                    mainBranch = getPrefixChild(node, flags);
                    delBranch = getInt(node + PREFIX_CONTENT_OFFSET);
                    assert getAlternateBranch(mainBranch) == NONE;
                }
            }

            if (mainBranch != NONE)
            {
                var duplicatable = ByteSource.duplicatable(key);
                // TODO: make sure putRecursive can delete/release nodes
                //  with or without downgrade (careful with single-transition sparse)
                //  need to check remaining transitions in split nodes for emptyness
                newMainBranch = putRecursive(mainBranch, duplicatable.duplicate(), deletion, (U existing, D deletion) -> control.delete((T) existing, deletion));
                key = duplicatable;
            }

            int newDelBranch = putDeletionRecursive(delBranch);

            if (newDelBranch != delBranch || newMainBranch != mainBranch)
            {
                if (mainBranch != node) // already have a prefix node
                {
                    if (newMainBranch != mainBranch)
                        node = updatePrefixNodeChild(node, newMainBranch);
                    putInt(node + PREFIX_CONTENT_OFFSET, newDelBranch);
                }
                else
                    node = createPrefixNode(newDelBranch, newMainBranch, false, PREFIX_ALTERNATE_PATH_FLAG);
            }
            return node;
        }


        private int putDeletionRecursive(int node) throws SpaceExhaustedException
        {
            // How do we get the covering deletion? Should we run a separate pass?
            // Put some content index and set value on the way back?
            int transition = key.next();
            if (transition == ByteSource.END_OF_STREAM)
            {
                if (isNull(node))
                {
                    deletionIndex = addContent((U) deletion);
                    return ~deletionIndex;
                }

                if (isLeaf(node))
                {
                    int contentIndex = ~node;
                    setContent(contentIndex, (U) control.resolve((D) getContent(contentIndex), deletion));
                    deletionIndex = -1;
                    return node;
                }

                if (offset(node) == PREFIX_OFFSET)
                {
                    int contentIndex = getInt(node + PREFIX_CONTENT_OFFSET);
                    setContent(contentIndex, (U) control.resolve((D) getContent(contentIndex), deletion));
                    deletionIndex = -1;
                    return node;
                }
                else
                {
                    // Deletion does not apply to children but we should have a covering state in first child.
                    U coveringState = getCoveringState(node);
                    int contentIndex;
                    if (coveringState != null)
                    {
                        contentIndex = addContent((U) control.resolve(((D) coveringState).leftSideAsCovering(), deletion));
                        deletionIndex = -1;
                    }
                    else
                        deletionIndex = contentIndex = addContent((U) deletion); // no luck, we still have to find covering deletion

                    return createContentPrefixNode(contentIndex, node, false);
                }
            }

            int child = getChild(node, transition);
            int newChild = putDeletionRecursive(child);
            if (deletionIndex >= 0 && !isChainNode(node))
            {
                // TODO: inefficient to create cursor every step of the way?
                U coveringState = getCoveringState(node, transition);
                if (coveringState != null)
                {
                    U resolved = (U) control.resolve(((D) coveringState).leftSideAsCovering(), deletion);
                    setContent(deletionIndex, resolved);
                    deletionIndex = -1;
                } // else try again one level up
            }
            return completeUpdate(node, transition, child, newChild);
        }

    }
    U getCoveringState(int node)
    {
        // Creating a cursor is the sanest way to walk the trie.
        var cursor = new InMemoryDTrie.DeterministicCursor<>(this, node, 0, 0);
        return (U) cursor.advanceToContent(null);
    }

    U getCoveringState(int node, int transition)
    {
        var cursor = new InMemoryDTrie.DeterministicCursor<>(this, node, 0, 0);
        if (cursor.skipTo(1, transition + 1) < 0)   // TODO make sure 256 here is okay
            return null;
        return (U) cursor.advanceToContent(null);
    }

    public void deleteRecursive(ByteComparable start, ByteComparable end, D deletion, DeletionControl<T, D> control)
    {
        // TODO
        //   use deletion.asReportableStart/End
    }

    /**
     * Override of dump to provide more detailed printout that includes the type of each node in the trie.
     * We do this via a wrapping cursor that returns a content string for the type of node for every node we return.
     */
    @SuppressWarnings("unchecked")
    @Override
    public String dump(Function<T, String> contentToString)
    {
        return dump(x -> contentToString.apply((T) x), root);
    }

    // TODO: Recursively put a deletion with pluggable way to identify attachment point (aim: partition boundary)
    // TODO: Apply deletion (content removal and deletion consolidation) during above operation
    // TODO: Singleton/range deletion cursor version of the above
    // TODO: Apply deletion-aware trie with content removal and deletion consolidation
}
