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

public interface RangeTrieImpl<M extends RangeTrieImpl.RangeMarker<M>> extends CursorWalkable<RangeTrieImpl.Cursor<M>>
{
    interface RangeMarker<M extends RangeMarker<M>>
    {
        M toContent();
        M leftSideAsCovering(/*side*/); // TODO: For reverse iteration this should accept a direction
        M rightSideAsCovering();  // TODO: combine with above when reversed iteration is done
        M asReportableStart(); // from covering state; TODO: direction parameter and combine with next
        M asReportableEnd();

        boolean lesserIncluded();
    }

    interface Cursor<M extends RangeTrieImpl.RangeMarker<M>> extends TrieImpl.Cursor<M>
    {
        /**
         * Returns a range that covers positions before this, including this position if content() is null.
         * This is the range that is active at (i.e. covers) a position that was skipped to, when the range trie jumps
         * past the requested position or does not have content.
         * Cannot be a reportable range (i.e. coveringState().toContent() must be null).
         * Note that this may also be non-null when the cursor is in an exhausted state, as well as immediately
         * after cursor construction, signifying, respectively, right and left unbounded ranges.
         */
        M coveringState();

        /**
         * Content is only returned for positions where the ranges change.
         * Note that if content() is non-null, coveringState() does not apply to this exact position.
         */
        @Override
        M content();

        @Override
        Cursor<M> duplicate();
    }


    /**
     * Process the trie using the given Walker.
     */
    default <R> R process(TrieImpl.Walker<M, R> walker)
    {
        return TrieImpl.process(walker, cursor());
    }

    class EmptyCursor<M extends RangeMarker<M>> extends TrieImpl.EmptyCursor<M> implements Cursor<M>
    {
        @Override
        public M coveringState()
        {
            return null;
        }

        @Override
        public M content()
        {
            return null;
        }

        @Override
        public Cursor<M> duplicate()
        {
            return depth == 0 ? new EmptyCursor<>() : this;
        }
    }

    @SuppressWarnings("unchecked")
    RangeTrieWithImpl EMPTY = EmptyCursor::new;


    static <M extends RangeTrieImpl.RangeMarker<M>> RangeIntersectionCursor.IntersectionController<TrieSetImpl.RangeState, M, M> rangeAndSetIntersectionController()
    {
        return new RangeIntersectionCursor.IntersectionController<TrieSetImpl.RangeState, M, M>()
        {
            @Override
            public M combineState(TrieSetImpl.RangeState lState, M rState)
            {
                if (rState == null)
                    return null;

                switch (lState)
                {
                    case OUTSIDE_PREFIX:
                        return null;
                    case INSIDE_PREFIX:
                        return rState;
                    case END:
                        return rState.asReportableEnd();
                    case START:
                        return rState.asReportableStart();
                    default:
                        throw new AssertionError();
                }
            }

            @Override
            public boolean includeLesserLeft(Cursor<TrieSetImpl.RangeState> cursor)
            {
                return cursor.coveringState().lesserIncluded();
            }

            @Override
            public M combineContentLeftAhead(Cursor<TrieSetImpl.RangeState> lCursor, Cursor<M> rCursor)
            {
                if (lCursor.coveringState().lesserIncluded())
                    return rCursor.content();
                else
                    return null;
            }
        };
    }

    static <M extends RangeTrieImpl.RangeMarker<M>> RangeTrieWithImpl<M> impl(RangeTrie<M> trieSet)
    {
        return (RangeTrieWithImpl<M>) trieSet;
    }
}
