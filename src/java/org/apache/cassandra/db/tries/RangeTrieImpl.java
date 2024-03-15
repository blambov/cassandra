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

public interface RangeTrieImpl<T extends RangeTrieImpl.RangeMarker<T>> extends CursorWalkable<RangeTrieImpl.Cursor<T>>
{
    interface RangeMarker<M extends RangeMarker<M>>
    {
        M toContent();
        M leftSideAsCovering(/*side*/); // TODO: For reverse iteration this should accept a side
        M rightSideAsCovering();  // TODO: combine with above when reversed iteration is done
        M asReportableStart(); // from covering state
        M asReportableEnd();

        boolean lesserIncluded();
    }

    interface Cursor<T extends RangeTrieImpl.RangeMarker<T>> extends TrieImpl.Cursor<T>
    {
        /**
         * Returns a range that covers positions before this, including this position if content() is null.
         * This is the range that is active at (i.e. covers) a position that was skipped to, when the range trie jumps
         * past the requested position or does not have content.
         * Cannot be a reportable range (i.e. coveringState().toContent() must be null).
         * Note that this may also be non-null when the cursor is in an exhausted state, as well as immediately
         * after cursor construction, signifying, respectively, right and left unbounded ranges.
         */
        T coveringState();

        /**
         * Content is only returned for positions where the ranges change.
         * Note that if content() is non-null, coveringState() does not apply to this exact position.
         */
        @Override
        T content();

        @Override
        Cursor<T> duplicate();
    }


    /**
     * Process the trie using the given Walker.
     */
    default <R> R process(TrieImpl.Walker<T, R> walker)
    {
        return TrieImpl.process(walker, cursor());
    }

    class EmptyCursor<T extends RangeMarker<T>> extends TrieImpl.EmptyCursor<T> implements Cursor<T>
    {
        @Override
        public T coveringState()
        {
            return null;
        }

        @Override
        public T content()
        {
            return null;
        }

        @Override
        public Cursor<T> duplicate()
        {
            return depth == 0 ? new EmptyCursor<>() : this;
        }
    }

    @SuppressWarnings("unchecked")
    RangeTrieWithImpl EMPTY = EmptyCursor::new;


    static <T extends RangeTrieImpl.RangeMarker<T>> RangeIntersectionCursor.IntersectionController<TrieSetImpl.RangeState, T, T> rangeAndSetIntersectionController()
    {
        return new RangeIntersectionCursor.IntersectionController<TrieSetImpl.RangeState, T, T>()
        {
            @Override
            public T combineState(TrieSetImpl.RangeState lState, T rState)
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
            public T combineContentLeftAhead(Cursor<TrieSetImpl.RangeState> lCursor, Cursor<T> rCursor)
            {
                if (lCursor.coveringState().lesserIncluded())
                    return rCursor.content();
                else
                    return null;
            }
        };
    }

    static <T extends RangeTrieImpl.RangeMarker<T>> RangeTrieWithImpl<T> impl(RangeTrie<T> trieSet)
    {
        return (RangeTrieWithImpl<T>) trieSet;
    }
}
