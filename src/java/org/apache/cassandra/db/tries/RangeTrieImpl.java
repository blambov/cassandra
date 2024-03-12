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
        M asActiveState(/*side*/); // TODO: For reverse iteration this should accept a side
        M asReportableStart();
        M asReportableEnd();

        boolean lesserIncluded();
    }

    interface Cursor<T extends RangeTrieImpl.RangeMarker<T>> extends TrieImpl.Cursor<T>
    {
        /**
         * Returns a range that is active at positions before the current.
         * This is useful to understand the range that is active at a position that was skipped to, when the
         * range trie jumps past the requested position or does not have content.
         * Note that this may also be non-null when the cursor is in an exhausted state, as well as immediately
         * after cursor construction, signifying, respectively, right and left unbounded ranges.
         */
        T state();

        /**
         * Content is only returned for positions where the ranges change, or singleton entries.
         */
        @Override
        default T content()
        {
            return content(state());
        }

        private T content(T value)
        {
            return (value == null) ? null : value.toContent();
        }

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
        public T state()
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


    // TODO: Range intersection must have set on the left side
    static <T extends RangeTrieImpl.RangeMarker<T>> RangeIntersectionCursor.IntersectionController<T, TrieSetImpl.RangeState, T> rangeAndSetIntersectionController()
    {
        return new RangeIntersectionCursor.IntersectionController<T, TrieSetImpl.RangeState, T>()
        {
            @Override
            public T combineState(T lState, TrieSetImpl.RangeState rState)
            {
                if (lState == null)
                    return null;

                switch (rState)
                {
                    case OUTSIDE_PREFIX:
                        return null;
                    case INSIDE_PREFIX:
                        return lState;
                    case END:
                        return lState.asReportableEnd();
                    case START:
                        return lState.asReportableStart();
                    default:
                        throw new AssertionError();
                }
            }

            @Override
            public T combineStateCoveringRight(T lState, TrieSetImpl.RangeState rCoveringState)
            {
                assert rCoveringState.lesserIncluded();
                return lState;
            }

            @Override
            public T combineStateCoveringLeft(TrieSetImpl.RangeState rState, T lCoveringState)
            {
                assert lCoveringState.lesserIncluded();
                switch (rState)
                {
                    case OUTSIDE_PREFIX:
                        return null;
                    case INSIDE_PREFIX:
                        return lCoveringState.asActiveState();
                    case END:
                        return lCoveringState.asActiveState().asReportableEnd();
                    case START:
                        return lCoveringState.asActiveState().asReportableStart();
                    default:
                        throw new AssertionError();
                }
            }
        };
    }

    static <T extends RangeTrieImpl.RangeMarker<T>> RangeTrieWithImpl<T> impl(RangeTrie<T> trieSet)
    {
        return (RangeTrieWithImpl<T>) trieSet;
    }
}
