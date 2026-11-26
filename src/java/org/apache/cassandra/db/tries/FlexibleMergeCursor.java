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

import java.util.function.BiFunction;

/// A [BasicFlexibleMergeCursor] of full cursors, which can also skip.
abstract class FlexibleMergeCursor<C extends Cursor<?>, D extends Cursor<?>, T> extends BasicFlexibleMergeCursor<C, D, T> implements Cursor<T>
{
    FlexibleMergeCursor(C c1)
    {
        super(c1);
    }

    FlexibleMergeCursor(C c1, D c2)
    {
        super(c1, c2);
    }

    @Override
    public long skipTo(long encodedSkipPosition)
    {
        if (state == State.C1_ONLY)
            return inC1Only(c1.skipTo(encodedSkipPosition));

        long c2encodedSkipPosition = DepthAdjustedCursor.fromAdjustedDepth(encodedSkipPosition, c2rootPosition, c2depthCorrection);
        // Handle request to exit c2 branch separately for simplicity
        if (Cursor.isExhausted(c2encodedSkipPosition))
        {
            switch (state)
            {
                case AT_C1:
                case AT_BOTH:
                    return leaveC2(c1.skipTo(encodedSkipPosition));
                case AT_C2:
                    return leaveC2(c1.skipToWhenAhead(encodedSkipPosition));
                default:
                    throw new AssertionError();
            }
        }

        switch (state)
        {
            case AT_C1:
                return checkOrder(c1.skipTo(encodedSkipPosition), c2.skipToWhenAhead(c2encodedSkipPosition));
            case AT_C2:
                return checkOrder(c1.skipToWhenAhead(encodedSkipPosition), c2.skipTo(c2encodedSkipPosition));
            case AT_BOTH:
                return checkOrder(c1.skipTo(encodedSkipPosition), c2.skipTo(c2encodedSkipPosition));
            default:
                throw new AssertionError();
        }
    }

    static abstract class WithMappedContent<T, U, C extends Cursor<T>, D extends Cursor<U>, Z> extends FlexibleMergeCursor<C, D, Z>
    {
        final BiFunction<T, U, Z> resolver;

        WithMappedContent(BiFunction<T, U, Z> resolver, C c1)
        {
            super(c1);
            this.resolver = resolver;
        }

        WithMappedContent(BiFunction<T, U, Z> resolver, C c1, D c2)
        {
            super(c1, c2);
            this.resolver = resolver;
        }

        @Override
        public Z content()
        {
            U mc = null;
            T nc = null;
            switch (state)
            {
                case C1_ONLY:
                case AT_C1:
                    nc = c1.content();
                    break;
                case AT_C2:
                    mc = c2.content();
                    break;
                case AT_BOTH:
                    mc = c2.content();
                    nc = c1.content();
                    break;
                default:
                    throw new AssertionError();
            }
            if (nc == null && mc == null)
                return null;
            return resolver.apply(nc, mc);
        }
    }
}
