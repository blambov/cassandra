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

import com.google.common.collect.Iterables;

/**
 * A merged view of two tries.
 */
class MergeTrie<T> extends Trie<T>
{
    /**
     * Transition value used to indicate a transition is not present. Must be greater than all valid transition values
     * (0-0xFF).
     */
    public static final int NOT_PRESENT = 0x100;

    private final MergeResolver<T> resolver;
    protected final Trie<T> t1;
    protected final Trie<T> t2;

    MergeTrie(MergeResolver<T> resolver, Trie<T> t1, Trie<T> t2)
    {
        this.resolver = resolver;
        this.t1 = t1;
        this.t2 = t2;
    }

    @Override
    protected Cursor<T> cursor()
    {
        return new MergeCursor<>(resolver, t1, t2);
    }

    static class MergeCursor<T> implements Cursor<T>
    {
        private final MergeResolver<T> resolver;
        private final Cursor<T> c1;
        private final Cursor<T> c2;

        boolean atC1;
        boolean atC2;

        MergeCursor(MergeResolver<T> resolver, Trie<T> t1, Trie<T> t2)
        {
            this.resolver = resolver;
            this.c1 = t1.cursor();
            this.c2 = t2.cursor();
            atC1 = atC2 = true;
        }

        @Override
        public int advance()
        {
            return checkOrder(atC1 ? c1.advance() : c1.level(),
                              atC2 ? c2.advance() : c2.level());
        }

        @Override
        public int ascend()
        {
            int c1level = c1.level();
            int c2level = c2.level();
            int level = Math.max(c1level, c2level);
            return checkOrder(c1level == level ? c1.ascend() : c1level,
                              c2level == level ? c2.ascend() : c2level);
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            if (atC1 & atC2)
                return advance();

            if (atC1)
            {
                int c2level = c2.level();
                int c1level = c1.advanceMultiple(receiver);
                if (c1level <= c2level)
                    return checkOrder(c1level, c2level);
                else
                    return c1level;   // atC1 stays true, atC2 false, c2 remains where it is
            }
            else // atC2
            {
                int c1level = c1.level();
                int c2level = c2.advanceMultiple(receiver);
                if (c2level <= c1level)
                    return checkOrder(c1level, c2level);
                else
                    return c2level;   // atC2 stays true, atC1 false, c1 remains where it is
            }
        }

        private int checkOrder(int c1level, int c2level)
        {
            if (c1level > c2level)
            {
                atC1 = true;
                atC2 = false;
                return c1level;
            }
            if (c1level < c2level)
            {
                atC1 = false;
                atC2 = true;
                return c2level;
            }
            int c1trans = c1.incomingTransition();
            int c2trans = c2.incomingTransition();
            atC1 = c1trans <= c2trans;
            atC2 = c1trans >= c2trans;
            assert atC1 | atC2;
            return c1level;
        }

        @Override
        public int level()
        {
            return atC1 ? c1.level() : c2.level();
        }

        @Override
        public int incomingTransition()
        {
            return atC1 ? c1.incomingTransition() : c2.incomingTransition();
        }

        public T content()
        {
            T mc = atC2 ? c2.content() : null;
            T nc = atC1 ? c1.content() : null;
            if (mc == null)
                return nc;
            else if (nc == null)
                return mc;
            else
                return resolver.resolve(nc, mc);
        }
    }

    /**
     * Special instance for sources that are guaranteed (by the caller) distinct. The main difference is that we can
     * form unordered value list by concatenating sources.
     */
    static class Distinct<T> extends MergeTrie<T>
    {
        Distinct(Trie<T> input1, Trie<T> input2)
        {
            super(throwingResolver(), input1, input2);
        }

        @Override
        public Iterable<T> valuesUnordered()
        {
            return Iterables.concat(t1.valuesUnordered(), t2.valuesUnordered());
        }
    }
}
