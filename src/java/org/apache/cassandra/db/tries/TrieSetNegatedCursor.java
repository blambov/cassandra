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

/**
 * Negation of trie sets.
 * <p>
 * Achieved by simply inverting the state() values.
 */
public class TrieSetNegatedCursor implements TrieSetImpl.Cursor
{
    final TrieSetImpl.Cursor source;

    TrieSetNegatedCursor(TrieSetImpl.Cursor source)
    {
        this.source = source;
    }

    @Override
    public int depth()
    {
        return source.depth();
    }

    @Override
    public int incomingTransition()
    {
        return source.incomingTransition();
    }

    @Override
    public Direction direction()
    {
        return source.direction();
    }

    @Override
    public TrieSetImpl.RangeState state()
    {
        return TrieSetImpl.RangeState.values()[source.state().ordinal() ^ 3];
    }

    @Override
    public int advance()
    {
        return source.advance();
    }

    @Override
    public int skipTo(int skipDepth, int skipTransition)
    {
        return source.skipTo(skipDepth, skipTransition);
    }

    @Override
    public int advanceMultiple(CursorWalkable.TransitionsReceiver receiver)
    {
        return source.advanceMultiple(receiver);
    }

    @Override
    public TrieSetImpl.Cursor duplicate()
    {
        return new TrieSetNegatedCursor(source.duplicate());
    }

    @Override
    public TrieSetImpl.Cursor tailCursor(Direction direction)
    {
        return new TrieSetNegatedCursor(source.tailCursor(direction));
    }
}
