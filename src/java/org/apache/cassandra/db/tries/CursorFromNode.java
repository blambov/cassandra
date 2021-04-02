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

public class CursorFromNode<T> implements Trie.Cursor<T>
{
    static final Trie.Node<Object, Trie.Node> EMPTY_NODE = new Trie.NoChildrenNode<Object, Trie.Node>(null)
    {
        public Object content()
        {
            return null;
        }
    };

    Trie.Node<T, Trie.Node> current;
    int level;

    CursorFromNode(Trie<T> trie)
    {
        current = trie.root();
        if (current == null)
            current = (Trie.Node<T, Trie.Node>) EMPTY_NODE;

        level = 0;
    }

    public int advance()
    {
        Trie.Remaining has = current.startIteration();
        Trie.Node<T, Trie.Node> child = null;
        do
        {
            while (has == null)
            {
                current = current.parentLink;
                --level;
                if (current == null)
                {
                    assert level == -1;
                    return level;
                }
                has = current.advanceIteration();
            }

            child = current.getCurrentChild(current);
            if (child == null)
                has = current.advanceIteration();
        }
        while (child == null);
        current = child;
        return ++level;
    }

    public int level()
    {
        return level;
    }

    public T content()
    {
        return current.content();
    }

    public int transitionAtLevel(int level)
    {
        int nodeLevel = this.level;
        Trie.Node<T, Trie.Node> node = current;
        while (nodeLevel > level)
        {
            node = node.parentLink;
            --nodeLevel;
        }
        return node.currentTransition;
    }
}
