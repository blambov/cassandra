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

import org.apache.cassandra.utils.AbstractIterator;

/**
 * Convertor of trie contents to flow.
 *
 * Note: the type argument L must be equal to {@code Trie.Node<T, L>}, but we can't define such a recursive type in
 * Java. Using {@code <>} when instantiating works, but any subclasses will also need to declare this useless type
 * argument.
 */
class TrieValuesIterator<T> extends AbstractIterator<T>
{
    private final Trie.Cursor<T> cursor;

    protected TrieValuesIterator(Trie<T> trie)
    {
        cursor = trie.cursor();
    }

    protected T computeNext()
    {
        T value = cursor.advanceToContent();
        if (value == null)
            return endOfData();
        else
            return value;
    }
}
