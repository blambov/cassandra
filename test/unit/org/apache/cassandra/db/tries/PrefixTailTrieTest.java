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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.function.Function;

import com.google.common.base.Predicates;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.InMemoryTrieTestBase.addToInMemoryDTrie;
import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.assertMapEquals;
import static org.apache.cassandra.db.tries.TrieUtil.generateKey;
import static org.apache.cassandra.db.tries.TrieUtil.generateKeys;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class PrefixTailTrieTest
{
    private static final int COUNT_TAIL = 5000;
    private static final int COUNT_HEAD = 40;
    Random rand = new Random();

    static final InMemoryTrie.UpsertTransformer<Object, Object> THROWING_UPSERT = (e, u) -> {
        if (e != null) throw new AssertionError();
        return u;
    };

    static final Function<Object, String> CONTENT_TO_STRING = x -> x instanceof ByteBuffer
                                                                   ? ByteBufferUtil.bytesToHex((ByteBuffer) x)
                                                                   : x.toString();

    static class Tail
    {
        byte[] prefix;
        Map<ByteComparable, ByteBuffer> data;

        public String toString()
        {
            return "Tail{" + ByteBufferUtil.bytesToHex(ByteBuffer.wrap(prefix)) + '}';
        }
    }

    static <T> T getRootContent(Trie<T> trie)
    {
        return TrieImpl.impl(trie).cursor(Direction.FORWARD).content();
    }

    @Test
    public void testPrefixTail() throws Exception
    {
        ByteComparable[] prefixes = generateKeys(rand, COUNT_HEAD);

        Map<ByteBuffer, Tail> data = new LinkedHashMap<>();
        InMemoryDTrie<Object> trie = new InMemoryDTrie<>(BufferType.ON_HEAP);
        for (int i = 0; i < COUNT_HEAD; ++i)
        {
            Tail t = new Tail();
            ByteComparable[] src = generateKeys(rand, COUNT_TAIL);
            NavigableMap<ByteComparable, ByteBuffer> content = new TreeMap<>((a, b) -> ByteComparable.compare(a, b, VERSION));
            InMemoryDTrie<Object> tail = new InMemoryDTrie<>(BufferType.ON_HEAP);
            addToInMemoryDTrie(src, content, tail, true);
            t.data = content;
            t.prefix = prefixes[i].asArray(VERSION);
            tail.putRecursive(ByteComparable.EMPTY, t, THROWING_UPSERT);
//            System.out.println(tail.dump(CONTENT_TO_STRING));
            trie.apply(tail.prefix(prefixes[i]), THROWING_UPSERT);
            data.put(ByteBuffer.wrap(t.prefix), t);
        }

//        System.out.println(trie.dump(CONTENT_TO_STRING));

        // Test tailTrie for known prefix
        for (int i = 0; i < COUNT_HEAD; ++i)
        {
            Tail t = data.get(ByteBuffer.wrap(prefixes[i].asArray(VERSION)));
            Trie<Object> tail = trie.tailTrie(prefixes[i]);
            assertEquals(t, getRootContent(tail));
            assertMapEquals(tail.filteredEntryIterator(Direction.FORWARD, ByteBuffer.class),
                            t.data.entrySet().iterator());
        }

        // Test tail iteration for metadata
        long count = 0;
        for (var en : trie.tailTries(Predicates.instanceOf(Tail.class), Direction.FORWARD))
        {
            System.out.println(en.getKey().byteComparableAsString(VERSION));
            Trie<Object> tail = en.getValue();
            Tail t = data.get(ByteBuffer.wrap(en.getKey().asArray(VERSION)));
            assertNotNull(t);
            assertEquals(t, getRootContent(tail));
            assertMapEquals(tail.filteredEntryIterator(Direction.FORWARD, ByteBuffer.class),
                            t.data.entrySet().iterator());
            ++count;
        }
        assertEquals(COUNT_HEAD, count);
    }

    // also do same prefix updates

    @Test
    public void testTailMerge() throws Exception
    {
        ByteComparable prefix = generateKey(rand);
        InMemoryDTrie<Object> trie = new InMemoryDTrie<>(BufferType.ON_HEAP);
        NavigableMap<ByteComparable, ByteBuffer> content = new TreeMap<>((a, b) -> ByteComparable.compare(a, b, VERSION));

        for (int i = 0; i < COUNT_HEAD; ++i)
        {
            ByteComparable[] src = generateKeys(rand, COUNT_TAIL);
            InMemoryDTrie<Object> tail = new InMemoryDTrie<>(BufferType.ON_HEAP);
            addToInMemoryDTrie(src, content, tail, true);
//                        System.out.println(tail.dump(CONTENT_TO_STRING));
            tail.putRecursive(ByteComparable.EMPTY, 1, THROWING_UPSERT);
            trie.apply(tail.prefix(prefix), (x, y) -> x instanceof Integer ? (Integer) x + (Integer) y : y);
        }

//                System.out.println(trie.dump(CONTENT_TO_STRING));

        Trie<Object> tail = trie.tailTrie(prefix);
        assertEquals(COUNT_HEAD, ((Integer) getRootContent(tail)).intValue());
        assertMapEquals(tail.filteredEntryIterator(Direction.FORWARD, ByteBuffer.class),
                        content.entrySet().iterator());


        // Test tail iteration for metadata
        long count = 0;
        for (var en : trie.tailTries(Predicates.instanceOf(Integer.class), Direction.FORWARD))
        {
            System.out.println(en.getKey().byteComparableAsString(VERSION));
            Trie<Object> tt = en.getValue();
            assertNotNull(tt);
            assertEquals(COUNT_HEAD, ((Integer) getRootContent(tail)).intValue());
            assertMapEquals(tt.filteredEntryIterator(Direction.FORWARD, ByteBuffer.class),
                            content.entrySet().iterator());
            ++count;
        }
        assertEquals(1, count);
    }
}
