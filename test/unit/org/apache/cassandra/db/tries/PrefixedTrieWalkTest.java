/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import java.util.Arrays;

import com.google.common.collect.Lists;
import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.junit.Assert.assertEquals;

/// Walks that do not track the path give no receiver to `advanceMultiple`, which must work for a prefix of any length.
/// This does not enable trie debugging, whose verification cursor always supplies a receiver.
public class PrefixedTrieWalkTest
{
    private static ByteComparable hex(String s)
    {
        return ByteComparable.preencoded(VERSION, ByteBufferUtil.hexToBytes(s));
    }

    @Test
    public void testValuesOfPrefixedTrie()
    {
        for (String prefix : new String[]{ "", "20", "203040" })
        {
            Trie<String> trie = Trie.singleton(hex("10"), VERSION, "v").prefixedBy(hex(prefix));
            assertEquals(prefix, Arrays.asList("v"), Lists.newArrayList(trie.values()));
            assertEquals(prefix, Arrays.asList("v"), Lists.newArrayList(trie.values(Direction.REVERSE)));
        }
    }
}
