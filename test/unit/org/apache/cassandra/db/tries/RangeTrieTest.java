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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RangeTrieTest
{
    @Test
    public void testSpecified()
    {
        ByteComparable l = ByteComparable.fixedLength(new byte[] {0});
        boolean includeLeft = false;
        ByteComparable r = ByteComparable.fixedLength(new byte[] {0, 0, 0});
        boolean includeRight = true;

        TrieSet set = new RangeTrieSet(l, includeLeft, r, includeRight);
        System.out.println(String.format("Range %s%s,%s%s",
                                         includeLeft ? "[" : "(",
                                         l != null ? l.byteComparableAsString(ByteComparable.Version.OSS41) : null,
                                         r != null ? r.byteComparableAsString(ByteComparable.Version.OSS41) : null,
                                         includeRight ? "]" : ")"
        ));
        System.out.println(set.dump());


//        testSpecifiedRanges(new String[]{
//                            "\000\000"
//                            },
//                            new String[]{
//                            "\000",
////                            "test12",
//                            });
        testSpecifiedRanges(new String[]{
                            "test1",
                            "test2",
                            "test55",
                            "test123",
                            "test124",
                            "test12",
                            "test21",
                            "tease",
                            "sort",
                            "sorting",
                            "square",
                            "\777\000",
                            "\000\777",
                            "\000\000",
                            "\000\000\000",
                            "\000\000\777",
                            "\777\777"
                            },
                            new String[]{
                            "test1",
                            "test11",
                            "test12",
                            "test13",
                            "test2",
                            "test21",
                            "te",
                            "s",
                            "q",
                            "\000",
                            "\777",
                            "\777\000",
                            "\000\777",
                            "\000\000",
                            "\000\000\000",
                            "\000\000\777",
                            "\777\777"
                            });
    }

    public void testSpecifiedRanges(String[] keys, String[] boundaries)
    {
        testSpecifiedRanges(toByteComparable(keys),
                            toByteComparable(boundaries));
    }

    private ByteComparable[] toByteComparable(String[] keys)
    {
        return Arrays.stream(keys)
                     .map(x -> ByteComparable.fixedLength(x.getBytes(StandardCharsets.UTF_8)))
                     .toArray(ByteComparable[]::new);
    }

    public void testSpecifiedRanges(ByteComparable[] keys, ByteComparable[] boundaries)
    {
        Arrays.sort(boundaries, (a, b) -> ByteComparable.compare(a, b, ByteComparable.Version.OSS41));
        for (int li = -1; li < boundaries.length; ++li)
        {
            ByteComparable l = li < 0 ? null : boundaries[li];
            for (int ri = Math.max(0, li); ri <= boundaries.length; ++ri)
            {
                ByteComparable r = ri == boundaries.length ? null : boundaries[ri];

                for (int i = li == ri ? 3 : 0; i < 4; ++i)
                {
                    boolean includeLeft = (i & 1) != 0;
                    boolean includeRight = (i & 2) != 0;
                    TrieSet set = new RangeTrieSet(l, includeLeft, r, includeRight);

                    verifySetProperties(set);

                    for (ByteComparable key : keys)
                    {
                        int cmp1 = l != null ? ByteComparable.compare(key, l, ByteComparable.Version.OSS41) : 1;
                        int cmp2 = r != null ? ByteComparable.compare(r, key, ByteComparable.Version.OSS41) : 1;
                        Trie<Boolean> ix = new SetIntersectionTrie<Boolean>(Trie.singleton(key, true),
                                                                            set);
                        boolean expected = true;
                        if (cmp1 < 0 || cmp1 == 0 && !includeLeft)
                            expected = false;
                        if (cmp2 < 0 || cmp2 == 0 && !includeRight)
                            expected = false;
                        boolean actual = Iterables.getFirst(ix.values(), false);
                        if (expected != actual)
                        {
                            System.err.println("Range trie");
                            System.err.println(set.dump());
                            System.err.println("Intersection");
                            System.err.println(ix.dump());
                            Assert.fail(String.format("Failed on range %s%s,%s%s key %s expected %s got %s\n",
                                                      includeLeft ? "[" : "(",
                                                      l != null ? l.byteComparableAsString(ByteComparable.Version.OSS41) : null,
                                                      r != null ? r.byteComparableAsString(ByteComparable.Version.OSS41) : null,
                                                      includeRight ? "]" : ")",
                                                      key.byteComparableAsString(ByteComparable.Version.OSS41),
                                                      expected,
                                                      actual));
                        }
                    }
                }
            }
        }
    }

    private void verifySetProperties(TrieSet set)
    {
        try
        {
            Trie.Cursor<TrieSet.InSet> cursor = set.cursor();
            int level = cursor.advance();
            while (level > 0)
            {
                assertEquals(level, cursor.level());
                TrieSet.InSet inSet = cursor.content();
                int prevLevel = level;
                level = cursor.advance();
//                if (inSet == null)
//                    assertTrue("non-included nodes presented by a set must have children", level > prevLevel);
//                else
                if (inSet == TrieSet.InSet.BRANCH)
                    assertFalse("fully included branches presented by a set must not have children", level > prevLevel);
            }
        }
        catch (AssertionError e)
        {
            System.err.println(set.dump());
            throw e;
        }
    }
}
