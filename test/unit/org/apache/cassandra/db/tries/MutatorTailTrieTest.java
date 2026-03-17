/*
 * Copyright DataStax, Inc.
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.TrieUtil.VERSION;
import static org.apache.cassandra.db.tries.TrieUtil.directComparable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class MutatorTailTrieTest
{
    @BeforeClass
    public static void disableVerification()
    {
        CassandraRelevantProperties.TRIE_DEBUG.setBoolean(false);
    }

    private static final InMemoryBaseTrie.UpsertTransformer<String, String> DATA_TRANSFORMER =
    new InMemoryBaseTrie.UpsertTransformer<String, String>()
    {
        public String apply(String e, String u)
        {
            return u;
        }
    };

    private static final InMemoryBaseTrie.UpsertTransformer<TestRangeState, TestRangeState> DELETION_TRANSFORMER =
    new InMemoryBaseTrie.UpsertTransformer<TestRangeState, TestRangeState>()
    {
        public TestRangeState apply(TestRangeState e, TestRangeState u)
        {
            return u;
        }
    };

    private static final InMemoryBaseTrie.UpsertTransformer<String, TestRangeState> EXISTING_DELETER =
    new InMemoryBaseTrie.UpsertTransformer<String, TestRangeState>()
    {
        public String apply(String e, TestRangeState u)
        {
            return e;
        }
    };

    private static final BiFunction<TestRangeState, String, String> INSERTED_DELETER =
    new BiFunction<TestRangeState, String, String>()
    {
        public String apply(TestRangeState d, String v)
        {
            return v;
        }
    };

    public enum Strategy
    {
        SHORT_LIVED
        {
            @Override
            <T, D extends RangeState<D>> InMemoryDeletionAwareTrie<T, D> create()
            {
                return InMemoryDeletionAwareTrie.shortLived(VERSION);
            }
        },
        LONG_LIVED
        {
            @Override
            <T, D extends RangeState<D>> InMemoryDeletionAwareTrie<T, D> create()
            {
                return InMemoryDeletionAwareTrie.longLived(VERSION, BufferType.OFF_HEAP, null);
            }
        };

        abstract <T, D extends RangeState<D>> InMemoryDeletionAwareTrie<T, D> create();
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> generateData()
    {
        List<Object[]> list = new ArrayList<>();
        for (Strategy s : Strategy.values())
            list.add(new Object[]{ s });
        return list;
    }

    @Parameterized.Parameter(0)
    public Strategy strategy;

    private InMemoryDeletionAwareTrie<String, TestRangeState> newTrie()
    {
        return strategy.create();
    }

    private static void insertData(InMemoryDeletionAwareTrie<String, TestRangeState> trie,
                                   ByteComparable key,
                                   String value) throws TrieSpaceExhaustedException
    {
        DeletionAwareTrie<String, TestRangeState> mutation =
        DeletionAwareTrie.<String, TestRangeState>singleton(key, VERSION, value);
        trie.apply(mutation, DATA_TRANSFORMER, DELETION_TRANSFORMER, EXISTING_DELETER, INSERTED_DELETER, false,
                   new Predicate<InMemoryBaseTrie.NodeFeatures<?>>()
                   {
                       public boolean test(InMemoryBaseTrie.NodeFeatures<?> f)
                       {
                           return false;
                       }
                   });
    }

    private static void insertDeletion(InMemoryDeletionAwareTrie<String, TestRangeState> trie,
                                       ByteComparable root,
                                       ByteComparable start,
                                       ByteComparable end,
                                       int value) throws TrieSpaceExhaustedException
    {
        DeletionAwareTrie<String, TestRangeState> mutation =
        DeletionAwareTrie.<String, TestRangeState>deletedRange(root, start, end, VERSION, TestRangeState.covering(value));
        trie.apply(mutation, DATA_TRANSFORMER, DELETION_TRANSFORMER, EXISTING_DELETER, INSERTED_DELETER, false,
                   new Predicate<InMemoryBaseTrie.NodeFeatures<?>>()
                   {
                       public boolean test(InMemoryBaseTrie.NodeFeatures<?> f)
                       {
                           return false;
                       }
                   });
    }

    /**
     * Runs an apply and captures the tail tries at the moment the mutation reaches the given key.
     * Fails if the key is never reached during the apply.
     */
    private static TailCapture captureAt(final InMemoryDeletionAwareTrie<String, TestRangeState> trie,
                                         final DeletionAwareTrie<String, TestRangeState> mutation,
                                         final String captureAtKey) throws TrieSpaceExhaustedException
    {
        final TailCapture capture = new TailCapture();

        trie.apply(mutation, DATA_TRANSFORMER, DELETION_TRANSFORMER, EXISTING_DELETER, INSERTED_DELETER, false,
                   new Predicate<InMemoryBaseTrie.NodeFeatures<?>>()
                   {
                       @SuppressWarnings("unchecked")
                       public boolean test(InMemoryBaseTrie.NodeFeatures<?> features)
                       {
                           if (!(features instanceof InMemoryDeletionAwareTrie.Mutator))
                               return false;

                           InMemoryDeletionAwareTrie<String, TestRangeState>.Mutator<String, TestRangeState> mutator =
                           (InMemoryDeletionAwareTrie<String, TestRangeState>.Mutator<String, TestRangeState>) features;

                           if (!captureAtKey.equals(new String(mutator.getCurrentKeyBytes())))
                               return false;

                           capture.existingData = materialiseData(mutator.getExistingTailTrie());
                           capture.mutationData = materialiseData(mutator.getMutationTailTrie());
                           // getExistingDeletionTailTrie() always returns a non-null lambda;
                           // it only fails when evaluated if no deletion branch exists at this position.
                           try
                           {
                               capture.existingDeletion = materialiseDeletion(mutator.getExistingDeletionTailTrie());
                           }
                           catch (Exception e)
                           {
                               capture.existingDeletion = null;
                           }

                           try
                           {
                               capture.mutationDeletion = materialiseDeletion(mutator.getMutationDeletionTailTrie());
                           }
                           catch (Exception e)
                           {
                               capture.mutationDeletion = null;
                           }
                           capture.fired = true;
                           return false;
                       }
                   });

        assertTrue("Mutation never reached key '" + captureAtKey + "'", capture.fired);
        return capture;
    }

    private static Map<String, String> materialiseData(DeletionAwareTrie<String, TestRangeState> trie)
    {
        final Map<String, String> result = new LinkedHashMap<>();
        trie.forEachEntry(new BiConsumer<ByteComparable.Preencoded, String>()
        {
            public void accept(ByteComparable.Preencoded key, String value)
            {
                result.put(key.byteComparableAsString(VERSION), value);
            }
        });
        return result;
    }

    private static Map<String, Integer> materialiseDeletion(RangeTrie<TestRangeState> trie)
    {
        final Map<String, Integer> result = new LinkedHashMap<>();
        trie.forEachEntry(new BiConsumer<ByteComparable.Preencoded, TestRangeState>()
        {
            public void accept(ByteComparable.Preencoded key, TestRangeState value)
            {
                result.put(key.byteComparableAsString(VERSION), value.rightSide);
            }
        });
        return result;
    }

    static class TailCapture
    {
        boolean fired = false;
        Map<String, String> existingData;
        Map<String, String> mutationData;
        Map<String, Integer> existingDeletion; // null if no deletion branch existed
        Map<String, Integer> mutationDeletion; // null if no deletion branch in mutation
    }

    /**
     * At the root position (EMPTY key), existing tail must contain all data in the trie,
     * and mutation tail must contain all data in the mutation.
     */
    @Test
    public void testExistingAndMutationTailAtRoot() throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = newTrie();
        insertData(trie, ByteComparable.EMPTY, "root");
        insertData(trie, directComparable("a"), "v_a");
        insertData(trie, directComparable("b"), "v_b");

        DeletionAwareTrie<String, TestRangeState> mutation =
        DeletionAwareTrie.<String, TestRangeState>singleton(directComparable("c"), VERSION, "m_c");

        TailCapture capture = captureAt(trie, mutation, "");

        assertEquals("root", capture.existingData.get(""));
        assertEquals("v_a", capture.existingData.get(directComparable("a").byteComparableAsString(VERSION)));
        assertEquals("v_b", capture.existingData.get(directComparable("b").byteComparableAsString(VERSION)));
        assertEquals(3, capture.existingData.size());

        assertEquals("m_c", capture.mutationData.get(directComparable("c").byteComparableAsString(VERSION)));
        assertEquals(1, capture.mutationData.size());
    }

    /**
     * At an intermediate node, existing tail must contain only the subtrie rooted there,
     * with paths relative to that node.
     */
    @Test
    public void testExistingTailIsRelativeToCurrentPosition() throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = newTrie();
        insertData(trie, directComparable("a"), "v_a");
        insertData(trie, directComparable("abc"), "v_abc");
        insertData(trie, directComparable("z"), "v_z");   // must NOT appear in tail at "a"

        DeletionAwareTrie<String, TestRangeState> mutation =
        DeletionAwareTrie.<String, TestRangeState>singleton(directComparable("ab"), VERSION, "m_ab");

        TailCapture capture = captureAt(trie, mutation, "a");

        // Paths in the tail are relative to "a", so "a" -> "" and "abc" -> "bc"
        assertEquals("v_a", capture.existingData.get(""));
        assertEquals("v_abc", capture.existingData.get(directComparable("bc").byteComparableAsString(VERSION)));
        assertEquals(2, capture.existingData.size());
        assertFalse(capture.existingData.containsValue("v_z"));
    }

    /**
     * Mutation tail at a given position must contain only the mutation's subtrie rooted there.
     */
    @Test
    public void testMutationTailIsRelativeToCurrentPosition() throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = newTrie();
        insertData(trie, directComparable("a"), "v_a");

        // mergeDistinct is correct here - we are merging non-overlapping tries
        DeletionAwareTrie<String, TestRangeState> mutation = DeletionAwareTrie.mergeDistinct(Arrays.asList(
        DeletionAwareTrie.<String, TestRangeState>singleton(directComparable("ab"), VERSION, "m_ab"),
        DeletionAwareTrie.<String, TestRangeState>singleton(directComparable("abc"), VERSION, "m_abc"),
        DeletionAwareTrie.<String, TestRangeState>singleton(directComparable("z"), VERSION, "m_z")
        ));

        TailCapture capture = captureAt(trie, mutation, "a");

        assertEquals("m_ab", capture.mutationData.get(directComparable("b").byteComparableAsString(VERSION)));
        assertEquals("m_abc", capture.mutationData.get(directComparable("bc").byteComparableAsString(VERSION)));
        assertEquals(2, capture.mutationData.size());
        assertFalse(capture.mutationData.containsValue("m_z"));
    }

    /**
     * When a deletion branch exists in the trie at a given position, getExistingDeletionTailTrie must
     * return it with correct range boundaries relative to that position.
     */
    @Test
    public void testExistingDeletionTailAtPosition() throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = newTrie();
        insertDeletion(trie, directComparable("ab"),
                       directComparable("c"), directComparable("d"), 100);

        DeletionAwareTrie<String, TestRangeState> mutation =
        DeletionAwareTrie.<String, TestRangeState>singleton(directComparable("ab"), VERSION, "m_ab");

        TailCapture capture = captureAt(trie, mutation, "ab");

        assertNotNull("Expected deletion branch at 'ab'", capture.existingDeletion);
        assertEquals(Integer.valueOf(100),
                     capture.existingDeletion.get(directComparable("c").byteComparableAsString(VERSION)));
    }

    /**
     * When the mutation carries a deletion branch, getMutationDeletionTailTrie must return it.
     */
    @Test
    public void testMutationDeletionTailAtPosition() throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = newTrie();
        insertData(trie, directComparable("ab"), "v_ab");

        DeletionAwareTrie<String, TestRangeState> mutation =
        DeletionAwareTrie.<String, TestRangeState>deletedRange(directComparable("ab"),
                                                               directComparable("e"), directComparable("f"),
                                                               VERSION, TestRangeState.covering(200));

        TailCapture capture = captureAt(trie, mutation, "ab");

        assertNotNull("Expected deletion branch in mutation at 'ab'", capture.mutationDeletion);
        assertEquals(Integer.valueOf(200),
                     capture.mutationDeletion.get(directComparable("e").byteComparableAsString(VERSION)));
    }

    /**
     * When there is no deletion branch in the existing trie at the capture position,
     * getExistingDeletionTailTrie must return null.
     */
    @Test
    public void testExistingDeletionTailIsNullWhenNoBranch() throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = newTrie();
        insertData(trie, directComparable("a"), "v_a");

        DeletionAwareTrie<String, TestRangeState> mutation =
        DeletionAwareTrie.<String, TestRangeState>singleton(directComparable("a"), VERSION, "m_a");

        TailCapture capture = captureAt(trie, mutation, "a");

        assertNull("No deletion branch should exist at 'a'", capture.existingDeletion);
    }

    /**
     * Existing tail over a long chain must contain only descendants relative to the capture point,
     * not siblings or ancestors.
     */
    @Test
    public void testExistingTailOverDeepChain() throws TrieSpaceExhaustedException
    {
        InMemoryDeletionAwareTrie<String, TestRangeState> trie = newTrie();
        insertData(trie, directComparable("abcdefgh"), "deep");

        DeletionAwareTrie<String, TestRangeState> mutation =
        DeletionAwareTrie.<String, TestRangeState>singleton(directComparable("abc"), VERSION, "m_abc");

        TailCapture capture = captureAt(trie, mutation, "abc");

        // relative path from "abc" to "abcdefgh" is "defgh"
        assertEquals("deep", capture.existingData.get(directComparable("defgh").byteComparableAsString(VERSION)));
        assertEquals(1, capture.existingData.size());
    }
}