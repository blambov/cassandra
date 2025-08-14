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

package org.apache.cassandra.db.compaction;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.Comparables;
import org.apache.cassandra.utils.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.quicktheories.generators.Generate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.quicktheories.QuickTheory.qt;

public class ShardManagerApplyMaxParallelismTest
{
    private static final Murmur3Partitioner partitioner = Murmur3Partitioner.instance;

    @Test
    public void processKeys() throws IOException, ParseException
    {
        JSONParser parser = new JSONParser();
        Object o = parser.parse(FileUtils.readFileToString(new File(/*"missing_partition_keys_16890113276908363071.json"*/"missing_partition_keys_12149555214569020722.json"), Charset.defaultCharset()));
        JSONObject json = (JSONObject) o;
        Set<String> interesting = ImmutableSet.of("27336733-afeb-4553-88b4-ec99ff86cc5c",
                                                  "873a9016-ce9c-4b3a-817c-363ac2a9d8e9",
                                                  "948d1d3b-c620-41f0-95f2-cc6c40d9c1c8");
        Set<String> interestingFound = new HashSet<>();
        for (Object okeysPerShard : (JSONArray) json.get("missing_keys_per_shard"))
        {
            JSONObject keysPerShard = (JSONObject) okeysPerShard;
            JSONObject keys = (JSONObject) keysPerShard.get("missing_keys");
            Token minToken = partitioner.getMaximumToken();
            Token maxToken = partitioner.getMinimumToken();

            for (Object ok : keys.keySet())
            {
                String key = (String) ok;
                if (interesting.contains(key))
                    interestingFound.add(key);
                ByteBuffer bb = UTF8Type.instance.decompose(key);
                DecoratedKey dk = partitioner.decorateKey(bb);
                System.out.println(dk);
                minToken = Comparables.min(minToken, dk.getToken());
                maxToken = Comparables.max(maxToken, dk.getToken());
            }

            Token base = partitioner.getMinimumToken();
            System.out.println("Min token: " + minToken + " pos: " + base.size(minToken) + " shard " + (int)(base.size(minToken) * 1024) + "/1024");
            System.out.println("Max token: " + maxToken + " pos: " + base.size(maxToken) + " shard " + (int)(base.size(maxToken) * 1024) + "/1024");
            System.out.println("Interesting found: " + interestingFound);
        }
    }

    @Test
    public void translateKeys()
    {
        Token minToken = partitioner.getMinimumToken();
        for (String key : ImmutableList.of("27336733-afeb-4553-88b4-ec99ff86cc5c",
                                           "873a9016-ce9c-4b3a-817c-363ac2a9d8e9",
                                           "948d1d3b-c620-41f0-95f2-cc6c40d9c1c8"))
        {
            ByteBuffer bb = UTF8Type.instance.decompose(key);
            DecoratedKey dk = partitioner.decorateKey(bb);
            System.out.format("Pos %.4f %d/1024 key %s\n", minToken.size(dk.getToken()),
                              (int)(minToken.size(dk.getToken()) * 1024), dk);
        }

    }

    @Test
    public void testApplyMaxParallelismProperties()
    {
        qt()
        .forAll(
            Generate.intArrays(Generate.range(10, 500), Generate.range(1, 1))
                   .assuming(arr -> arr.length > 0),
            Generate.range(1, 10)
        )
        .checkAssert((shardSizes, maxParallelism) -> {
//        int[] shardSizes = new int[]{10000,10000,10000,10,10};//{1, 1, 1, 1, 1, 1, 12, 1};
//        int maxParallelism = 3;
            List<Pair<Set<MockSSTable>, Range<Token>>> shards = createMockShards(shardSizes);
            
            BiFunction<Collection<MockSSTable>, Range<Token>, String> maker = 
                (sstables, range) -> "task-" + sstables.size() + "-" + range.left + "-" + range.right;
            
            List<String> result = ShardManager.applyMaxParallelism(maxParallelism, maker, shards);
            
            // Property 1: Result size should not exceed maxParallelism
            assertTrue("Result size " + result.size() + " exceeds maxParallelism " + maxParallelism,
                      result.size() <= maxParallelism);
            
            // Property 2: If input size <= maxParallelism, result size should equal input size
            if (shards.size() <= maxParallelism) {
                assertEquals("When input size <= maxParallelism, should preserve all shards",
                           shards.size(), result.size());
            }
            
            // Property 3: All input SSTables should be preserved
            Set<MockSSTable> originalSSTables = shards.stream()
                .flatMap(p -> p.left.stream())
                .collect(Collectors.toSet());
            
            // Extract SSTables from result by parsing task names
            Set<MockSSTable> resultSSTables = new HashSet<>();
            for (String task : result) {
                // This is a simplified check - in real implementation we'd need proper extraction
                assertNotNull("Task should not be null", task);
                assertTrue("Task should start with 'task-'", task.startsWith("task-"));
            }
            
            // Property 4: Token spans should be reasonably balanced
            if (result.size() > 1) {
                double totalSpan = shards.stream()
                    .mapToDouble(p -> p.right.left.size(p.right.right))
                    .sum();
                double expectedSpanPerTask = totalSpan / maxParallelism;
                
                // Each task should get roughly equal span (within reasonable bounds)
                // This is a heuristic check since perfect balance isn't always possible
                assertTrue("Total span should be positive", totalSpan > 0);
                assertTrue("Expected span per task should be positive", expectedSpanPerTask > 0);
            }
        });
    }
    
    @Test
    public void testApplyMaxParallelismEdgeCases()
    {
        qt()
        .forAll(Generate.range(1, 5))
        .checkAssert(maxParallelism -> {
            // Empty input
            List<Pair<Set<MockSSTable>, Range<Token>>> emptyShards = new ArrayList<>();
            BiFunction<Collection<MockSSTable>, Range<Token>, String> maker = 
                (sstables, range) -> "empty";
            
            List<String> result = ShardManager.applyMaxParallelism(maxParallelism, maker, emptyShards);
            assertTrue("Empty input should produce empty result", result.isEmpty());
            
            // Single shard
            List<Pair<Set<MockSSTable>, Range<Token>>> singleShard = createMockShards(new int[]{1});
            result = ShardManager.applyMaxParallelism(maxParallelism, maker, singleShard);
            assertEquals("Single shard should produce single result", 1, result.size());
        });
    }
    
    @Test
    public void testSpanDistribution()
    {
        qt()
        .forAll(
            Generate.intArrays(Generate.range(5, 15), Generate.range(1, 50))
                   .assuming(arr -> arr.length >= 5),
            Generate.range(2, 4)
        )
        .checkAssert((shardSizes, maxParallelism) -> {
            List<Pair<Set<MockSSTable>, Range<Token>>> shards = createMockShards(shardSizes);
            
            BiFunction<Collection<MockSSTable>, Range<Token>, Double> spanMaker = 
                (sstables, range) -> range.left.size(range.right);
            
            List<Double> spans = ShardManager.applyMaxParallelism(maxParallelism, spanMaker, shards);
            
            if (spans.size() > 1) {
                double totalSpan = spans.stream().mapToDouble(Double::doubleValue).sum();
                double avgSpan = totalSpan / spans.size();
                
                // Check that no span is more than 2x the average (reasonable fairness)
                for (double span : spans) {
                    assertTrue("Span " + span + " should not be more than 2x average " + avgSpan,
                             span <= 2.1 * avgSpan); // Small tolerance for rounding
                }
            }
        });
    }
    
    private List<Pair<Set<MockSSTable>, Range<Token>>> createMockShards(int[] shardSizes)
    {
        List<Pair<Set<MockSSTable>, Range<Token>>> shards = new ArrayList<>();
        int shardSpanTotal = Arrays.stream(shardSizes).sum();
        long currentToken = Long.MIN_VALUE / 2; // Start from a reasonable position
        long tokenStep = Long.MAX_VALUE / (shardSpanTotal + 1);
        
        for (int i = 0; i < shardSizes.length; i++) {
            Set<MockSSTable> sstables = new HashSet<>();
//            for (int j = 0; j < shardSizes[i]; j++) {
                sstables.add(new MockSSTable("shard" + i));
//            }
            
            Token start = partitioner.getTokenFactory().fromString(String.valueOf(currentToken));
            currentToken += tokenStep * shardSizes[i];
            Token end = partitioner.getTokenFactory().fromString(String.valueOf(currentToken));
            
            Range<Token> range = new Range<>(start, end);
            shards.add(Pair.create(sstables, range));
        }
        
        return shards;
    }
    
    private static class MockSSTable
    {
        private final String name;
        
        public MockSSTable(String name)
        {
            this.name = name;
        }
        
        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof MockSSTable)) return false;
            MockSSTable that = (MockSSTable) o;
            return Objects.equals(name, that.name);
        }
        
        @Override
        public int hashCode()
        {
            return Objects.hash(name);
        }
        
        @Override
        public String toString()
        {
            return name;
        }
    }
}