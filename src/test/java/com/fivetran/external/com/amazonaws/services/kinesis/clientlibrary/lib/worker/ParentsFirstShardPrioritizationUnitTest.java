/*
 *  Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Amazon Software License (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */
package com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.*;

import org.junit.Test;

import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;

public abstract class ParentsFirstShardPrioritizationUnitTest<T extends PriorizeableShard> {

    @Test(expected = IllegalArgumentException.class)
    public void testMaxDepthNegativeShouldFail() {
        new ParentsFirstShardPrioritization(-1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMaxDepthZeroShouldFail() {
        new ParentsFirstShardPrioritization(0);
    }

    @Test
    public void testMaxDepthPositiveShouldNotFail() {
        new ParentsFirstShardPrioritization(1);
    }

    abstract T prioritizable(String shardId, List<String> parentShardIds);
    abstract T prioritizable(String shardId, String... parentShardIds);

    @Test
    public void testSorting() {
        Random random = new Random(987654);
        int numberOfShards = 7;

        List<String> shardIdsDependencies = new ArrayList<>();
        shardIdsDependencies.add("unknown");
        List<T> original = new ArrayList<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            original.add(prioritizable(shardId, shardIdsDependencies));
            shardIdsDependencies.add(shardId);
        }

        ParentsFirstShardPrioritization ordering = new ParentsFirstShardPrioritization(Integer.MAX_VALUE);

        // shuffle original list as it is already ordered in right way
        Collections.shuffle(original, random);
        List<T> ordered = ordering.prioritize(original);

        assertEquals(numberOfShards, ordered.size());
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            assertEquals(shardId, ordered.get(shardNumber).getShardId());
        }
    }

    @Test
    public void testSortingAndFiltering() {
        Random random = new Random(45677);
        int numberOfShards = 10;

        List<String> shardIdsDependencies = new ArrayList<>();
        shardIdsDependencies.add("unknown");
        List<T> original = new ArrayList<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            original.add(prioritizable(shardId, shardIdsDependencies));
            shardIdsDependencies.add(shardId);
        }

        int maxDepth = 3;
        ParentsFirstShardPrioritization ordering = new ParentsFirstShardPrioritization(maxDepth);

        // shuffle original list as it is already ordered in right way
        Collections.shuffle(original, random);
        List<T> ordered = ordering.prioritize(original);
        // in this case every shard has its own level, so we don't expect to
        // have more shards than max depth
        assertEquals(maxDepth, ordered.size());

        for (int shardNumber = 0; shardNumber < maxDepth; shardNumber++) {
            String shardId = shardId(shardNumber);
            assertEquals(shardId, ordered.get(shardNumber).getShardId());
        }
    }

    @Test
    public void testSimpleOrdering() {
        Random random = new Random(1234);
        int numberOfShards = 10;

        String parentId = "unknown";
        List<T> original = new ArrayList<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            original.add(prioritizable(shardId, parentId));
            parentId = shardId;
        }

        ParentsFirstShardPrioritization ordering = new ParentsFirstShardPrioritization(Integer.MAX_VALUE);

        // shuffle original list as it is already ordered in right way
        Collections.shuffle(original, random);
        List<T> ordered = ordering.prioritize(original);
        assertEquals(numberOfShards, ordered.size());
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            assertEquals(shardId, ordered.get(shardNumber).getShardId());
        }
    }

    /**
     * This should be impossible as shards don't have circular dependencies,
     * but this code should handle it properly and fail
     */
    @Test
    public void testCircularDependencyBetweenShards() {
        Random random = new Random(13468798);
        int numberOfShards = 10;

        // shard-0 will point in middle shard (shard-5) in current test
        String parentId = shardId(numberOfShards / 2);
        List<T> original = new ArrayList<>();
        for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
            String shardId = shardId(shardNumber);
            original.add(prioritizable(shardId, parentId));
            parentId = shardId;
        }

        ParentsFirstShardPrioritization ordering = new ParentsFirstShardPrioritization(Integer.MAX_VALUE);

        // shuffle original list as it is already ordered in right way
        Collections.shuffle(original, random);
        try {
            ordering.prioritize(original);
            fail("Processing should fail in case we have circular dependency");
        } catch (IllegalArgumentException expected) {

        }
    }

    private String shardId(int shardNumber) {
        return "shardId-" + shardNumber;
    }

    /**
     * Builder class for ShardInfo.
     */
    static class ShardInfoBuilder {
        private String shardId;
        private String concurrencyToken;
        private List<String> parentShardIds = Collections.emptyList();
        private ExtendedSequenceNumber checkpoint = ExtendedSequenceNumber.LATEST;

        ShardInfoBuilder() {
        }

        ShardInfoBuilder withShardId(String shardId) {
            this.shardId = shardId;
            return this;
        }

        ShardInfoBuilder withConcurrencyToken(String concurrencyToken) {
            this.concurrencyToken = concurrencyToken;
            return this;
        }

        ShardInfoBuilder withParentShards(List<String> parentShardIds) {
            this.parentShardIds = parentShardIds;
            return this;
        }

        ShardInfoBuilder withCheckpoint(ExtendedSequenceNumber checkpoint) {
            this.checkpoint = checkpoint;
            return this;
        }

        ShardInfo build() {
            return new ShardInfo(shardId, concurrencyToken, parentShardIds, checkpoint);
        }
    }

}
