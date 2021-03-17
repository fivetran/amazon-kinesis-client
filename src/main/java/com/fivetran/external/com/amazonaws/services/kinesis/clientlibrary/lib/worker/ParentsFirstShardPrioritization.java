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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shard Prioritization that prioritizes parent shards first.
 * It also limits number of shards that will be available for initialization based on their depth.
 * It doesn't make a lot of sense to work on a shard that has too many unfinished parents.
 */
public class ParentsFirstShardPrioritization implements
        ShardPrioritization {
    private static final SortingNode PROCESSING_NODE = new SortingNode(null, Integer.MIN_VALUE);

    private final int maxDepth;

    /**
     * Creates ParentFirst prioritization with filtering based on depth of the shard.
     * Shards that have depth > maxDepth will be ignored and will not be returned by this prioritization.
     * 
     * @param maxDepth any shard that is deeper than max depth, will be excluded from processing
     */
    public ParentsFirstShardPrioritization(int maxDepth) {
        /* Depth 0 means that shard is completed or cannot be found,
        * it is impossible to process such shards.
        */
        if (maxDepth <= 0) {
            throw new IllegalArgumentException("Max depth cannot be negative or zero. Provided value: " + maxDepth);
        }
        this.maxDepth = maxDepth;
    }

    @Override
    public <T extends PriorizeableShard> List<T> prioritize(List<T> original) {
        Map<String, T> shards = new HashMap<>();
        for (T shardInfo : original) {
            shards.put(shardInfo.getShardId(),
                    shardInfo);
        }

        Map<String, SortingNode<T>> processedNodes = new HashMap<>();

        for (T shardInfo : original) {
            populateDepth(shardInfo.getShardId(),
                    shards,
                    processedNodes);
        }

        List<T> orderedInfos = new ArrayList<>(original.size());

        List<SortingNode> orderedNodes = new ArrayList<>(processedNodes.values());
        Collections.sort(orderedNodes);

        for (SortingNode sortingTreeNode : orderedNodes) {
            // don't process shards with depth > maxDepth
            if (sortingTreeNode.getDepth() <= maxDepth) {
                orderedInfos.add((T) sortingTreeNode.shardInfo);
            }
        }
        return orderedInfos;
    }

    private <T extends PriorizeableShard> int populateDepth(String shardId,
            Map<String, T> shards,
            Map<String, SortingNode<T>> processedNodes) {
        SortingNode processed = processedNodes.get(shardId);
        if (processed != null) {
            if (processed == PROCESSING_NODE) {
                throw new IllegalArgumentException("Circular dependency detected. Shard Id "
                    + shardId + " is processed twice");
            }
            return processed.getDepth();
        }

        T shardInfo = shards.get(shardId);
        if (shardInfo == null) {
            // parent doesn't exist in our list, so this shard is root-level node
            return 0;
        }

        if (shardInfo.isCompleted()) {
            // we treat completed shards as 0-level
            return 0;
        }

        // storing processing node to make sure we track progress and avoid circular dependencies
        processedNodes.put(shardId, PROCESSING_NODE);

        int maxParentDepth = 0;
        for (String parentId : shardInfo.getParentShardIds()) {
            maxParentDepth = Math.max(maxParentDepth,
                    populateDepth(parentId,
                            shards,
                            processedNodes));
        }

        int currentNodeLevel = maxParentDepth + 1;
        SortingNode previousValue = processedNodes.put(shardId,
                new SortingNode(shardInfo,
                        currentNodeLevel));
        if (previousValue != PROCESSING_NODE) {
            throw new IllegalStateException("Validation failed. Depth for shardId " + shardId + " was populated twice");
        }

        return currentNodeLevel;
    }

    /**
     * Class to store depth of shards during prioritization.
     */
    private static class SortingNode<T extends PriorizeableShard> implements
            Comparable<SortingNode> {
        private final T shardInfo;
        private final int depth;

        public SortingNode(T shardInfo,
                int depth) {
            this.shardInfo = shardInfo;
            this.depth = depth;
        }

        public int getDepth() {
            return depth;
        }

        @Override
        public int compareTo(SortingNode o) {
            return Integer.compare(depth,
                    o.depth);
        }
    }
}
