package com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParentsFirstShardInfoPrioritzationUnitTest extends ParentsFirstShardPrioritizationUnitTest<ShardInfo> {
    @Override
    ShardInfo prioritizable(String shardId, List<String> parentShardIds) {
        // copy into new list just in case ShardInfo will stop doing it
        List<String> newParentShardIds = new ArrayList<>(parentShardIds);
        return new ShardInfoBuilder()
                .withShardId(shardId)
                .withParentShards(newParentShardIds)
                .build();
    }

    @Override
    ShardInfo prioritizable(String shardId, String... parentShardIds) {
        return new ShardInfoBuilder()
                .withShardId(shardId)
                .withParentShards(Arrays.asList(parentShardIds))
                .build();
    }
}
