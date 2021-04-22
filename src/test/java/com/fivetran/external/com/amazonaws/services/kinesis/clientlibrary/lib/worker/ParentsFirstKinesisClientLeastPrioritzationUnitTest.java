package com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import com.fivetran.external.com.amazonaws.services.kinesis.leases.impl.KinesisClientLease;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class ParentsFirstKinesisClientLeastPrioritzationUnitTest extends ParentsFirstShardPrioritizationUnitTest<KinesisClientLease> {

    @Override
    KinesisClientLease prioritizable(String shardId, List<String> parentShardIds) {
        return new KinesisClientLease(shardId, null, null, null, null, null, null, null, new HashSet<>(parentShardIds));
    }

    @Override
    KinesisClientLease prioritizable(String shardId, String... parentShardIds) {
        return prioritizable(shardId, Arrays.asList(parentShardIds));
    }
}
