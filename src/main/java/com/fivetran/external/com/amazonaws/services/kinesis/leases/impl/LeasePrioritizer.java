package com.fivetran.external.com.amazonaws.services.kinesis.leases.impl;

import com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker.ParentsFirstShardPrioritization;
import com.fivetran.external.com.amazonaws.services.kinesis.leases.interfaces.LeaseOrderer;

import java.util.List;

public class LeasePrioritizer implements LeaseOrderer<KinesisClientLease> {
    @Override
    public List<KinesisClientLease> order(List<KinesisClientLease> leases) {
        return new ParentsFirstShardPrioritization(Integer.MAX_VALUE).prioritize(leases);
    }
}
