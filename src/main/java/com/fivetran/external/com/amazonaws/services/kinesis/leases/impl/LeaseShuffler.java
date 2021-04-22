package com.fivetran.external.com.amazonaws.services.kinesis.leases.impl;

import com.fivetran.external.com.amazonaws.services.kinesis.leases.interfaces.LeaseOrderer;

import java.util.Collections;
import java.util.List;

public class LeaseShuffler<T extends Lease> implements LeaseOrderer<T> {

    @Override
    public List<T> order(List<T> leases) {
        Collections.shuffle(leases);
        return leases;
    }
}
