package com.fivetran.external.com.amazonaws.services.kinesis.leases.interfaces;

import com.fivetran.external.com.amazonaws.services.kinesis.leases.impl.Lease;

import java.util.List;

public interface LeaseOrderer<T extends Lease> {
    List<T> order(List<T> leases);
}
