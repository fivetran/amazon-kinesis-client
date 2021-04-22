package com.fivetran.external.com.amazonaws.services.kinesis.clientlibrary.lib.worker;

import java.util.Collection;

public interface PriorizeableShard {
    String getShardId();
    Collection<String> getParentShardIds();
    boolean isCompleted();
}
