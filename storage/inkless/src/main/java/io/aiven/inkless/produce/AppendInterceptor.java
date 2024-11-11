// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;

public class AppendInterceptor {
    /**
     * Intercept an attempt to append records.
     *
     * <p>If the interception happened, the {@code responseCallback} is called from inside the interceptor.
     * @return {@code true} if interception happened
     */
    public boolean intercept(final Map<TopicPartition, MemoryRecords> entriesPerPartition,
                             final Consumer<Map<TopicPartition, ProduceResponse.PartitionResponse>> responseCallback) {
        return false;
    }
}
