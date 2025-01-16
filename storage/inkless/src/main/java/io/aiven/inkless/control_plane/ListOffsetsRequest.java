// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;

public record ListOffsetsRequest(
        TopicIdPartition topicIdPartition,
        long timestamp
) {

    public static final long EARLIEST_TIMESTAMP = org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_TIMESTAMP;
    public static final long LATEST_TIMESTAMP = org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIMESTAMP;
    public static final long MAX_TIMESTAMP = org.apache.kafka.common.requests.ListOffsetsRequest.MAX_TIMESTAMP;
    public static final long EARLIEST_LOCAL_TIMESTAMP = org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP;
    public static final long LATEST_TIERED_TIMESTAMP = org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIERED_TIMESTAMP;
}
