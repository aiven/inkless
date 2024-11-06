// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

public interface MetadataView {
    Set<TopicPartition> getTopicPartitions(String topicName);

    Uuid getTopicId(String topicName);
}
