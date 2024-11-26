// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.internals.log.LogConfig;

import java.util.Set;

public interface MetadataView {
    Set<TopicPartition> getTopicPartitions(String topicName);

    Uuid getTopicId(String topicName);

    boolean isInklessTopic(String topicName);

    LogConfig getTopicConfig(String topicName);
}
