// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.admin.BrokerMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import java.util.Properties;
import java.util.Set;

public interface MetadataView {
    Iterable<BrokerMetadata> getAliveBrokers();

    Set<TopicPartition> getTopicPartitions(String topicName);

    Uuid getTopicId(String topicName);

    boolean isInklessTopic(String topicName);

    Properties getTopicConfig(String topicName);
}
