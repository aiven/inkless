package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;

public record PruneDisklessLogsRequest(TopicIdPartition topicIdPartition, long highestRemoteOffset) {
}
