// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.image.TopicsDelta;

@FunctionalInterface
public interface TopicMetadataChangesSubscriber {
    void onTopicMetadataChanges(TopicsDelta topicsDelta);
}
