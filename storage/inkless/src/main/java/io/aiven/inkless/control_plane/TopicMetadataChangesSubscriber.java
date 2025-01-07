// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.image.TopicsDelta;

import java.util.concurrent.Future;

@FunctionalInterface
public interface TopicMetadataChangesSubscriber {
    Future<?> onTopicMetadataChanges(TopicsDelta topicsDelta);
}
