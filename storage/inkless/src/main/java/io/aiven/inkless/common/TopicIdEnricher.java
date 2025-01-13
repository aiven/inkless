// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.Map;

import io.aiven.inkless.control_plane.MetadataView;

public class TopicIdEnricher {
    /**
     * Add topic IDs to {@link TopicPartition} map keys.
     * @param metadata the metadata to look for topic IDs.
     * @throws TopicIdNotFoundException when topic ID isn't found.
     */
    public static <T> Map<TopicIdPartition, T> enrich(final MetadataView metadata,
                                                      final Map<TopicPartition, T> input
    ) throws TopicIdNotFoundException {
        final Map<TopicIdPartition, T> result = new HashMap<>();
        for (final var entry : input.entrySet()) {
            final String topicName = entry.getKey().topic();
            final Uuid topicId = metadata.getTopicId(topicName);
            // This should not happen as the upstream code should check the topic exists.
            if (topicId.equals(Uuid.ZERO_UUID)) {
                throw new TopicIdNotFoundException(topicName);
            }
            result.put(new TopicIdPartition(topicId, entry.getKey()), entry.getValue());
        }
        return result;
    }

    public static class TopicIdNotFoundException extends Exception {
        public final String topicName;

        public TopicIdNotFoundException(final String topicName) {
            this.topicName = topicName;
        }
    }
}
