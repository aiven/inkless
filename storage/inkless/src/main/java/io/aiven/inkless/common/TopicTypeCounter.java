// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.apache.kafka.common.TopicPartition;

import java.util.Set;

import io.aiven.inkless.control_plane.MetadataView;

/**
 * A utility for interceptors to count topic types (Inkless/classic) in requests.
 */
public class TopicTypeCounter {
    private final MetadataView metadata;

    public TopicTypeCounter(final MetadataView metadata) {
        this.metadata = metadata;
    }

    public Result count(final Set<TopicPartition> topicPartitions) {
        int entitiesForInklessTopics = 0;
        int entitiesForNonInklessTopics = 0;
        for (final var tp : topicPartitions) {
            if (metadata.isInklessTopic(tp.topic())) {
                entitiesForInklessTopics += 1;
            } else {
                entitiesForNonInklessTopics += 1;
            }
        }
        return new Result(entitiesForInklessTopics, entitiesForNonInklessTopics);
    }

    public record Result(int entityCountForInklessTopics,
                         int entityCountForNonInklessTopics) {
        public boolean bothTypesPresent() {
            return entityCountForInklessTopics > 0 && entityCountForNonInklessTopics > 0;
        }

        public boolean noInkless() {
            return entityCountForInklessTopics == 0;
        }
    }
}
