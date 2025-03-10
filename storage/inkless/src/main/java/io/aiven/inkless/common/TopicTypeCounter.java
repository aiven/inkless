/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
