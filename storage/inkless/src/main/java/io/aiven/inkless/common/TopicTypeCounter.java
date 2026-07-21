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
 * A utility for interceptors to count topic types (diskless/classic) in requests.
 */
public class TopicTypeCounter {
    private final MetadataView metadata;

    public TopicTypeCounter(final MetadataView metadata) {
        this.metadata = metadata;
    }

    public Result count(final Set<TopicPartition> topicPartitions) {
        int entitiesForDisklessTopics = 0;
        int entitiesForNonDisklessTopics = 0;
        for (final var tp : topicPartitions) {
            if (metadata.isDisklessTopic(tp.topic())) {
                entitiesForDisklessTopics += 1;
            } else {
                entitiesForNonDisklessTopics += 1;
            }
        }
        return new Result(entitiesForDisklessTopics, entitiesForNonDisklessTopics);
    }

    public record Result(int entityCountForDisklessTopics,
                         int entityCountForNonDisklessTopics) {
        public boolean bothTypesPresent() {
            return entityCountForDisklessTopics > 0 && entityCountForNonDisklessTopics > 0;
        }

        public boolean noDiskless() {
            return entityCountForDisklessTopics == 0;
        }
    }
}
