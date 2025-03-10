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
