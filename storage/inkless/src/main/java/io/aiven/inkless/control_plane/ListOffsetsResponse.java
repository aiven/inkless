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
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

public record ListOffsetsResponse(
        Errors errors,
        TopicIdPartition topicIdPartition,
        long timestamp,
        long offset
) {

    public static ListOffsetsResponse success(TopicIdPartition topicIdPartition, final long timestamp, final long offset) {
        return new ListOffsetsResponse(Errors.NONE, topicIdPartition, timestamp, offset);
    }

    public static ListOffsetsResponse unknownTopicOrPartition(TopicIdPartition topicIdPartition) {
        return new ListOffsetsResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, topicIdPartition, NO_TIMESTAMP, -1);
    }

    public static ListOffsetsResponse unknownServerError(TopicIdPartition topicIdPartition) {
        return new ListOffsetsResponse(Errors.UNKNOWN_SERVER_ERROR, topicIdPartition, NO_TIMESTAMP, -1);
    }
}
