/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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

public record ListOffsetsRequest(
        TopicIdPartition topicIdPartition,
        long timestamp
) {

    public static final long EARLIEST_TIMESTAMP = org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_TIMESTAMP;
    public static final long LATEST_TIMESTAMP = org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIMESTAMP;
    public static final long MAX_TIMESTAMP = org.apache.kafka.common.requests.ListOffsetsRequest.MAX_TIMESTAMP;
    public static final long EARLIEST_LOCAL_TIMESTAMP = org.apache.kafka.common.requests.ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP;
    public static final long LATEST_TIERED_TIMESTAMP = org.apache.kafka.common.requests.ListOffsetsRequest.LATEST_TIERED_TIMESTAMP;
}
