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

import org.apache.kafka.common.Uuid;

/**
 * Request to create control-plane {@code logs} rows for a topic's partitions.
 *
 * <p>{@code numPartitions} is the topic's partition count once the operation completes; rows are created
 * for {@code [firstPartition, numPartitions)}. Topic creation starts at 0, while a partition-count
 * increase starts at the previous count so it does not insert an empty row over a partition that is
 * concurrently switching from classic to diskless (KC-387, see
 * {@code V23__Init_diskless_log_authoritative_seal.sql}).
 */
public record CreateTopicAndPartitionsRequest(Uuid topicId,
                                              String topicName,
                                              int firstPartition,
                                              int numPartitions) {

    public CreateTopicAndPartitionsRequest {
        if (firstPartition < 0 || firstPartition > numPartitions) {
            throw new IllegalArgumentException(String.format(
                "firstPartition must be within [0, %d] for topic %s, but was %d",
                numPartitions, topicName, firstPartition));
        }
    }

    public CreateTopicAndPartitionsRequest(final Uuid topicId, final String topicName, final int numPartitions) {
        this(topicId, topicName, 0, numPartitions);
    }

    public int partitionsToCreate() {
        return numPartitions - firstPartition;
    }
}
