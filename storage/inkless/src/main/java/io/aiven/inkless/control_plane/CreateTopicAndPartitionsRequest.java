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
 * Request to create control-plane {@code logs} rows for {@code [firstPartition, numPartitions)}.
 * {@code numPartitions} is the exclusive end of that range, not necessarily the topic's partition
 * count: a retry that fills holes may emit several such ranges.
 *
 * Switch-pending and sealed partitions are skipped when the published image already shows them.
 * A partition missing from the image is still inserted. The guarded upsert in
 * {@code V23__Init_diskless_log_authoritative_seal.sql} is what actually refuses a zero-offset
 * placeholder over a switching row (KC-387).
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
