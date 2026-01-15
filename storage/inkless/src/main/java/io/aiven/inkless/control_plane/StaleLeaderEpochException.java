/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
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
 * Exception thrown when an operation is rejected because the provided leader epoch
 * is stale (less than or equal to the current leader epoch stored in the control plane).
 */
public class StaleLeaderEpochException extends ControlPlaneException {

    private final Uuid topicId;
    private final int partition;
    private final int requestedEpoch;

    public StaleLeaderEpochException(final Uuid topicId, final int partition, final int requestedEpoch) {
        super(String.format("Stale leader epoch %d for topic %s partition %d",
            requestedEpoch, topicId, partition));
        this.topicId = topicId;
        this.partition = partition;
        this.requestedEpoch = requestedEpoch;
    }

    public Uuid topicId() {
        return topicId;
    }

    public int partition() {
        return partition;
    }

    public int requestedEpoch() {
        return requestedEpoch;
    }
}
