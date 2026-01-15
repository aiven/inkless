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

/**
 * Represents a single producer state entry to be stored in the control plane.
 * This is used during the initialization of diskless log to transfer producer state
 * from the local log to the control plane.
 *
 * @param producerId      The producer ID
 * @param producerEpoch   The producer epoch
 * @param baseSequence    The base sequence number of the batch
 * @param lastSequence    The last sequence number of the batch
 * @param assignedOffset  The offset assigned to this batch
 * @param batchMaxTimestamp The maximum timestamp in the batch
 */
public record ProducerStateSnapshot(
    long producerId,
    short producerEpoch,
    int baseSequence,
    int lastSequence,
    long assignedOffset,
    long batchMaxTimestamp
) {
}
