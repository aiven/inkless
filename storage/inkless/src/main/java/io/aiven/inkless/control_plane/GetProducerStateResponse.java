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

import org.apache.kafka.common.protocol.Errors;

import java.util.List;

public record GetProducerStateResponse(Errors errors,
                                       List<ProducerStateEntry> entries) {

    public record ProducerStateEntry(long producerId,
                                     short producerEpoch,
                                     int baseSequence,
                                     int lastSequence,
                                     long assignedOffset,
                                     long batchMaxTimestamp) {
    }

    public static GetProducerStateResponse success(final List<ProducerStateEntry> entries) {
        return new GetProducerStateResponse(Errors.NONE, entries);
    }

    public static GetProducerStateResponse unknownTopicOrPartition() {
        return new GetProducerStateResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, List.of());
    }
}
