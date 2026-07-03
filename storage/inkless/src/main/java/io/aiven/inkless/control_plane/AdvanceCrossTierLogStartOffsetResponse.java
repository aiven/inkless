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

import org.apache.kafka.common.protocol.Errors;

/**
 * Result of an {@link AdvanceCrossTierLogStartOffsetRequest}.
 *
 * <p>{@code remoteLogStartOffset} is the value stored after the (forward-only) update, which may be
 * unchanged if the reported value did not advance past the previously stored one. It is {@code -1}
 * when no remote tier has been tracked yet or when the request failed.
 */
public record AdvanceCrossTierLogStartOffsetResponse(Errors errors,
                                                     long remoteLogStartOffset) {

    public static final long NO_OFFSET = -1L;

    public static AdvanceCrossTierLogStartOffsetResponse success(final long remoteLogStartOffset) {
        return new AdvanceCrossTierLogStartOffsetResponse(Errors.NONE, remoteLogStartOffset);
    }

    public static AdvanceCrossTierLogStartOffsetResponse unknownTopicOrPartition() {
        return new AdvanceCrossTierLogStartOffsetResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, NO_OFFSET);
    }
}
