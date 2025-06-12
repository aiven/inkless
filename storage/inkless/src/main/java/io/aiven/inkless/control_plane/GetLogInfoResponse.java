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

public record GetLogInfoResponse(Errors errors,
                                 long logStartOffset,
                                 long highWatermark,
                                 long byteSize) {
    public static final long INVALID_OFFSET = -1L;
    public static final long INVALID_BYTE_SIZE = -1L;

    public static GetLogInfoResponse success(final long logStartOffset,
                                             final long highWatermark,
                                             final long byteSize) {
        return new GetLogInfoResponse(Errors.NONE, logStartOffset, highWatermark, byteSize);
    }

    public static GetLogInfoResponse unknownTopicOrPartition() {
        return new GetLogInfoResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, INVALID_OFFSET, INVALID_OFFSET, INVALID_BYTE_SIZE);
    }
}
