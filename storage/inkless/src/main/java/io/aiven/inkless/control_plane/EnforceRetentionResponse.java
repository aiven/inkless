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

public record EnforceRetentionResponse(Errors errors,
                                       int batchesDeleted,
                                       long bytesDeleted,
                                       long logStartOffset) {

    public static EnforceRetentionResponse success(final int batchesDeleted,
                                                   final long bytesDeleted,
                                                   final long newLogStartOffset) {
        return new EnforceRetentionResponse(Errors.NONE, batchesDeleted, bytesDeleted, newLogStartOffset);
    }

    public static EnforceRetentionResponse unknownTopicOrPartition() {
        return new EnforceRetentionResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, -1, -1, -1);
    }
}
