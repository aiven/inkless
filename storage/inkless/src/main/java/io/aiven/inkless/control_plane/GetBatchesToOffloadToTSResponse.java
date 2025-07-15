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

import java.util.List;
import java.util.Objects;

public record GetBatchesToOffloadToTSResponse(Errors errors,
                                              List<BatchInfo> batches) {
    public static GetBatchesToOffloadToTSResponse unknownTopicOrPartition() {
        return new GetBatchesToOffloadToTSResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null);
    }

    public static GetBatchesToOffloadToTSResponse notEnoughData() {
        return new GetBatchesToOffloadToTSResponse(Errors.NOT_ENOUGH_DATA, null);
    }

    public static GetBatchesToOffloadToTSResponse success(final List<BatchInfo> batches) {
        return new GetBatchesToOffloadToTSResponse(
            Errors.NONE, Objects.requireNonNull(batches, "batches cannot be null"));
    }

    public enum Errors {
        NONE,
        UNKNOWN_TOPIC_OR_PARTITION,
        NOT_ENOUGH_DATA
    }
}
