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

public record OffloadSegmentToTSResponse(Errors errors) {
    // TODO something to add here? some offsets, new TS HWM?

    public static OffloadSegmentToTSResponse unknownTopicOrPartition() {
        return new OffloadSegmentToTSResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION);
    }

    public static OffloadSegmentToTSResponse invalidOffsets() {
        return new OffloadSegmentToTSResponse(Errors.INVALID_OFFSETS);
    }

    public static OffloadSegmentToTSResponse success() {
        return new OffloadSegmentToTSResponse(Errors.NONE);
    }

    public enum Errors {
        NONE,
        UNKNOWN_TOPIC_OR_PARTITION,
        INVALID_OFFSETS
    }
}
