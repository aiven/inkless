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
package io.aiven.inkless.common;

public enum ObjectFormat {

    /**
     * An object consisting of batches which:
     * <ul>
     *     <li>are locally ordered by byte position
     *     <li>include a valid timestamp iff {@link org.apache.kafka.common.record.TimestampType#CREATE_TIME} is used
     *     <li>do not include a valid start/end log offset
     *     <li>are members of multiple partitions
     * </ul>
     * <p>This object may contain invalid data between batches.
     * <p>Batches are formatted with magic number >= 2.
     */
    WRITE_AHEAD_MULTI_SEGMENT((byte) 1);

    public final byte id;

    ObjectFormat(byte id) {
        this.id = id;
    }

    public static ObjectFormat forId(byte id) {
        if (id == WRITE_AHEAD_MULTI_SEGMENT.id) {
            return WRITE_AHEAD_MULTI_SEGMENT;
        } else {
            throw new IllegalArgumentException("Unknown object format " + id);
        }
    }
}
