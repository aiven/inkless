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

/**
 * The reasons why a file on the remote storage exists.
 */
public enum FileReason {
    /**
     * Uploaded by a broker as the result of producing.
     */
    PRODUCE("produce"),

    /**
     * Uploaded by a broker as the result of merging.
     */
    MERGE("merge");

    public final String name;

    FileReason(final String name) {
        this.name = name;
    }

    public static FileReason fromName(final String name) {
        if (PRODUCE.name.equals(name)) {
            return PRODUCE;
        } else if (MERGE.name.equals(name)) {
            return MERGE;
        } else {
            throw new IllegalArgumentException("Unknown name " + name);
        }
    }
}
