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

public enum FileState {
    /**
     * Uploaded by a broker, in use, etc.
     */
    UPLOADED("uploaded"),

    /**
     * Marked for deletion.
     */
    DELETING("deleting");

    public final String name;

    FileState(final String name) {
        this.name = name;
    }

    public static FileState fromName(final String name) {
        if (UPLOADED.name.equals(name)) {
            return UPLOADED;
        } else if (DELETING.name.equals(name)) {
            return DELETING;
        } else {
            throw new IllegalArgumentException("Unknown name " + name);
        }
    }
}
