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
package io.aiven.inkless.common;

import java.util.Objects;

public record PlainObjectKey(Path path) implements ObjectKey {
    public PlainObjectKey {
        Objects.requireNonNull(path, "path cannot be null");
    }

    public static PlainObjectKey create(String prefix, String mainPath) {
        return new PlainObjectKey(Path.create(prefix, mainPath));
    }

    public static PlainObjectKey from(String value) {
        return new PlainObjectKey(Path.from(value));
    }

    @Override
    public String value() {
        return path.value();
    }

    @Override
    public String toString() {
        return value();
    }
}
