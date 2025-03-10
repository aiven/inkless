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

import java.util.Objects;

public interface ObjectKey {
    static ObjectKeyCreator creator(String prefix, boolean masked) {
        if (masked) {
            return new ObjectKeyCreator(prefix) {
                @Override
                public ObjectKey from(String value) {
                    return new MaskedPrefixObjectKey(Path.from(value));
                }

                @Override
                public ObjectKey create(String value) {
                    return new MaskedPrefixObjectKey(Path.create(prefix, value));
                }
            };
        } else {
            return new ObjectKeyCreator(prefix) {
                @Override
                public ObjectKey from(String value) {
                    return new PlainObjectKey(Path.from(value));
                }

                @Override
                public ObjectKey create(String value) {
                    return new PlainObjectKey(Path.create(prefix, value));
                }
            };
        }
    }

    String value();

    record Path(String prefix, String name) {
        static String SEPARATOR = "/";

        public Path {
            Objects.requireNonNull(prefix, "prefix cannot be null");
            Objects.requireNonNull(name, "name cannot be null");

            if (!prefix.isBlank()) {
                if (prefix.startsWith(SEPARATOR) || prefix.endsWith(SEPARATOR)) {
                    throw new IllegalArgumentException("prefix cannot start or end with a separator");
                }
            }

            if (name.contains(SEPARATOR)) {
                throw new IllegalArgumentException("name cannot contain a separator");
            }
        }

        public static Path create(String prefix, String mainPath) {
            return new Path(prefix, mainPath);
        }

        public static Path from(String value) {
            final int index = value.lastIndexOf(SEPARATOR);
            if (index == -1) {
                return new Path("", value);
            }
            return new Path(value.substring(0, index), value.substring(index + 1));
        }

        public String value() {
            if (prefix.isEmpty()) return name;
            else return prefix + SEPARATOR + name;
        }
    }
}
