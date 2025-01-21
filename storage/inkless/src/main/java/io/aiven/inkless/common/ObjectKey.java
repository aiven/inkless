// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
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
