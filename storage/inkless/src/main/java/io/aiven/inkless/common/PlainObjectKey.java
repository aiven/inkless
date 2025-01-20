// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
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
