// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import java.util.Objects;

public record PlainObjectKey(String prefix, String mainPath) implements ObjectKey {
    public PlainObjectKey {
        Objects.requireNonNull(prefix, "prefix cannot be null");
        Objects.requireNonNull(mainPath, "mainPath cannot be null");
    }

    @Override
    public String value() {
        return prefix + mainPath;
    }

    @Override
    public String storedPart() {
        return mainPath;
    }

    @Override
    public String toString() {
        return value();
    }

    public static ObjectKeyCreator creator(final String prefix) {
        return (s) -> new PlainObjectKey(prefix, s);
    }
}
