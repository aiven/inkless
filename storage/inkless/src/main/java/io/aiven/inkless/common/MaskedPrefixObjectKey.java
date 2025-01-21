// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import java.util.Objects;

public record MaskedPrefixObjectKey(ObjectKey.Path path) implements ObjectKey {
    public MaskedPrefixObjectKey {
        Objects.requireNonNull(path, "path cannot be null");
    }

    public static MaskedPrefixObjectKey create(String prefix, String mainPath) {
        return new MaskedPrefixObjectKey(Path.create(prefix, mainPath));
    }

    public static MaskedPrefixObjectKey from(String value) {
        return new MaskedPrefixObjectKey(Path.from(value));
    }

    @Override
    public String value() {
        return path.value();
    }

    @Override
    public String toString() {
        return "<prefix>/" + path.name();
    }
}
