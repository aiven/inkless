// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import java.util.Objects;

public record MaskedPrefixObjectKey(String prefix, String mainPath) implements ObjectKey {
    public MaskedPrefixObjectKey {
        Objects.requireNonNull(prefix, "prefix cannot be null");
        Objects.requireNonNull(mainPath, "mainPath cannot be null");
    }

    @Override
    public String value() {
        return prefix + mainPath;
    }

    @Override
    public String toString() {
        return "<prefix>" + mainPath;
    }

    public static ObjectKeyCreator creator(final String prefix) {
        return (s) -> new MaskedPrefixObjectKey(prefix, s);
    }
}
