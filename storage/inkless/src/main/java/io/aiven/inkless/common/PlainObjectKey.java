// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

public record PlainObjectKey(String prefix, String mainPathAndSuffix) implements ObjectKey {
    @Override
    public String value() {
        return prefix + mainPathAndSuffix;
    }

    @Override
    public String toString() {
        return value();
    }
}
