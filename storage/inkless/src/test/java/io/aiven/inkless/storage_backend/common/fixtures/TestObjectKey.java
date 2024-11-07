// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.common.fixtures;

import java.util.Objects;

import io.aiven.inkless.common.ObjectKey;

public class TestObjectKey implements ObjectKey {
    private final String key;

    public TestObjectKey(final String key) {
        this.key = Objects.requireNonNull(key, "key cannot be null");
    }

    @Override
    public String value() {
        return key;
    }

    @Override
    public String toString() {
        return key;
    }
}
