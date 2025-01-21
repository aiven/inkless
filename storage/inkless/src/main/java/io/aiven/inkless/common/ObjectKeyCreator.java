// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

public abstract class ObjectKeyCreator {
    final String prefix;

    protected ObjectKeyCreator(String prefix) {
        this.prefix = prefix;
    }

    public abstract ObjectKey from(String value);
    public abstract ObjectKey create(String value);
}
