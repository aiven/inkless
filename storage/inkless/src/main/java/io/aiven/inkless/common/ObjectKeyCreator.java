// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

@FunctionalInterface
public interface ObjectKeyCreator {
    ObjectKey create(String value);
}
