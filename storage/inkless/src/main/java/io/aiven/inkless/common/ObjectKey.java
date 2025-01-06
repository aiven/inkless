// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

public interface ObjectKey {
    static ObjectKeyCreator create(String prefix, boolean masked) {
        if (masked) {
            return (s) -> new MaskedPrefixObjectKey(prefix, s);
        } else {
            return (s) -> new PlainObjectKey(prefix, s);
        }
    }

    String value();
}
