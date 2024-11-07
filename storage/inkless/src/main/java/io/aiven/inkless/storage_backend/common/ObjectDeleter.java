// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.common;

import java.util.Set;

import io.aiven.inkless.common.ObjectKey;

public interface ObjectDeleter {
    /**
     * Delete the object with the specified key.
     *
     * <p>If the object doesn't exist, the operation still succeeds as it is idempotent.
     */
    void delete(ObjectKey key) throws StorageBackendException;

    /**
     * Delete objects from a set of keys.
     *
     * <p>If the object doesn't exist, the operation still succeeds as it is idempotent.
     */
    void delete(Set<ObjectKey> keys) throws StorageBackendException;
}
