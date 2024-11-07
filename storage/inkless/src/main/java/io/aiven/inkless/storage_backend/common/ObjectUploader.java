// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.common;

import io.aiven.inkless.common.ObjectKey;

public interface ObjectUploader {
    void upload(ObjectKey key, byte[] data) throws StorageBackendException;
}
