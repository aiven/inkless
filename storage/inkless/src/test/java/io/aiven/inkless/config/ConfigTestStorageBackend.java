// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.config;

import java.io.InputStream;
import java.util.Map;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

public class ConfigTestStorageBackend implements StorageBackend {
    public Map<String, ?> passedConfig = null;

    @Override
    public void configure(final Map<String, ?> configs) {
        passedConfig = configs;
    }

    @Override
    public void delete(ObjectKey key) throws StorageBackendException {
    }

    @Override
    public void delete(Set<ObjectKey> keys) throws StorageBackendException {
    }

    @Override
    public InputStream fetch(ObjectKey key, ByteRange range) throws StorageBackendException {
        return null;
    }

    @Override
    public void upload(ObjectKey key, byte[] data) throws StorageBackendException {
    }
}
