// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.common;

import com.groupcdg.pitest.annotations.CoverageIgnore;

import io.aiven.inkless.common.ObjectKey;

@CoverageIgnore
public class KeyNotFoundException extends StorageBackendException {
    public KeyNotFoundException(final StorageBackend storage, final ObjectKey key) {
        super(getMessage(storage, key));
    }

    public KeyNotFoundException(final StorageBackend storage, final ObjectKey key, final Exception e) {
        super(getMessage(storage, key), e);
    }

    private static String getMessage(final StorageBackend storage, final ObjectKey key) {
        return "Key " + key + " does not exists in storage " + storage;
    }
}
