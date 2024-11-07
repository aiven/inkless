// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.common;

import com.groupcdg.pitest.annotations.CoverageIgnore;

@CoverageIgnore
public class StorageBackendException extends Exception {

    public StorageBackendException(final String message) {
        super(message);
    }

    public StorageBackendException(final String message, final Throwable e) {
        super(message, e);
    }
}
