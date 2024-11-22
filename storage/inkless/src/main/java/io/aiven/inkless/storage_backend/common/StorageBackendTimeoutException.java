// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.common;

import com.groupcdg.pitest.annotations.CoverageIgnore;

@CoverageIgnore
public class StorageBackendTimeoutException extends StorageBackendException {
    public StorageBackendTimeoutException(final String message, final Throwable e) {
        super(message, e);
    }
}
