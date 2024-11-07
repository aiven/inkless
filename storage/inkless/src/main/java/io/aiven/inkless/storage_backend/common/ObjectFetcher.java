// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.common;

import java.io.InputStream;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;

public interface ObjectFetcher {
    InputStream fetch(ObjectKey key, ByteRange range) throws StorageBackendException;
}
