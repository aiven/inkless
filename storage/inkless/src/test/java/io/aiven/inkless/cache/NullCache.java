// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.cache;

import java.io.IOException;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

/**
 * A cache implementation without any storage, always misses.
 */
public class NullCache implements ObjectCache {
    @Override
    public FileExtent get(CacheKey key) {
        return null;
    }

    @Override
    public void put(CacheKey key, FileExtent value) {
    }

    @Override
    public boolean remove(CacheKey key) {
        return false;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        // no-op
    }
}
