// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.cache;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public class MemoryCache implements ObjectCache {

    private final Cache<CacheKey, FileExtent> backingCache = new LRUCache<>(10);
    @Override
    public FileExtent get(CacheKey key) {
        return backingCache.get(key);
    }

    @Override
    public void put(CacheKey key, FileExtent value) {
        backingCache.put(key, value);
    }

    @Override
    public boolean remove(CacheKey key) {
        return backingCache.remove(key);
    }

    @Override
    public long size() {
        return backingCache.size();
    }
}
