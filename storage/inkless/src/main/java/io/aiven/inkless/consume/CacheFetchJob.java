// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.utils.Time;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.cache.CacheKey;
import io.aiven.inkless.cache.FileExtent;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

public class CacheFetchJob implements Callable<FileExtent> {

    private final ObjectCache cache;
    private final CacheKey key;
    private final FileFetchJob fallback;

    public CacheFetchJob(
            ObjectCache cache,
            CacheKey key,
            Time time,
            ObjectFetcher objectFetcher,
            Consumer<Long> fileFetchDurationCallback
    ) {
        this.cache = cache;
        this.key = key;
        this.fallback = new FileFetchJob(time, objectFetcher, key.object(), key.range(), fileFetchDurationCallback);
    }

    @Override
    public FileExtent call() throws Exception {
        FileExtent file = cache.get(key);
        if (file == null) {
            // cache miss
            file = fallback.call();
            cache.put(key, file);
        }
        return file;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheFetchJob that = (CacheFetchJob) o;
        return Objects.equals(cache, that.cache)
                && Objects.equals(key, that.key)
                && Objects.equals(fallback, that.fallback);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cache, key, fallback);
    }
}
