/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.consume;

import org.apache.kafka.common.utils.Time;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

public class CacheFetchJob implements Callable<FileExtent> {

    private final ObjectCache cache;
    private final Time time;
    private final Consumer<Long> cacheQueryDurationCallback;
    private final Consumer<Boolean> cacheHitRateCallback;
    private final Consumer<Long> cacheStoreDurationCallback;
    private final CacheKey key;
    private final FileFetchJob fallback;

    public CacheFetchJob(
            ObjectCache cache,
            ObjectKey objectKey,
            ByteRange byteRange,
            Time time,
            ObjectFetcher objectFetcher,
            Consumer<Long> cacheQueryDurationCallback,
            Consumer<Long> cacheStoreDurationCallback,
            Consumer<Boolean> cacheHitRateCallback,
            Consumer<Long> fileFetchDurationCallback
    ) {
        this.cache = cache;
        this.time = time;
        this.cacheQueryDurationCallback = cacheQueryDurationCallback;
        this.cacheStoreDurationCallback = cacheStoreDurationCallback;
        this.cacheHitRateCallback = cacheHitRateCallback;
        this.key = createCacheKey(objectKey, byteRange);
        this.fallback = new FileFetchJob(time, objectFetcher, objectKey, byteRange, fileFetchDurationCallback);
    }

    // visible for testing
    static CacheKey createCacheKey(ObjectKey object, ByteRange byteRange) {
        return new CacheKey().
                setObject(object.value())
                .setRange(new CacheKey.ByteRange()
                        .setOffset(byteRange.offset())
                        .setLength(byteRange.size()));
    }

    @Override
    public FileExtent call() throws Exception {
        try {
            FileExtent file = TimeUtils.measureDurationMs(time, () -> cache.get(key), cacheQueryDurationCallback);
            cacheHitRateCallback.accept(file != null);
            if (file != null) {
                // cache hit
                return file;
            }
        } catch (final Exception e) {
            throw new CacheFetchException(e);
        }
        // cache miss
        FileExtent freshFile = fallback.call();
        try {
            TimeUtils.measureDurationMs(time, () -> cache.put(key, freshFile), cacheStoreDurationCallback);
        } catch (final Exception e) {
            throw new CacheFetchException(e);
        }
        return freshFile;
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
