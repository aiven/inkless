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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

public class CacheFetchJob implements Callable<Set<FileExtent>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheFetchJob.class);

    private final ObjectCache cache;
    private final Time time;
    private final Consumer<Long> cacheQueryDurationCallback;
    private final Consumer<Boolean> cacheHitRateCallback;
    private final Consumer<Long> cacheStoreDurationCallback;
    private final Consumer<Integer> cacheEntrySize;
    final Consumer<Long> fileFetchDurationCallback;
    private final ObjectKey objectKey;
    private final ObjectKeyCreator objectKeyCreator;
    private final List<BatchInfo> batchInfoList;
    final ObjectFetcher objectFetcher;

    public CacheFetchJob(
        final ObjectCache cache,
        final ObjectFetcher objectFetcher,
        final ObjectKey objectKey,
        final ObjectKeyCreator objectKeyCreator,
        final List<BatchInfo> batchInfoList,
        final Time time,
        final Consumer<Long> cacheQueryDurationCallback,
        final Consumer<Long> cacheStoreDurationCallback,
        final Consumer<Boolean> cacheHitRateCallback,
        final Consumer<Long> fileFetchDurationCallback,
        final Consumer<Integer> cacheEntrySize
    ) {
        this.cache = cache;
        this.time = time;
        this.cacheQueryDurationCallback = cacheQueryDurationCallback;
        this.cacheStoreDurationCallback = cacheStoreDurationCallback;
        this.cacheHitRateCallback = cacheHitRateCallback;
        this.cacheEntrySize = cacheEntrySize;
        this.fileFetchDurationCallback = fileFetchDurationCallback;
        this.objectKey = objectKey;
        this.objectKeyCreator = objectKeyCreator;
        this.objectFetcher = objectFetcher;
        this.batchInfoList = batchInfoList;
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
    public Set<FileExtent> call() {
        // Catch cache-related exceptions but let remote storage exceptions bubble up.
        return batchInfoList.stream().map(batchInfo -> {
            final ByteRange batchRange = batchInfo.metadata().range();
            final ObjectKey batchObjectKey = objectKeyCreator.from(batchInfo.objectKey());
            final CacheKey cacheKey = createCacheKey(objectKey, batchRange);

            if (cache.supportsComputeIfAbsent()) {
                final AtomicBoolean cacheHit = new AtomicBoolean(true);
                final FileExtent fileExtent = cache.computeIfAbsent(cacheKey, mappingKey -> {
                    final FileExtent freshFile = loadFileExtent(batchObjectKey, batchRange);
                    cacheHit.set(false);
                    cacheEntrySize.accept(freshFile.data().length);
                    return freshFile;
                });
                cacheHitRateCallback.accept(cacheHit.get());
                return fileExtent;
            } else{
                try {
                    FileExtent file = TimeUtils.measureDurationMs(time, () -> cache.get(cacheKey), cacheQueryDurationCallback);
                    cacheHitRateCallback.accept(file != null);
                    if (file != null) {
                        // cache hit
                        return file;
                    }
                } catch (final Exception e) {
                    LOGGER.warn("Cache get exception", new CacheFetchException(e));
                }
                // cache miss
                final FileExtent freshFile = loadFileExtent(objectKey, batchRange);
                try {
                    TimeUtils.measureDurationMs(time, () -> cache.put(cacheKey, freshFile), cacheStoreDurationCallback);
                    cacheEntrySize.accept(freshFile.data().length);
                } catch (final Exception e) {
                    LOGGER.warn("Cache put exception", new CacheFetchException(e));
                }
                return freshFile;
            }
        }).collect(Collectors.toSet());
    }

    private FileExtent loadFileExtent(final ObjectKey loadObjectKey, final ByteRange batchRange) {
        final FileExtent freshFile;
        final FileFetchJob fallback = new FileFetchJob(time, objectFetcher, loadObjectKey, batchRange, fileFetchDurationCallback);
        try {
            freshFile = fallback.call();
        } catch (Exception e) {
            throw new FetchException(e);
        }
        return freshFile;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheFetchJob that = (CacheFetchJob) o;
        return Objects.equals(cache, that.cache)
            && Objects.equals(objectKey, that.objectKey)
            && Objects.equals(batchInfoList, that.batchInfoList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cache, objectKey, batchInfoList);
    }

    @Override
    public String toString() {
        return "CacheFetchJob{" +
                "cache=" + cache +
                "objectKey=" + objectKey +
                ", batchInfoList=" + batchInfoList +
                '}';
    }
}
