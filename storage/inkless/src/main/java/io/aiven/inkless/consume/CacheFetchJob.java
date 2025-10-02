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

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.cache.CacheKeyCreator;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

public class CacheFetchJob implements Callable<Set<FileExtent>> {

    private final ObjectCache cache;
    private final Time time;
    private final Consumer<Integer> cacheEntrySize;
    final Consumer<Long> fileFetchDurationCallback;
    private final ObjectKey objectKey;
    private final Set<BatchInfo> batchInfoList;
    final ObjectFetcher objectFetcher;

    public CacheFetchJob(
        final ObjectCache cache,
        final ObjectFetcher objectFetcher,
        final ObjectKey objectKey,
        final Set<BatchInfo> batchInfoList,
        final Time time,
        final Consumer<Long> fileFetchDurationCallback,
        final Consumer<Integer> cacheEntrySize
    ) {
        this.cache = cache;
        this.time = time;
        this.cacheEntrySize = cacheEntrySize;
        this.fileFetchDurationCallback = fileFetchDurationCallback;
        this.objectKey = objectKey;
        this.objectFetcher = objectFetcher;
        this.batchInfoList = Collections.unmodifiableSet(batchInfoList);
    }

    @Override
    public Set<FileExtent> call() {
        Set<FileExtent> set = new HashSet<>();
        for (BatchInfo info : batchInfoList) {
            final ByteRange batchRange = info.metadata().range();
            final CacheKey lookupKey = CacheKeyCreator.create(objectKey, batchRange);

            FileExtent apply = cache.computeIfAbsent(lookupKey, cacheKey -> {
                // Let remote storage exceptions bubble up, do not catch the exceptions.
                final FileExtent freshFile = loadFileExtent(objectKey, batchRange);
                // TODO: add cache entry size also to produce/file commit
                cacheEntrySize.accept(freshFile.data().length);
                return freshFile;
            });
            set.add(apply);
        }
        return set;
    }

    private FileExtent loadFileExtent(final ObjectKey key, final ByteRange batchRange) {
        final FileExtent freshFile;
        final FileFetchJob fallback = new FileFetchJob(time, objectFetcher, key, batchRange, fileFetchDurationCallback);
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
