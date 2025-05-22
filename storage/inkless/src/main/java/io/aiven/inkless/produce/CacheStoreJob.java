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
package io.aiven.inkless.produce;

import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public class CacheStoreJob implements Runnable {

    private final Time time;
    private final ObjectCache cache;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final ByteBuffer data;
    private final int totalSize;
    private final Future<ObjectKey> uploadFuture;
    private final Consumer<Long> cacheStoreDurationCallback;

    public CacheStoreJob(
        Time time,
        ObjectCache cache,
        KeyAlignmentStrategy keyAlignmentStrategy,
        ByteBuffer data,
        int totalSize,
        Future<ObjectKey> uploadFuture,
        Consumer<Long> cacheStoreDurationCallback
    ) {
        this.time = time;
        this.cache = cache;
        this.keyAlignmentStrategy = keyAlignmentStrategy;
        this.data = data;
        this.totalSize = totalSize;
        this.uploadFuture = uploadFuture;
        this.cacheStoreDurationCallback = cacheStoreDurationCallback;
    }

    @Override
    public void run() {
        try {
            ObjectKey objectKey = uploadFuture.get();
            storeToCache(objectKey);
        } catch (final Throwable e) {
            // If the upload failed there's nothing to cache and we succeed vacuously.
        }
    }

    private void storeToCache(ObjectKey objectKey) {
        ByteRange fileRange = new ByteRange(0, totalSize);
        Set<ByteRange> ranges = keyAlignmentStrategy.align(Collections.singletonList(fileRange));
        String object = objectKey.value();
        for (ByteRange range : ranges) {
            CacheKey key = new CacheKey()
                    .setObject(object)
                    .setRange(new CacheKey.ByteRange()
                            .setOffset(range.offset())
                            .setLength(range.size()));
            ByteRange intersect = ByteRange.intersect(range, fileRange);
            final byte[] cachedData = new byte[(int) intersect.size()];
            data.duplicate().position(intersect.bufferOffset()).get(cachedData);
            FileExtent extent = new FileExtent()
                .setObject(object)
                .setRange(new FileExtent.ByteRange()
                    .setOffset(intersect.offset())
                    .setLength(intersect.size()))
                .setData(cachedData);
            TimeUtils.measureDurationMs(time, () -> cache.put(key, extent), cacheStoreDurationCallback);
        }
    }
}
