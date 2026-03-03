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

import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

/**
 * Handles caching of uploaded file data to the object cache.
 *
 * <p>This class implements {@link UploadCompletionConsumer} for use with {@code CompletableFuture.whenCompleteAsync()}.
 * When the upload completes (successfully or with error), the callback is invoked on the
 * provided executor to either store the data to cache or release resources.
 *
 * <p><b>Thread Safety:</b> The {@link #onUploadComplete} method is safe to call from any thread.
 * The actual cache store work is executed on the provided executor.
 */
public class CacheStoreJob implements UploadCompletionConsumer {

    private final Time time;
    private final ObjectCache cache;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final byte[] data;
    private final Consumer<Long> cacheStoreDurationCallback;

    public CacheStoreJob(Time time,
                         ObjectCache cache,
                         KeyAlignmentStrategy keyAlignmentStrategy,
                         byte[] data,
                         Consumer<Long> cacheStoreDurationCallback) {
        this.time = time;
        this.cache = cache;
        this.keyAlignmentStrategy = keyAlignmentStrategy;
        this.data = data;
        this.cacheStoreDurationCallback = cacheStoreDurationCallback;
    }

    /**
     * {@inheritDoc}
     *
     * <p>If the upload succeeded (objectKey is non-null and error is null), the data is stored
     * to cache. If the upload failed, this is a no-op since there's nothing to cache.
     */
    @Override
    public void onUploadComplete(ObjectKey objectKey, Throwable error) {
        if (objectKey != null && error == null) {
            storeToCache(objectKey);
        }
        // If the upload failed there's nothing to cache and we succeed vacuously.
    }

    private void storeToCache(ObjectKey objectKey) {
        ByteRange fileRange = new ByteRange(0, data.length);
        Set<ByteRange> ranges = keyAlignmentStrategy.align(Collections.singletonList(fileRange));
        String object = objectKey.value();
        for (ByteRange range : ranges) {
            CacheKey key = new CacheKey()
                    .setObject(object)
                    .setRange(new CacheKey.ByteRange()
                            .setOffset(range.offset())
                            .setLength(range.size()));
            ByteRange intersect = ByteRange.intersect(range, fileRange);
            byte[] extentBytes = new byte[intersect.bufferSize()];
            System.arraycopy(data, intersect.bufferOffset(), extentBytes, 0, extentBytes.length);
            FileExtent extent = new FileExtent()
                    .setObject(object)
                    .setRange(new FileExtent.ByteRange()
                            .setOffset(intersect.offset())
                            .setLength(intersect.size()))
                    .setData(extentBytes);
            TimeUtils.measureDurationMs(time, () -> cache.put(key, extent), cacheStoreDurationCallback);
        }
    }
}
