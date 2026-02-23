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
import java.util.Optional;
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
import io.aiven.inkless.produce.buffer.BatchBufferData;

public class CacheStoreJob implements Runnable {

    private final Time time;
    private final ObjectCache cache;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final BatchBufferData data;
    private final Future<ObjectKey> uploadFuture;
    private final Consumer<Long> cacheStoreDurationCallback;

    public CacheStoreJob(Time time, ObjectCache cache, KeyAlignmentStrategy keyAlignmentStrategy, BatchBufferData data, Future<ObjectKey> uploadFuture, Consumer<Long> cacheStoreDurationCallback) {
        this.time = time;
        this.cache = cache;
        this.keyAlignmentStrategy = keyAlignmentStrategy;
        this.data = data;
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
        } finally {
            // Release the buffer reference when cache store is done
            data.release();
        }
    }

    private void storeToCache(ObjectKey objectKey) {
        ByteRange fileRange = new ByteRange(0, data.size());
        Set<ByteRange> ranges = keyAlignmentStrategy.align(Collections.singletonList(fileRange));
        String object = objectKey.value();
        for (ByteRange range : ranges) {
            CacheKey key = new CacheKey()
                    .setObject(object)
                    .setRange(new CacheKey.ByteRange()
                            .setOffset(range.offset())
                            .setLength(range.size()));
            ByteRange intersect = ByteRange.intersect(range, fileRange);
            byte[] extentBytes = extractBytes(intersect);
            FileExtent extent = new FileExtent()
                    .setObject(object)
                    .setRange(new FileExtent.ByteRange()
                            .setOffset(intersect.offset())
                            .setLength(intersect.size()))
                    .setData(extentBytes);
            TimeUtils.measureDurationMs(time, () -> cache.put(key, extent), cacheStoreDurationCallback);
        }
    }

    /**
     * Extract bytes for the given range from the buffer data.
     * Uses ByteBuffer slice when available, falls back to copyTo().
     */
    private byte[] extractBytes(ByteRange range) {
        // Try ByteBuffer slice path for pooled buffers
        Optional<ByteBuffer> slice = data.asSlice(range);
        if (slice.isPresent()) {
            ByteBuffer buf = slice.get();
            byte[] bytes = new byte[buf.remaining()];
            buf.get(bytes);
            return bytes;
        }

        // Fall back to copyTo for heap byte array data
        int size = range.bufferSize();
        byte[] bytes = new byte[size];
        data.copyTo(range.bufferOffset(), bytes, 0, size);
        return bytes;
    }
}
