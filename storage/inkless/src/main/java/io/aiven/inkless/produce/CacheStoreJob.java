// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;
import org.apache.kafka.common.utils.Time;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public class CacheStoreJob implements Runnable {

    private final Time time;
    private final ObjectCache cache;
    private final KeyAlignmentStrategy keyAlignmentStrategy;
    private final byte[] data;
    private final Future<ObjectKey> uploadFuture;
    private final Consumer<Long> cacheStoreDurationCallback;

    public CacheStoreJob(Time time, ObjectCache cache, KeyAlignmentStrategy keyAlignmentStrategy, byte[] data, Future<ObjectKey> uploadFuture, Consumer<Long> cacheStoreDurationCallback) {
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
        }
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
