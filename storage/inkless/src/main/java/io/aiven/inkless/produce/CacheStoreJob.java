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

import java.util.List;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.cache.CacheKeyCreator;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public class CacheStoreJob implements Runnable {

    private final Time time;
    private final ObjectCache cache;
    private final byte[] data;
    private final Future<ObjectKey> uploadFuture;
    private final List<CommitBatchResponse> commitBatchResponses;
    private final Consumer<Long> cacheStoreDurationCallback;

    public CacheStoreJob(
            final Time time,
            final ObjectCache cache,
            final byte[] data,
            final Future<ObjectKey> uploadFuture,
            final List<CommitBatchResponse> commitBatchResponses,
            final Consumer<Long> cacheStoreDurationCallback) {
        this.time = time;
        this.cache = cache;
        this.data = data;
        this.uploadFuture = uploadFuture;
        this.commitBatchResponses = commitBatchResponses;
        this.cacheStoreDurationCallback = cacheStoreDurationCallback;
    }

    @Override
    public void run() {
        try {
            final ObjectKey objectKey = uploadFuture.get();
            for (final CommitBatchResponse commitBatchResponse : commitBatchResponses) {
                storeToCache(objectKey, commitBatchResponse);
            }
        } catch (final Throwable e) {
            // If the upload failed there's nothing to cache and we succeed vacuously.
        }
    }

    private void storeToCache(final ObjectKey objectKey, final CommitBatchResponse commitBatchResponse) {
        final CommitBatchRequest batchRequest = commitBatchResponse.request();
        if (batchRequest == null) {
            return;
        }
        final int offset = batchRequest.byteOffset();
        final int length = batchRequest.size();
        final CacheKey cacheKey = CacheKeyCreator.create(objectKey, new ByteRange(offset, length));
        final byte[] cacheData = new byte[length];
        System.arraycopy(data, offset, cacheData, 0, length);
        FileExtent extent = new FileExtent()
                .setObject(objectKey.value())
                .setRange(new FileExtent.ByteRange()
                        .setOffset(offset)
                        .setLength(length))
                .setData(cacheData);
        TimeUtils.measureDurationMs(time, () -> cache.put(cacheKey, extent), cacheStoreDurationCallback);
    }
}
