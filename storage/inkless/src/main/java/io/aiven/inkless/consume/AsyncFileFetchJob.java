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

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.Storage;

/**
 * Async file fetch job using the unified Storage interface.
 */
public class AsyncFileFetchJob {

    private final Time time;
    private final Storage storage;
    private final ObjectKey key;
    private final ByteRange range;
    private final Consumer<Long> durationCallback;

    public AsyncFileFetchJob(final Time time,
                             final Storage storage,
                             final ObjectKey key,
                             final ByteRange range,
                             final Consumer<Long> durationCallback) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.storage = Objects.requireNonNull(storage, "storage cannot be null");
        this.key = Objects.requireNonNull(key, "key cannot be null");
        this.range = Objects.requireNonNull(range, "range cannot be null");
        this.durationCallback = Objects.requireNonNull(durationCallback, "durationCallback cannot be null");
    }

    /**
     * Fetches the file extent asynchronously.
     *
     * @return CompletableFuture that completes with the FileExtent
     */
    public CompletableFuture<FileExtent> fetchAsync() {
        final Instant start = TimeUtils.durationMeasurementNow(time);

        return storage.fetch(key, range)
            .thenApply(buffer -> createFileExtent(key, range, buffer))
            .whenComplete((result, error) -> {
                final Instant end = TimeUtils.durationMeasurementNow(time);
                final long durationMs = Duration.between(start, end).toMillis();
                durationCallback.accept(durationMs);
            });
    }

    private static FileExtent createFileExtent(final ObjectKey object,
                                               final ByteRange byteRange,
                                               final ByteBuffer buffer) {
        // Copy buffer content to byte array since buffer may be read-only (CRT returns direct buffers)
        final byte[] data = new byte[buffer.remaining()];
        buffer.get(data);

        return new FileExtent()
            .setObject(object.value())
            .setRange(new FileExtent.ByteRange()
                .setOffset(byteRange.offset())
                .setLength(data.length))
            .setData(data);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final AsyncFileFetchJob that = (AsyncFileFetchJob) o;
        return Objects.equals(storage, that.storage)
            && Objects.equals(key, that.key)
            && Objects.equals(range, that.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storage, key, range);
    }
}
