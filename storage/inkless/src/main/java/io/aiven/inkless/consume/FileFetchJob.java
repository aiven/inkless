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

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

public class FileFetchJob implements Callable<FileExtent> {

    private final Time time;
    private final ObjectFetcher objectFetcher;
    private final ObjectKey key;
    private final ByteRange range;
    private final int size;
    private final Consumer<Long> durationCallback;

    public FileFetchJob(Time time,
                        ObjectFetcher objectFetcher,
                        ObjectKey key,
                        ByteRange range,
                        Consumer<Long> durationCallback) {
        this.time = time;
        this.objectFetcher = objectFetcher;
        this.key = key;
        this.range = range;
        this.durationCallback = durationCallback;
        this.size = range.bufferSize();
    }

    // visible for testing
    static FileExtent createFileExtent(ObjectKey object, ByteRange byteRange, ByteBuffer buffer) {
        return new FileExtent()
                .setObject(object.value())
                .setRange(new FileExtent.ByteRange()
                        .setOffset(byteRange.offset())
                        .setLength(buffer.limit()))
                .setData(buffer.array());
    }

    @Override
    public FileExtent call() throws Exception {
        return TimeUtils.measureDurationMs(time, this::doWork, durationCallback);
    }

    private FileExtent doWork() throws Exception {
        try (InputStream stream = objectFetcher.fetch(key, range)) {
            byte[] bytes = stream.readNBytes(size);
            return createFileExtent(key, range, ByteBuffer.wrap(bytes));
        } catch (final Exception e) {
            throw new FileFetchException(e);
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileFetchJob that = (FileFetchJob) o;
        return size == that.size
                && Objects.equals(objectFetcher, that.objectFetcher)
                && Objects.equals(key, that.key)
                && Objects.equals(range, that.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectFetcher, key, range, size);
    }
}
