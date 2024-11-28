// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import org.apache.kafka.common.utils.Time;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

public class FileFetchJob implements Callable<FetchedFile> {

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
        if (range.size() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot fetch size " + range.size() + " more than " + Integer.MAX_VALUE);
        }
        this.size = (int) range.size();
    }

    @Override
    public FetchedFile call() throws Exception {
        return TimeUtils.measureDurationMs(time, this::doWork, durationCallback);
    }

    private FetchedFile doWork() throws StorageBackendException, IOException {
        try (InputStream stream = objectFetcher.fetch(key, range)) {
            byte[] bytes = stream.readNBytes(size);
            return new FetchedFile(key, range, ByteBuffer.wrap(bytes));
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
