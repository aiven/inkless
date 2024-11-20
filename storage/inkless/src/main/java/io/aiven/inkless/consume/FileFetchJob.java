// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

public class FileFetchJob implements Callable<FetchedFile> {

    private final ObjectFetcher objectFetcher;
    private final ObjectKey key;
    private final ByteRange range;
    private final int size;

    public FileFetchJob(ObjectFetcher objectFetcher, ObjectKey key, ByteRange range) {
        this.objectFetcher = objectFetcher;
        this.key = key;
        this.range = range;
        if (range.size() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Cannot fetch size " + range.size() + " more than " + Integer.MAX_VALUE);
        }
        this.size = (int) range.size();
    }

    @Override
    public FetchedFile call() throws Exception {
        try (InputStream stream = objectFetcher.fetch(key, range)) {
            byte[] bytes = stream.readNBytes(size);
            return new FetchedFile(key, range, ByteBuffer.wrap(bytes));
        }
    }
}
