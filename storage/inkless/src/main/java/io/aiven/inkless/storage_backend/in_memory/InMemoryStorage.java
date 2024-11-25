// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.in_memory;

import org.apache.commons.io.input.BoundedInputStream;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

/**
 * The in-memory implementation of {@link StorageBackend}.
 *
 * <p>Useful for testing.
 */
public class InMemoryStorage implements StorageBackend {
    private final ConcurrentHashMap<ObjectKey, byte[]> storage = new ConcurrentHashMap<>();

    @Override
    public void configure(final Map<String, ?> configs) {
        // do nothing
    }

    @Override
    public void upload(final ObjectKey key, final byte[] data) throws StorageBackendException {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(data, "data cannot be null");
        storage.put(key, data);
    }

    @Override
    public InputStream fetch(final ObjectKey key, final ByteRange range) throws StorageBackendException {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(range, "range cannot be null");

        final byte[] data = storage.get(key);
        if (data == null) {
            throw new KeyNotFoundException(this, key);
        }

        if (range.offset() >= data.length) {
            throw new InvalidRangeException("Range start offset " + range.offset()
                + " is outside of data size " + data.length);
        }

        final ByteArrayInputStream inner = new ByteArrayInputStream(data);
        inner.skip(range.offset());
        return new BoundedInputStream(inner, range.size());
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        Objects.requireNonNull(key, "key cannot be null");
        storage.remove(key);
    }

    @Override
    public void delete(final Set<ObjectKey> keys) throws StorageBackendException {
        Objects.requireNonNull(keys, "keys cannot be null");
        keys.forEach(storage::remove);
    }
}
