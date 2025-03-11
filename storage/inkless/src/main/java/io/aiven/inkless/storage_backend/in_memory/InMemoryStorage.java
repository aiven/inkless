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
package io.aiven.inkless.storage_backend.in_memory;

import org.apache.commons.io.input.BoundedInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
    public void uploadMultiPart(final ObjectKey key, final InputStream inputStream) throws StorageBackendException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            inputStream.transferTo(byteArrayOutputStream);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }

        storage.put(key, byteArrayOutputStream.toByteArray());
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
