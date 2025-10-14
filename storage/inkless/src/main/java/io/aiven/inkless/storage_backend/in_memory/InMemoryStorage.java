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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
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
    public void upload(final ObjectKey key, final InputStream inputStream, final long length) throws StorageBackendException {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(inputStream, "inputStream cannot be null");
        if (length <= 0) {
            throw new IllegalArgumentException("length must be positive");
        }
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            long transferred = inputStream.transferTo(byteArrayOutputStream);
            if (transferred != length) {
                throw new StorageBackendException(
                        "Object " + key + " created with incorrect length " + transferred + " instead of " + length);
            }
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }

        storage.put(key, byteArrayOutputStream.toByteArray());
    }

    @Override
    public ReadableByteChannel fetch(final ObjectKey key, final ByteRange range) throws StorageBackendException, IOException {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(range, "range cannot be null");

        final byte[] data = storage.get(key);
        if (data == null) {
            throw new KeyNotFoundException(this, key);
        }

        if (range.offset() >= data.length) {
            throw new InvalidRangeException("Failed to fetch " + key + ": Invalid range " + range + " for blob size " + data.length);
        }

        final int dataSize;
        if (range.size() > data.length - range.offset()) {
            dataSize = Math.toIntExact((data.length - range.offset()));
        }  else {
            dataSize = Math.toIntExact(Math.min(range.size(), data.length));
        }
        final int copyLength = Math.toIntExact(Math.min(range.size(), data.length - 1));
        final byte[] dstData = new byte[dataSize];
        System.arraycopy(data, range.bufferOffset(), dstData, 0, copyLength);

        return Channels.newChannel(new ByteArrayInputStream(dstData));
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

    @Override
    public void close() throws IOException {
        storage.clear();
    }
}
