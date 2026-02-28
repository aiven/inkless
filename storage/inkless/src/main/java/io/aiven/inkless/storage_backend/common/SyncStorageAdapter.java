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
package io.aiven.inkless.storage_backend.common;

import org.apache.kafka.common.utils.ByteBufferInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;

/**
 * Adapter that wraps a synchronous {@link StorageBackend} to implement the unified {@link Storage} interface.
 *
 * <p>This adapter enables code that uses the async-first {@link Storage} interface to work with
 * existing synchronous storage implementations. The sync operations are executed on the calling
 * thread and wrapped in completed/failed CompletableFutures.
 *
 * <p>Note: Unlike true async implementations (e.g., CRT-backed S3AsyncStorage), this adapter
 * blocks the calling thread during I/O operations. The CompletableFuture wrapper provides
 * API consistency but not non-blocking behavior.
 */
public class SyncStorageAdapter implements Storage {

    private final StorageBackend delegate;

    public SyncStorageAdapter(final StorageBackend delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate cannot be null");
    }

    @Override
    public CompletableFuture<Void> upload(final ObjectKey key, final ByteBuffer buffer) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(buffer, "buffer cannot be null");

        try {
            final int length = buffer.remaining();
            if (length <= 0) {
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException("buffer must have remaining bytes"));
            }
            delegate.upload(key, new ByteBufferInputStream(buffer), length);
            return CompletableFuture.completedFuture(null);
        } catch (final Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<ByteBuffer> fetch(final ObjectKey key, final ByteRange range) {
        Objects.requireNonNull(key, "key cannot be null");

        try {
            if (range != null && range.empty()) {
                return CompletableFuture.completedFuture(ByteBuffer.allocate(0));
            }
            final var channel = delegate.fetch(key, range);
            final ByteBuffer result = delegate.readToByteBuffer(channel);
            return CompletableFuture.completedFuture(result);
        } catch (final Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> delete(final ObjectKey key) {
        Objects.requireNonNull(key, "key cannot be null");

        try {
            delegate.delete(key);
            return CompletableFuture.completedFuture(null);
        } catch (final Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> delete(final Set<ObjectKey> keys) {
        Objects.requireNonNull(keys, "keys cannot be null");

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        try {
            delegate.delete(keys);
            return CompletableFuture.completedFuture(null);
        } catch (final Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /**
     * Returns the underlying StorageBackend delegate.
     * Useful for cases that still need direct access to the sync API.
     */
    public StorageBackend delegate() {
        return delegate;
    }
}
