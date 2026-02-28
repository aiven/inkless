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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;

/**
 * Unified storage interface providing async operations via CompletableFuture.
 *
 * <p>This interface abstracts storage operations behind an async-first API. Implementations can be:
 * <ul>
 *   <li>True async (e.g., AWS CRT-backed S3AsyncStorage) - non-blocking I/O</li>
 *   <li>Sync adapter (e.g., SyncStorageAdapter wrapping StorageBackend) - blocking operations
 *       wrapped in CompletableFuture for API consistency</li>
 * </ul>
 *
 * <p>Components using this interface don't need to know which implementation they're using,
 * enabling unified code paths without null checks or conditional branching.
 */
public interface Storage extends Closeable {

    /**
     * Uploads an object to object storage.
     *
     * @param key    key of the object to upload
     * @param buffer data of the object that will be uploaded
     * @return a CompletableFuture that completes when the upload is finished
     */
    CompletableFuture<Void> upload(ObjectKey key, ByteBuffer buffer);

    /**
     * Fetches an object from object storage.
     *
     * @param key   key of the object to fetch
     * @param range optional byte range to fetch (null for entire object)
     * @return a CompletableFuture containing the fetched data as ByteBuffer
     */
    CompletableFuture<ByteBuffer> fetch(ObjectKey key, ByteRange range);

    /**
     * Deletes an object from object storage.
     *
     * <p>If the object doesn't exist, the operation still succeeds as it is idempotent.
     *
     * @param key key of the object to delete
     * @return a CompletableFuture that completes when the deletion is finished
     */
    CompletableFuture<Void> delete(ObjectKey key);

    /**
     * Deletes multiple objects from object storage.
     *
     * <p>If an object doesn't exist, the operation still succeeds as it is idempotent.
     *
     * @param keys set of keys to delete
     * @return a CompletableFuture that completes when all deletions are finished
     */
    CompletableFuture<Void> delete(Set<ObjectKey> keys);
}
