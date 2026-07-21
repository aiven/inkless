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
package io.aiven.inkless.cache;

import org.apache.kafka.common.cache.Cache;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public interface ObjectCache extends Cache<CacheKey, FileExtent>, Closeable {
    /**
     * Asynchronously computes the value if absent, using the provided executor for the computation.
     * This method always returns a {@link CompletableFuture} promptly, regardless of whether the key
     * is already present in the cache. The cache lookup and bookkeeping may still perform synchronous
     * work on the calling thread, but on a cache miss the actual load computation is executed on the
     * supplied {@code loadExecutor}.
     *
     * @param key the cache key
     * @param load the function to compute the value if absent
     * @param loadExecutor the executor to use for async computation on cache miss
     * @return a CompletableFuture that will complete with the cached or computed value
     */
    CompletableFuture<FileExtent> computeIfAbsent(
        CacheKey key,
        Function<CacheKey, FileExtent> load,
        Executor loadExecutor
    );
}
