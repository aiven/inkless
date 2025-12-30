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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

/**
 * A cache implementation without any storage, always misses.
 */
public class NullCache implements ObjectCache {
    @Override
    public FileExtent get(CacheKey key) {
        return null;
    }

    @Override
    public void put(CacheKey key, FileExtent value) {
    }

    @Override
    public CompletableFuture<FileExtent> computeIfAbsent(
        CacheKey key,
        Function<CacheKey, FileExtent> load,
        Executor loadExecutor
    ) {
        // Always a cache miss, compute on executor
        return CompletableFuture.supplyAsync(() -> load.apply(key), loadExecutor);
    }

    @Override
    public boolean remove(CacheKey key) {
        return false;
    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void close() throws IOException {
        // no-op
    }
}
