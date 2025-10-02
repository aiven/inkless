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
import org.apache.kafka.common.cache.LRUCache;

import java.io.IOException;
import java.util.function.Function;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public class MemoryCache implements ObjectCache {

    private final Cache<CacheKey, FileExtent> backingCache = new LRUCache<>(10);
    @Override
    public FileExtent get(CacheKey key) {
        return backingCache.get(key);
    }

    @Override
    public void put(CacheKey key, FileExtent value) {
        backingCache.put(key, value);
    }

    @Override
    public FileExtent computeIfAbsent(CacheKey key, Function<CacheKey, FileExtent> mappingFunction) {
        final FileExtent fileExtent = backingCache.get(key);
        if (fileExtent == null) {
            final FileExtent newFileExtent = mappingFunction.apply(key);
            backingCache.put(key, newFileExtent);
            return newFileExtent;
        }
        return fileExtent;
    }

    @Override
    public boolean remove(CacheKey key) {
        return backingCache.remove(key);
    }

    @Override
    public long size() {
        return backingCache.size();
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

}
