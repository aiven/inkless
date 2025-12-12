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
import java.util.function.Function;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public interface ObjectCache extends Cache<CacheKey, FileExtent>, Closeable {
    FileExtent computeIfAbsent(CacheKey key, Function<CacheKey, FileExtent> mappingFunction);

    /**
     * Computes if absent with batch timestamp hint for cache tiering.
     *
     * <p>The batch timestamp can be used by tiered cache implementations to route
     * requests to the appropriate cache tier (hot vs cold) based on data age.</p>
     *
     * <p>Default implementation ignores the timestamp and delegates to the regular
     * computeIfAbsent method.</p>
     *
     * @param key the cache key
     * @param mappingFunction the function to compute the value if absent
     * @param batchTimestamp the timestamp of the batch (from BatchMetadata.timestamp())
     * @return the cached or computed file extent
     */
    default FileExtent computeIfAbsent(CacheKey key, Function<CacheKey, FileExtent> mappingFunction, long batchTimestamp) {
        return computeIfAbsent(key, mappingFunction);
    }
}
