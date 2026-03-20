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

import org.junit.jupiter.api.Test;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

import static org.assertj.core.api.Assertions.assertThat;

class CaffeineCacheTest {

    @Test
    void countBasedEvictionWhenBytesDisabled() throws Exception {
        // maxCacheBytes = 0 disables byte-based eviction, uses count-based (maxCacheSize = 2)
        try (CaffeineCache cache = new CaffeineCache(2, 0, 3600, 180)) {
            cache.put(key("a"), extent(10));
            cache.put(key("b"), extent(10));
            cache.cleanUp();
            assertThat(cache.size()).isEqualTo(2);

            cache.put(key("c"), extent(10));
            cache.cleanUp();
            assertThat(cache.size()).isLessThanOrEqualTo(2);
        }
    }

    @Test
    void bytesBasedEvictionWhenBytesConfigured() throws Exception {
        // maxCacheBytes = 100 bytes; each entry is 50 bytes => max ~2 entries
        try (CaffeineCache cache = new CaffeineCache(1000, 100, 3600, 180)) {
            cache.put(key("a"), extent(50));
            cache.put(key("b"), extent(50));
            cache.put(key("c"), extent(50));
            cache.cleanUp();

            // With 100 bytes max and 50 bytes per entry, at most 2 entries should remain
            assertThat(cache.size()).isLessThanOrEqualTo(2);
        }
    }

    @Test
    void bytesBasedEvictionIgnoresCountLimit() throws Exception {
        // maxCacheSize = 1 but bytes takes precedence; 200 bytes fits 4x50-byte entries
        try (CaffeineCache cache = new CaffeineCache(1, 200, 3600, 180)) {
            cache.put(key("a"), extent(50));
            cache.put(key("b"), extent(50));
            cache.put(key("c"), extent(50));
            cache.cleanUp();

            // Count limit (1) is ignored because bytes limit is configured
            assertThat(cache.size()).isGreaterThan(1);
        }
    }

    private static CacheKey key(String id) {
        return new CacheKey()
            .setObject(id)
            .setRange(new CacheKey.ByteRange().setOffset(0).setLength(1));
    }

    private static FileExtent extent(int dataSize) {
        return new FileExtent().setData(new byte[dataSize]);
    }
}
