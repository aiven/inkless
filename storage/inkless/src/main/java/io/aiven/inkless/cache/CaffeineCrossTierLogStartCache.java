/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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

import org.apache.kafka.common.TopicIdPartition;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;

import java.io.IOException;
import java.time.Duration;

/**
 * Caffeine-backed {@link CrossTierLogStartCache} with a fixed time-to-live applied from the last
 * write. Entries are advanced monotonically.
 */
public class CaffeineCrossTierLogStartCache implements CrossTierLogStartCache {

    private final Cache<TopicIdPartition, Long> cache;
    private final CrossTierLogStartCacheMetrics metrics;

    public CaffeineCrossTierLogStartCache(final Duration ttl) {
        this(ttl, Ticker.systemTicker());
    }

    // Visible for testing: allows a controllable ticker to exercise TTL expiry deterministically.
    CaffeineCrossTierLogStartCache(final Duration ttl, final Ticker ticker) {
        if (ttl == null || ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("TTL must be a positive duration.");
        }
        this.cache = Caffeine.newBuilder()
            .expireAfterWrite(ttl)
            .ticker(ticker)
            .build();
        this.metrics = new CrossTierLogStartCacheMetrics(cache::estimatedSize);
    }

    @Override
    public Long get(final TopicIdPartition topicIdPartition) {
        final Long offset = cache.getIfPresent(topicIdPartition);
        if (offset == null) {
            metrics.recordCacheMiss();
        } else {
            metrics.recordCacheHit();
        }
        return offset;
    }

    @Override
    public void put(final TopicIdPartition topicIdPartition, final long offset) {
        if (offset < 0) {
            return;
        }
        cache.asMap().merge(topicIdPartition, offset, Math::max);
    }

    @Override
    public void close() throws IOException {
        cache.invalidateAll();
        cache.cleanUp();
        metrics.close();
    }
}
