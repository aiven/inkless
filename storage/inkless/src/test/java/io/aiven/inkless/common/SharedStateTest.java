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
package io.aiven.inkless.common;

import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.aiven.inkless.cache.CaffeineCache;
import io.aiven.inkless.cache.TieredObjectCache;
import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.control_plane.MetadataView;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class SharedStateTest {

    private final MockTime time = new MockTime();

    @Test
    void initializeWithLaggingCacheDisabled() throws IOException {
        // Given: Config with lagging cache disabled (default)
        Map<String, String> configs = minimalConfig();
        configs.put("consume.lagging.cache.enabled", "false");
        InklessConfig config = new InklessConfig(configs);

        // When
        SharedState state = SharedState.initialize(
            time,
            1,
            config,
            mock(MetadataView.class),
            new InMemoryControlPlane(time),
            new BrokerTopicStats(),
            () -> new LogConfig(new Properties())
        );

        // Then: Cache should be a regular CaffeineCache, not TieredObjectCache
        assertThat(state.cache()).isInstanceOf(CaffeineCache.class);

        state.close();
    }

    @Test
    void initializeWithLaggingCacheEnabled() throws IOException {
        // Given: Config with lagging cache enabled
        Map<String, String> configs = minimalConfig();
        configs.put("consume.lagging.cache.enabled", "true");
        configs.put("consume.lagging.cache.max.count", "100");
        configs.put("consume.lagging.cache.ttl.sec", "10");
        configs.put("consume.lagging.cache.rate.limit.bytes.per.sec", "52428800");
        InklessConfig config = new InklessConfig(configs);

        // When
        SharedState state = SharedState.initialize(
            time,
            1,
            config,
            mock(MetadataView.class),
            new InMemoryControlPlane(time),
            new BrokerTopicStats(),
            () -> new LogConfig(new Properties())
        );

        // Then: Cache should be a TieredObjectCache
        assertThat(state.cache()).isInstanceOf(TieredObjectCache.class);

        state.close();
    }

    @Test
    void initializeWithLaggingCacheRateLimitDisabled() throws IOException {
        // Given: Config with lagging cache enabled but rate limiting disabled
        Map<String, String> configs = minimalConfig();
        configs.put("consume.lagging.cache.enabled", "true");
        configs.put("consume.lagging.cache.rate.limit.bytes.per.sec", "-1");
        InklessConfig config = new InklessConfig(configs);

        // When
        SharedState state = SharedState.initialize(
            time,
            1,
            config,
            mock(MetadataView.class),
            new InMemoryControlPlane(time),
            new BrokerTopicStats(),
            () -> new LogConfig(new Properties())
        );

        // Then: Cache should still be a TieredObjectCache (rate limiting is optional)
        assertThat(state.cache()).isInstanceOf(TieredObjectCache.class);

        state.close();
    }

    private Map<String, String> minimalConfig() {
        Map<String, String> configs = new HashMap<>();
        configs.put("control.plane.class", InMemoryControlPlane.class.getCanonicalName());
        configs.put("storage.backend.class", "io.aiven.inkless.config.ConfigTestStorageBackend");
        return configs;
    }
}
