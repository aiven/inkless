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
package io.aiven.inkless.produce;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.aiven.inkless.control_plane.MetadataView;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LogConfigCacheTest {

    private MetadataView metadataView;
    private LogConfig defaultLogConfig;
    private Supplier<LogConfig> defaultConfigSupplier;
    private LogConfigCache cache;

    @BeforeEach
    void setUp() {
        metadataView = mock(MetadataView.class);
        defaultLogConfig = createDefaultLogConfig();
        defaultConfigSupplier = () -> defaultLogConfig;
        cache = new LogConfigCache(metadataView, defaultConfigSupplier);
    }

    @Test
    void shouldReturnCachedConfigOnSubsequentCalls() {
        // Given
        final String topic = "test-topic";
        final Properties overrides = new Properties();
        when(metadataView.getTopicConfig(topic)).thenReturn(overrides);

        // When
        final LogConfig first = cache.get(topic);
        final LogConfig second = cache.get(topic);

        // Then
        assertNotNull(first);
        assertSame(first, second, "Should return same cached instance");
        assertEquals(1, cache.size());
        // getTopicConfig is called each time to check for changes
        verify(metadataView, times(2)).getTopicConfig(topic);
    }

    @Test
    void shouldInvalidateCacheWhenOverridesChange() {
        // Given
        final String topic = "test-topic";
        final Properties overrides1 = new Properties();
        overrides1.setProperty(TopicConfig.RETENTION_MS_CONFIG, "1000");

        final Properties overrides2 = new Properties();
        overrides2.setProperty(TopicConfig.RETENTION_MS_CONFIG, "2000");

        when(metadataView.getTopicConfig(topic))
            .thenReturn(overrides1)
            .thenReturn(overrides1)
            .thenReturn(overrides2);

        // When
        final LogConfig first = cache.get(topic);
        final LogConfig second = cache.get(topic);
        final LogConfig third = cache.get(topic);

        // Then
        assertSame(first, second, "Should return cached instance when overrides unchanged");
        assertNotSame(second, third, "Should create new instance when overrides change");
        assertEquals(1000L, first.retentionMs);
        assertEquals(2000L, third.retentionMs);
    }

    @Test
    void shouldCacheMultipleTopics() {
        // Given
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";
        final Properties overrides1 = new Properties();
        final Properties overrides2 = new Properties();
        overrides2.setProperty(TopicConfig.RETENTION_MS_CONFIG, "5000");

        when(metadataView.getTopicConfig(topic1)).thenReturn(overrides1);
        when(metadataView.getTopicConfig(topic2)).thenReturn(overrides2);

        // When
        final Map<String, LogConfig> configs = cache.getAll(Set.of(topic1, topic2));

        // Then
        assertEquals(2, configs.size());
        assertEquals(2, cache.size());
        assertNotNull(configs.get(topic1));
        assertNotNull(configs.get(topic2));
        assertEquals(5000L, configs.get(topic2).retentionMs);
    }

    @Test
    void shouldInvalidateSpecificTopic() {
        // Given
        final String topic = "test-topic";
        final Properties overrides = new Properties();
        when(metadataView.getTopicConfig(topic)).thenReturn(overrides);

        final LogConfig first = cache.get(topic);
        assertEquals(1, cache.size());

        // When
        cache.invalidate(topic);

        // Then
        assertEquals(0, cache.size());

        // Next get should create new instance
        final LogConfig second = cache.get(topic);
        assertNotSame(first, second);
    }

    @Test
    void shouldInvalidateAllTopics() {
        // Given
        final String topic1 = "topic-1";
        final String topic2 = "topic-2";
        when(metadataView.getTopicConfig(topic1)).thenReturn(new Properties());
        when(metadataView.getTopicConfig(topic2)).thenReturn(new Properties());

        cache.get(topic1);
        cache.get(topic2);
        assertEquals(2, cache.size());

        // When
        cache.invalidateAll();

        // Then
        assertEquals(0, cache.size());
    }

    @Test
    void shouldHandleEmptyOverrides() {
        // Given
        final String topic = "test-topic";
        when(metadataView.getTopicConfig(topic)).thenReturn(new Properties());

        // When
        final LogConfig config = cache.get(topic);

        // Then
        assertNotNull(config);
        assertEquals(1, cache.size());
    }

    @Test
    void shouldHandleNullOverrides() {
        // Given
        final String topic = "test-topic";
        when(metadataView.getTopicConfig(topic)).thenReturn(null);

        // When
        final LogConfig config = cache.get(topic);

        // Then
        assertNotNull(config);
        assertEquals(1, cache.size());
    }

    @Test
    void shouldRefreshDefaultsWhenSupplierReturnsNewInstance() {
        // Given
        final String topic = "test-topic";
        when(metadataView.getTopicConfig(topic)).thenReturn(new Properties());

        final AtomicInteger callCount = new AtomicInteger(0);
        final LogConfig defaultConfig1 = createDefaultLogConfig();
        final LogConfig defaultConfig2 = createDefaultLogConfig();

        final Supplier<LogConfig> changingSupplier = () -> {
            return callCount.incrementAndGet() <= 2 ? defaultConfig1 : defaultConfig2;
        };

        final LogConfigCache cacheWithChangingDefaults = new LogConfigCache(metadataView, changingSupplier);

        // When
        final LogConfig first = cacheWithChangingDefaults.get(topic);
        final LogConfig second = cacheWithChangingDefaults.get(topic);

        // Then - both should use cached defaults
        assertSame(first, second);
    }

    private LogConfig createDefaultLogConfig() {
        final Properties props = new Properties();
        props.setProperty(TopicConfig.SEGMENT_BYTES_CONFIG, String.valueOf(1024 * 1024 * 1024));
        props.setProperty(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(7 * 24 * 60 * 60 * 1000L));
        return new LogConfig(props);
    }
}
