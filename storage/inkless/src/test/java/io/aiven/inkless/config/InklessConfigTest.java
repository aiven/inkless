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
package io.aiven.inkless.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import io.aiven.inkless.control_plane.InMemoryControlPlane;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InklessConfigTest {

    private Metrics storageMetrics;

    @Test
    void publicConstructor() {
        final String controlPlaneClass = InMemoryControlPlane.class.getCanonicalName();
        final Map<String, String> configs = new HashMap<>();
        configs.put("inkless.control.plane.class", controlPlaneClass);
        configs.put("inkless.object.key.prefix", "prefix/");
        configs.put("inkless.produce.commit.interval.ms", "100");
        configs.put("inkless.produce.buffer.max.bytes", "1024");
        configs.put("inkless.produce.max.upload.attempts", "5");
        configs.put("inkless.produce.upload.backoff.ms", "30");
        configs.put("inkless.storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName());
        configs.put("inkless.file.cleaner.interval.ms", "100");
        configs.put("inkless.file.cleaner.retention.period.ms", "200");
        configs.put("inkless.file.merger.interval.ms", "100");
        configs.put("inkless.consume.cache.max.count", "100");
        configs.put("inkless.consume.cache.expiration.lifespan.sec", "200");
        configs.put("inkless.consume.cache.expiration.max.idle.sec", "100");
        configs.put("inkless.produce.upload.thread.pool.size", "16");
        configs.put("inkless.fetch.data.thread.pool.size", "12");
        configs.put("inkless.fetch.metadata.thread.pool.size", "14");
        configs.put("inkless.retention.enforcement.max.batches.per.request", "50");
        final InklessConfig config = new InklessConfig(new AbstractConfig(new ConfigDef(), configs));
        assertThat(config.controlPlaneClass()).isEqualTo(InMemoryControlPlane.class);
        assertThat(config.controlPlaneConfig()).isEqualTo(Map.of("class", controlPlaneClass));
        assertThat(config.objectKeyPrefix()).isEqualTo("prefix/");
        assertThat(config.commitInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.produceBufferMaxBytes()).isEqualTo(1024);
        assertThat(config.produceMaxUploadAttempts()).isEqualTo(5);
        assertThat(config.produceUploadBackoff()).isEqualTo(Duration.ofMillis(30));
        storageMetrics = new Metrics(new MockTime());
        assertThat(config.storage(storageMetrics)).isInstanceOf(ConfigTestStorageBackend.class);
        assertThat(config.fileCleanerInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.fileCleanerRetentionPeriod()).isEqualTo(Duration.ofMillis(200));
        assertThat(config.fileMergerInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.cacheMaxCount()).isEqualTo(100);
        assertThat(config.cacheExpirationLifespanSec()).isEqualTo(200);
        assertThat(config.cacheExpirationMaxIdleSec()).isEqualTo(100);
        assertThat(config.produceUploadThreadPoolSize()).isEqualTo(16);
        assertThat(config.fetchDataThreadPoolSize()).isEqualTo(12);
        assertThat(config.fetchMetadataThreadPoolSize()).isEqualTo(14);
        assertThat(config.maxBatchesPerEnforcementRequest()).isEqualTo(50);
    }

    @Test
    void minimalConfig() {
        final String controlPlaneClass = InMemoryControlPlane.class.getCanonicalName();
        final var config = new InklessConfig(
            Map.of(
                "control.plane.class", controlPlaneClass,
                "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName()
            )
        );
        assertThat(config.controlPlaneClass()).isEqualTo(InMemoryControlPlane.class);
        assertThat(config.controlPlaneConfig()).isEqualTo(Map.of("class", controlPlaneClass));
        assertThat(config.objectKeyPrefix()).isEqualTo("");
        assertThat(config.commitInterval()).isEqualTo(Duration.ofMillis(250));
        assertThat(config.produceBufferMaxBytes()).isEqualTo(8 * 1024 * 1024);
        assertThat(config.produceMaxUploadAttempts()).isEqualTo(3);
        assertThat(config.produceUploadBackoff()).isEqualTo(Duration.ofMillis(10));
        assertThat(config.storage(storageMetrics)).isInstanceOf(ConfigTestStorageBackend.class);
        assertThat(config.fileCleanerInterval()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.fileCleanerRetentionPeriod()).isEqualTo(Duration.ofMinutes(1));
        assertThat(config.fileMergerInterval()).isEqualTo(Duration.ofMinutes(1));
        assertThat(config.cacheMaxCount()).isEqualTo(1000);
        assertThat(config.cacheExpirationLifespanSec()).isEqualTo(60);
        assertThat(config.cacheExpirationMaxIdleSec()).isEqualTo(-1);
        assertThat(config.produceUploadThreadPoolSize()).isEqualTo(8);
        assertThat(config.fetchDataThreadPoolSize()).isEqualTo(32);
        assertThat(config.fetchMetadataThreadPoolSize()).isEqualTo(8);
        assertThat(config.maxBatchesPerEnforcementRequest()).isEqualTo(0);
    }

    @Test
    void fullConfig() {
        final String controlPlaneClass = InMemoryControlPlane.class.getCanonicalName();
        Map<String, String> configs = new HashMap<>();
        configs.put("control.plane.class", controlPlaneClass);
        configs.put("object.key.prefix", "prefix/");
        configs.put("produce.commit.interval.ms", "100");
        configs.put("produce.buffer.max.bytes", "1024");
        configs.put("produce.max.upload.attempts", "5");
        configs.put("produce.upload.backoff.ms", "30");
        configs.put("storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName());
        configs.put("file.cleaner.interval.ms", "100");
        configs.put("file.cleaner.retention.period.ms", "200");
        configs.put("file.merger.interval.ms", "100");
        configs.put("consume.cache.max.count", "100");
        configs.put("consume.cache.expiration.lifespan.sec", "200");
        configs.put("consume.cache.expiration.max.idle.sec", "100");
        configs.put("produce.upload.thread.pool.size", "16");
        configs.put("fetch.data.thread.pool.size", "12");
        configs.put("fetch.metadata.thread.pool.size", "14");
        configs.put("fetch.lagging.consumer.thread.pool.size", "20");
        configs.put("fetch.lagging.consumer.threshold.ms", "240000");  // 4 minutes
        configs.put("fetch.lagging.consumer.request.rate.limit", "250");
        configs.put("retention.enforcement.max.batches.per.request", "10");
        final var config = new InklessConfig(
            configs
        );
        assertThat(config.controlPlaneClass()).isEqualTo(InMemoryControlPlane.class);
        assertThat(config.controlPlaneConfig()).isEqualTo(Map.of("class", controlPlaneClass));
        assertThat(config.objectKeyPrefix()).isEqualTo("prefix/");
        assertThat(config.commitInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.produceBufferMaxBytes()).isEqualTo(1024);
        assertThat(config.produceMaxUploadAttempts()).isEqualTo(5);
        assertThat(config.produceUploadBackoff()).isEqualTo(Duration.ofMillis(30));
        assertThat(config.storage(storageMetrics)).isInstanceOf(ConfigTestStorageBackend.class);
        assertThat(config.fileCleanerInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.fileCleanerRetentionPeriod()).isEqualTo(Duration.ofMillis(200));
        assertThat(config.fileMergerInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.cacheMaxCount()).isEqualTo(100);
        assertThat(config.cacheExpirationLifespanSec()).isEqualTo(200);
        assertThat(config.cacheExpirationMaxIdleSec()).isEqualTo(100);
        assertThat(config.produceUploadThreadPoolSize()).isEqualTo(16);
        assertThat(config.fetchDataThreadPoolSize()).isEqualTo(12);
        assertThat(config.fetchMetadataThreadPoolSize()).isEqualTo(14);
        assertThat(config.fetchLaggingConsumerThreadPoolSize()).isEqualTo(20);
        assertThat(config.fetchLaggingConsumerThresholdMs()).isEqualTo(240_000L);
        assertThat(config.fetchLaggingConsumerRequestRateLimit()).isEqualTo(250);
        assertThat(config.maxBatchesPerEnforcementRequest()).isEqualTo(10);
    }

    @Test
    void objectKeyPrefixNull() {
        final Map<String, String> config = new HashMap<>();
        config.put("control.plane.class", InMemoryControlPlane.class.getCanonicalName());
        config.put("storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName());
        config.put("object.key.prefix", null);
        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value null for configuration object.key.prefix: entry must be non null");
    }

    @Test
    void produceCommitIntervalZero() {
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "produce.commit.interval.ms", "0"
        );
        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration produce.commit.interval.ms: Value must be at least 1");
    }

    @Test
    void produceBufferMaxBytesZero() {
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "produce.buffer.max.bytes", "0"
        );
        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration produce.buffer.max.bytes: Value must be at least 1");
    }

    @Test
    void produceMaxUploadAttemptsZero() {
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "produce.max.upload.attempts", "0"
        );
        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration produce.max.upload.attempts: Value must be at least 1");
    }

    @Test
    void produceMaxUploadBackoffMsNegative() {
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "produce.upload.backoff.ms", "-1"
        );
        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value -1 for configuration produce.upload.backoff.ms: Value must be at least 0");
    }

    @Test
    void controlPlaneConfiguration() {
        final String controlPlaneClass = InMemoryControlPlane.class.getCanonicalName();
        final String backendClass = ConfigTestStorageBackend.class.getCanonicalName();
        final var config = new InklessConfig(
            Map.of(
                "control.plane.class", controlPlaneClass,
                "control.plane.a", "1",
                "control.plane.b", "str",
                "storage.backend.class", backendClass,
                "unrelated", "x"
            )
        );
        assertThat(config.controlPlaneConfig()).isEqualTo(Map.of(
            "class", controlPlaneClass,
            "a", "1",
            "b", "str"));
    }

    @Test
    void objectStorageConfiguration() {
        final String controlPlaneClass = InMemoryControlPlane.class.getCanonicalName();
        final String backendClass = ConfigTestStorageBackend.class.getCanonicalName();
        final var config = new InklessConfig(
            Map.of(
                "control.plane.class", controlPlaneClass,
                "storage.backend.class", backendClass,
                "storage.a", "1",
                "storage.b", "str",
                "unrelated", "x"
            )
        );
        assertThat(config.storage(storageMetrics)).isInstanceOf(ConfigTestStorageBackend.class);
        final var storage = (ConfigTestStorageBackend) config.storage(storageMetrics);
        assertThat(storage.passedConfig).isEqualTo(Map.of(
            "backend.class", backendClass,
            "a", "1",
            "b", "str"));
    }

    @Test
    void consumeCacheSizeLessThanOne() {
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "consume.cache.max.count", "0"
        );
        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration consume.cache.max.count: Value must be at least 1");
    }

    @Test
    void laggingConsumerConfigDefaults() {
        // Test that lagging consumer configs have correct default values
        final var config = new InklessConfig(
            Map.of(
                "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
                "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName()
            )
        );

        // Default thread pool size
        assertThat(config.fetchLaggingConsumerThreadPoolSize()).isEqualTo(16);

        // Default rate limit
        assertThat(config.fetchLaggingConsumerRequestRateLimit()).isEqualTo(200);

        // Default threshold: -1 (auto) should use heuristic: cache TTL
        // Default cache TTL is 60 seconds, so threshold should follow that
        assertThat(config.fetchLaggingConsumerThresholdMs()).isEqualTo(60_000L);
    }

    @Test
    void laggingConsumerConfigExplicitValues() {
        // Test that explicit config values override defaults
        final Map<String, String> configs = new HashMap<>();
        configs.put("control.plane.class", InMemoryControlPlane.class.getCanonicalName());
        configs.put("storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName());
        configs.put("fetch.lagging.consumer.thread.pool.size", "32");
        configs.put("fetch.lagging.consumer.request.rate.limit", "500");
        configs.put("fetch.lagging.consumer.threshold.ms", "300000");  // 5 minutes explicit

        final var config = new InklessConfig(configs);

        assertThat(config.fetchLaggingConsumerThreadPoolSize()).isEqualTo(32);
        assertThat(config.fetchLaggingConsumerRequestRateLimit()).isEqualTo(500);
        assertThat(config.fetchLaggingConsumerThresholdMs()).isEqualTo(300_000L);
    }

    @Test
    void laggingConsumerThreadPoolSizeCanBeZero() {
        // Test that thread pool size can be 0 (disables feature)
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "fetch.lagging.consumer.thread.pool.size", "0"
        );

        final var inklessConfig = new InklessConfig(config);
        assertThat(inklessConfig.fetchLaggingConsumerThreadPoolSize()).isEqualTo(0);
    }

    @Test
    void laggingConsumerThreadPoolSizeNegativeInvalid() {
        // Test that negative thread pool size is invalid
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "fetch.lagging.consumer.thread.pool.size", "-1"
        );

        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value -1 for configuration fetch.lagging.consumer.thread.pool.size: Value must be at least 0");
    }

    @Test
    void laggingConsumerRateLimitCanBeZero() {
        // Test that rate limit can be 0 (disables rate limiting)
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "fetch.lagging.consumer.request.rate.limit", "0"
        );

        final var inklessConfig = new InklessConfig(config);
        assertThat(inklessConfig.fetchLaggingConsumerRequestRateLimit()).isEqualTo(0);
    }

    @Test
    void laggingConsumerRateLimitNegativeInvalid() {
        // Test that negative rate limit is invalid
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "fetch.lagging.consumer.request.rate.limit", "-1"
        );

        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value -1 for configuration fetch.lagging.consumer.request.rate.limit: Value must be at least 0");
    }

    @Test
    void laggingConsumerThresholdAutoHeuristic() {
        // Test that threshold=-1 (auto) correctly applies cache TTL heuristic
        final Map<String, String> configs = new HashMap<>();
        configs.put("control.plane.class", InMemoryControlPlane.class.getCanonicalName());
        configs.put("storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName());
        configs.put("consume.cache.expiration.lifespan.sec", "300");  // 5 minutes
        configs.put("fetch.lagging.consumer.threshold.ms", "-1");  // auto (default)

        final var config = new InklessConfig(configs);

        assertThat(config.fetchLaggingConsumerThresholdMs()).isEqualTo(300_000L);
    }

    @Test
    void laggingConsumerThresholdIndependentFromCacheTTL() {
        // Test that explicit threshold decouples from cache TTL
        final Map<String, String> configs = new HashMap<>();
        configs.put("control.plane.class", InMemoryControlPlane.class.getCanonicalName());
        configs.put("storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName());
        configs.put("consume.cache.expiration.lifespan.sec", "60");  // 1 minute cache
        configs.put("fetch.lagging.consumer.threshold.ms", "600000");  // 10 minutes threshold

        final var config = new InklessConfig(configs);

        // Threshold should be explicit value, not cache TTL
        assertThat(config.cacheExpirationLifespanSec()).isEqualTo(60);
        assertThat(config.fetchLaggingConsumerThresholdMs()).isEqualTo(600_000L);  // Independent!
    }

    @Test
    void laggingConsumerThresholdBelowMinusOneInvalid() {
        // Test that threshold < -1 is invalid
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "fetch.lagging.consumer.threshold.ms", "-2"
        );

        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value -2 for configuration fetch.lagging.consumer.threshold.ms: Value must be at least -1");
    }

    @Test
    void laggingConsumerThresholdZeroInvalidWhenCacheEnabled() {
        // Test that threshold=0 is invalid when cache lifespan > 0
        // This prevents routing cached data to the cold path
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "consume.cache.expiration.lifespan.sec", "60",  // Default 60 seconds
            "fetch.lagging.consumer.threshold.ms", "0"
        );

        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("fetch.lagging.consumer.threshold.ms")
            .hasMessageContaining("must be >= cache lifespan");
    }

    @Test
    void laggingConsumerThresholdZeroValidWhenCacheMinimal() {
        // Test that threshold can equal minimum cache lifespan (10 seconds)
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "consume.cache.expiration.lifespan.sec", "10",  // Minimum allowed (10 seconds)
            "fetch.lagging.consumer.threshold.ms", "10000"  // 10 seconds - equal to cache lifespan
        );

        final var inklessConfig = new InklessConfig(config);
        assertThat(inklessConfig.fetchLaggingConsumerThresholdMs()).isEqualTo(10000L);
    }

    @Test
    void laggingConsumerThresholdZeroInvalidWhenCacheMinimal() {
        // Test that threshold=0 is invalid even with minimum cache lifespan (10 seconds)
        // This ensures threshold=0 is always invalid when cache is enabled
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "consume.cache.expiration.lifespan.sec", "10",  // Minimum allowed (10 seconds = 10000ms)
            "fetch.lagging.consumer.threshold.ms", "0"
        );

        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("fetch.lagging.consumer.threshold.ms")
            .hasMessageContaining("must be >= cache lifespan");
    }

    @Test
    void laggingConsumerRateLimitExceedsUpperBoundInvalid() {
        // Test that rate limit exceeding upper bound (10000) is invalid
        final Map<String, String> config = Map.of(
            "control.plane.class", InMemoryControlPlane.class.getCanonicalName(),
            "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "fetch.lagging.consumer.request.rate.limit", "10001"
        );

        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("fetch.lagging.consumer.request.rate.limit")
            .hasMessageContaining("Value must be no more than 10000");
    }

    @Test
    void laggingConsumerThresholdBelowCacheLifespanInvalid() {
        // Test that threshold < cache lifespan is invalid when explicitly set
        final Map<String, String> config = new HashMap<>();
        config.put("control.plane.class", InMemoryControlPlane.class.getCanonicalName());
        config.put("storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName());
        config.put("consume.cache.expiration.lifespan.sec", "120");  // 2 minutes = 120000ms
        config.put("fetch.lagging.consumer.threshold.ms", "60000");  // 1 minute - less than cache lifespan

        assertThatThrownBy(() -> new InklessConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("fetch.lagging.consumer.threshold.ms")
            .hasMessageContaining("must be >= cache lifespan");
    }

    @Test
    void laggingConsumerThresholdEqualToCacheLifespanValid() {
        // Test that threshold == cache lifespan is valid
        final Map<String, String> config = new HashMap<>();
        config.put("control.plane.class", InMemoryControlPlane.class.getCanonicalName());
        config.put("storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName());
        config.put("consume.cache.expiration.lifespan.sec", "60");  // 1 minute = 60000ms
        config.put("fetch.lagging.consumer.threshold.ms", "60000");  // 1 minute - equal to cache lifespan

        final var inklessConfig = new InklessConfig(config);
        assertThat(inklessConfig.fetchLaggingConsumerThresholdMs()).isEqualTo(60_000L);
    }

    @Test
    void laggingConsumerThresholdAutoSkipsValidation() {
        // Test that threshold=-1 (auto) skips validation even if cache lifespan is large
        final Map<String, String> config = new HashMap<>();
        config.put("control.plane.class", InMemoryControlPlane.class.getCanonicalName());
        config.put("storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName());
        config.put("consume.cache.expiration.lifespan.sec", "300");  // 5 minutes
        config.put("fetch.lagging.consumer.threshold.ms", "-1");  // auto (default)

        // Should not throw - auto mode uses heuristic and skips validation
        final var inklessConfig = new InklessConfig(config);
        assertThat(inklessConfig.fetchLaggingConsumerThresholdMs()).isEqualTo(300_000L);
    }

    @Test
    void fullConfigWithLaggingConsumer() {
        // Test complete configuration including all lagging consumer settings
        final String controlPlaneClass = InMemoryControlPlane.class.getCanonicalName();
        Map<String, String> configs = new HashMap<>();
        configs.put("control.plane.class", controlPlaneClass);
        configs.put("object.key.prefix", "prefix/");
        configs.put("produce.commit.interval.ms", "100");
        configs.put("produce.buffer.max.bytes", "1024");
        configs.put("produce.max.upload.attempts", "5");
        configs.put("produce.upload.backoff.ms", "30");
        configs.put("storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName());
        configs.put("file.cleaner.interval.ms", "100");
        configs.put("file.cleaner.retention.period.ms", "200");
        configs.put("file.merger.interval.ms", "100");
        configs.put("consume.cache.max.count", "100");
        configs.put("consume.cache.expiration.lifespan.sec", "200");
        configs.put("consume.cache.expiration.max.idle.sec", "100");
        configs.put("produce.upload.thread.pool.size", "16");
        configs.put("fetch.data.thread.pool.size", "12");
        configs.put("fetch.metadata.thread.pool.size", "14");
        configs.put("fetch.lagging.consumer.thread.pool.size", "24");
        configs.put("fetch.lagging.consumer.threshold.ms", "240000");  // 4 minutes (must be >= cache lifespan of 200 sec)
        configs.put("fetch.lagging.consumer.request.rate.limit", "300");
        configs.put("retention.enforcement.max.batches.per.request", "10");

        final var config = new InklessConfig(configs);

        assertThat(config.controlPlaneClass()).isEqualTo(InMemoryControlPlane.class);
        assertThat(config.controlPlaneConfig()).isEqualTo(Map.of("class", controlPlaneClass));
        assertThat(config.objectKeyPrefix()).isEqualTo("prefix/");
        assertThat(config.commitInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.produceBufferMaxBytes()).isEqualTo(1024);
        assertThat(config.produceMaxUploadAttempts()).isEqualTo(5);
        assertThat(config.produceUploadBackoff()).isEqualTo(Duration.ofMillis(30));
        assertThat(config.storage(storageMetrics)).isInstanceOf(ConfigTestStorageBackend.class);
        assertThat(config.fileCleanerInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.fileCleanerRetentionPeriod()).isEqualTo(Duration.ofMillis(200));
        assertThat(config.fileMergerInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.cacheMaxCount()).isEqualTo(100);
        assertThat(config.cacheExpirationLifespanSec()).isEqualTo(200);
        assertThat(config.cacheExpirationMaxIdleSec()).isEqualTo(100);
        assertThat(config.produceUploadThreadPoolSize()).isEqualTo(16);
        assertThat(config.fetchDataThreadPoolSize()).isEqualTo(12);
        assertThat(config.fetchMetadataThreadPoolSize()).isEqualTo(14);
        assertThat(config.fetchLaggingConsumerThreadPoolSize()).isEqualTo(24);
        assertThat(config.fetchLaggingConsumerThresholdMs()).isEqualTo(240_000L);
        assertThat(config.fetchLaggingConsumerRequestRateLimit()).isEqualTo(300);
        assertThat(config.maxBatchesPerEnforcementRequest()).isEqualTo(10);
    }
}
