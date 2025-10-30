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
}
