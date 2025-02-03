// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import io.aiven.inkless.control_plane.InMemoryControlPlane;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InklessConfigTest {
    @Test
    void publicConstructor() {
        final String controlPlaneClass = InMemoryControlPlane.class.getCanonicalName();
        final InklessConfig config = new InklessConfig(new AbstractConfig(new ConfigDef(), Map.of(
            "inkless.control.plane.class", controlPlaneClass,
            "inkless.object.key.prefix", "prefix/",
            "inkless.produce.commit.interval.ms", "100",
            "inkless.produce.buffer.max.bytes", "1024",
            "inkless.produce.max.upload.attempts", "5",
            "inkless.produce.upload.backoff.ms", "30",
            "inkless.storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
            "inkless.file.cleaner.interval.ms", "100",
            "inkless.file.cleaner.retention.period.ms", "200",
            "inkless.file.merger.interval.ms", "100"
        )));
        assertThat(config.controlPlaneClass()).isEqualTo(InMemoryControlPlane.class);
        assertThat(config.controlPlaneConfig()).isEqualTo(Map.of("class", controlPlaneClass));
        assertThat(config.objectKeyPrefix()).isEqualTo("prefix/");
        assertThat(config.commitInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.produceBufferMaxBytes()).isEqualTo(1024);
        assertThat(config.produceMaxUploadAttempts()).isEqualTo(5);
        assertThat(config.produceUploadBackoff()).isEqualTo(Duration.ofMillis(30));
        assertThat(config.storage()).isInstanceOf(ConfigTestStorageBackend.class);
        assertThat(config.fileCleanerInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.fileCleanerRetentionPeriod()).isEqualTo(Duration.ofMillis(200));
        assertThat(config.fileMergerInterval()).isEqualTo(Duration.ofMillis(100));
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
        assertThat(config.storage()).isInstanceOf(ConfigTestStorageBackend.class);
        assertThat(config.fileCleanerInterval()).isEqualTo(Duration.ofMinutes(5));
        assertThat(config.fileCleanerRetentionPeriod()).isEqualTo(Duration.ofMinutes(10));
        assertThat(config.fileMergerInterval()).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void fullConfig() {
        final String controlPlaneClass = InMemoryControlPlane.class.getCanonicalName();
        final var config = new InklessConfig(
            Map.of(
                "control.plane.class", controlPlaneClass,
                "object.key.prefix", "prefix/",
                "produce.commit.interval.ms", "100",
                "produce.buffer.max.bytes", "1024",
                "produce.max.upload.attempts", "5",
                "produce.upload.backoff.ms", "30",
                "storage.backend.class", ConfigTestStorageBackend.class.getCanonicalName(),
                "file.cleaner.interval.ms", "100",
                "file.cleaner.retention.period.ms", "200",
                "file.merger.interval.ms", "100"
            )
        );
        assertThat(config.controlPlaneClass()).isEqualTo(InMemoryControlPlane.class);
        assertThat(config.controlPlaneConfig()).isEqualTo(Map.of("class", controlPlaneClass));
        assertThat(config.objectKeyPrefix()).isEqualTo("prefix/");
        assertThat(config.commitInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.produceBufferMaxBytes()).isEqualTo(1024);
        assertThat(config.produceMaxUploadAttempts()).isEqualTo(5);
        assertThat(config.produceUploadBackoff()).isEqualTo(Duration.ofMillis(30));
        assertThat(config.storage()).isInstanceOf(ConfigTestStorageBackend.class);
        assertThat(config.fileCleanerInterval()).isEqualTo(Duration.ofMillis(100));
        assertThat(config.fileCleanerRetentionPeriod()).isEqualTo(Duration.ofMillis(200));
        assertThat(config.fileMergerInterval()).isEqualTo(Duration.ofMillis(100));
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
        assertThat(config.storage()).isInstanceOf(ConfigTestStorageBackend.class);
        final var storage = (ConfigTestStorageBackend) config.storage();
        assertThat(storage.passedConfig).isEqualTo(Map.of(
            "backend.class", backendClass,
            "a", "1",
            "b", "str"));
    }
}
