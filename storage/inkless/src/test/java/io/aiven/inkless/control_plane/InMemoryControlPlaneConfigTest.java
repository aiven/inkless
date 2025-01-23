// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryControlPlaneConfigTest {
    @Test
    void fullConfig() {
        final var config = new InMemoryControlPlaneConfig(
            Map.of(
                "file.merge.size.threshold.bytes", "1234",
                "file.merge.lock.period.ms", "4567"
            )
        );

        assertThat(config.fileMergeSizeThresholdBytes()).isEqualTo(1234);
        assertThat(config.fileMergeLockPeriod()).isEqualTo(Duration.ofMillis(4567));
    }

    @Test
    void minimalConfig() {
        final var config = new InMemoryControlPlaneConfig(
            Map.of()
        );

        assertThat(config.fileMergeSizeThresholdBytes()).isEqualTo(100 * 1024 * 1024);
        assertThat(config.fileMergeLockPeriod()).isEqualTo(Duration.ofHours(1));
    }

    @Test
    void fileMergeSizeThresholdBytesNotPositive() {
        final Map<String, String> config = Map.of(
            "file.merge.size.threshold.bytes", "0"
        );
        assertThatThrownBy(() -> new InMemoryControlPlaneConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration file.merge.size.threshold.bytes: Value must be at least 1");
    }

    @Test
    void fileMergeLockPeriodNotPositive() {
        final Map<String, String> config = Map.of(
            "file.merge.lock.period.ms", "0"
        );
        assertThatThrownBy(() -> new InMemoryControlPlaneConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration file.merge.lock.period.ms: Value must be at least 1");
    }
}
