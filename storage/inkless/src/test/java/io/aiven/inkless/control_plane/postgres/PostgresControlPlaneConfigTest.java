// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresControlPlaneConfigTest {
    @Test
    void fullConfig() {
        final var config = new PostgresControlPlaneConfig(
            Map.of(
                "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
                "username", "username",
                "password", "password",
                "file.merge.size.threshold.bytes", "1234",
                "file.merge.lock.period.ms", "4567"
            )
        );

        assertThat(config.connectionString()).isEqualTo("jdbc:postgresql://127.0.0.1:5432/inkless");
        assertThat(config.username()).isEqualTo("username");
        assertThat(config.password()).isEqualTo("password");
        assertThat(config.fileMergeSizeThresholdBytes()).isEqualTo(1234);
        assertThat(config.fileMergeLockPeriod()).isEqualTo(Duration.ofMillis(4567));
    }

    @Test
    void minimalConfig() {
        final var config = new PostgresControlPlaneConfig(
            Map.of(
                "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
                "username", "username",
                "password", "password"
            )
        );

        assertThat(config.connectionString()).isEqualTo("jdbc:postgresql://127.0.0.1:5432/inkless");
        assertThat(config.username()).isEqualTo("username");
        assertThat(config.password()).isEqualTo("password");
        assertThat(config.fileMergeSizeThresholdBytes()).isEqualTo(100 * 1024 * 1024);
        assertThat(config.fileMergeLockPeriod()).isEqualTo(Duration.ofHours(1));
    }

    @Test
    void connectionStringMissing() {
        assertThatThrownBy(() -> new PostgresControlPlaneConfig(
            Map.of(
                "username", "username",
                "password", "password"
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"connection.string\" which has no default value.");
    }

    @Test
    void usernameMissing() {
        assertThatThrownBy(() -> new PostgresControlPlaneConfig(
            Map.of(
                "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
                "password", "password"
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"username\" which has no default value.");
    }

    @Test
    void defaultPassword() {
        final var config = new PostgresControlPlaneConfig(
            Map.of(
                "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
                "username", "username"
            )
        );
        assertThat(config.password()).isNull();
    }

    @Test
    void fileMergeSizeThresholdBytesNotPositive() {
        final Map<String, String> config = Map.of(
            "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
            "username", "username",
            "password", "password",
            "file.merge.size.threshold.bytes", "0"
        );
        assertThatThrownBy(() -> new PostgresControlPlaneConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration file.merge.size.threshold.bytes: Value must be at least 1");
    }

    @Test
    void fileMergeLockPeriodNotPositive() {
        final Map<String, String> config = Map.of(
            "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
            "username", "username",
            "password", "password",
            "file.merge.lock.period.ms", "0"
        );
        assertThatThrownBy(() -> new PostgresControlPlaneConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration file.merge.lock.period.ms: Value must be at least 1");
    }
}
