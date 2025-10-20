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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
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
                "max.connections", "11",
                "file.merge.size.threshold.bytes", "1234",
                "file.merge.lock.period.ms", "4567"
            )
        );

        assertThat(config.connectionString()).isEqualTo("jdbc:postgresql://127.0.0.1:5432/inkless");
        assertThat(config.username()).isEqualTo("username");
        assertThat(config.password()).isEqualTo("password");
        assertThat(config.fileMergeSizeThresholdBytes()).isEqualTo(1234);
        assertThat(config.fileMergeLockPeriod()).isEqualTo(Duration.ofMillis(4567));
        assertThat(config.maxConnections()).isEqualTo(11);
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
        assertThat(config.maxConnections()).isEqualTo(10);

        // ensure read/write configs are null when not set
        config.initializeReadWriteConfigs();
        assertThat(config.readConfig()).isNull();
        assertThat(config.writeConfig()).isNull();
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

    @Test
    void writeReadConfigs() {
        // given a config with read/write settings
        final Map<String, String> configs = new HashMap<>();
        configs.putAll(
            Map.of(
                "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
                "username", "username",
                "password", "password"
            )
        );
        configs.putAll(
            Map.of(
                "read.connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless-read",
                "read.username", "username-r",
                "read.password", "password-r",
                "read.max.connections", "20"
            )
        );
        configs.putAll(
            Map.of(
                "write.connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless-write",
                "write.username", "username-w",
                "write.password", "password-w",
                "write.max.connections", "15"
            )
        );
        // when configs are initialized
        final var config = new PostgresControlPlaneConfig(configs);
        config.initializeReadWriteConfigs();

        // then read/write configs are properly set
        assertThat(config.connectionString()).isEqualTo("jdbc:postgresql://127.0.0.1:5432/inkless");
        assertThat(config.username()).isEqualTo("username");
        assertThat(config.password()).isEqualTo("password");
        assertThat(config.readConfig()).isNotNull();
        assertThat(config.readConfig().connectionString()).isEqualTo("jdbc:postgresql://127.0.0.1:5432/inkless-read");
        assertThat(config.readConfig().username()).isEqualTo("username-r");
        assertThat(config.readConfig().password()).isEqualTo("password-r");
        assertThat(config.writeConfig()).isNotNull();
        assertThat(config.writeConfig().connectionString()).isEqualTo("jdbc:postgresql://127.0.0.1:5432/inkless-write");
        assertThat(config.writeConfig().username()).isEqualTo("username-w");
        assertThat(config.writeConfig().password()).isEqualTo("password-w");
    }
}
