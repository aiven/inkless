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

    @Test
    void jdbcTuningDefaults() {
        // Verify JDBC tuning config defaults
        final var config = new PostgresControlPlaneConfig(
            Map.of(
                "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
                "username", "username",
                "password", "password"
            )
        );

        // JDBC driver tuning defaults
        assertThat(config.jdbcPrepareThreshold()).isEqualTo(5);
        assertThat(config.jdbcPreparedStatementCacheQueries()).isEqualTo(256);
        assertThat(config.jdbcPreparedStatementCacheSizeMib()).isEqualTo(5);
        assertThat(config.jdbcDefaultRowFetchSize()).isEqualTo(100);
        assertThat(config.jdbcTcpKeepAlive()).isTrue();
        assertThat(config.jdbcBinaryTransfer()).isTrue();
    }

    @Test
    void jooqSettingsDefaults() {
        // Verify JOOQ settings defaults
        final var config = new PostgresControlPlaneConfig(
            Map.of(
                "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
                "username", "username",
                "password", "password"
            )
        );

        // JOOQ settings defaults
        assertThat(config.jooqExecuteLogging()).isFalse();
        assertThat(config.jooqRenderCatalog()).isFalse();
        assertThat(config.jooqRenderSchema()).isFalse();
        assertThat(config.jooqReflectionCaching()).isTrue();
        assertThat(config.jooqCacheRecordMappers()).isTrue();
        assertThat(config.jooqInListPadding()).isTrue();
    }

    @Test
    void jdbcTuningOverrides() {
        // Verify JDBC tuning configs can be overridden
        final var config = new PostgresControlPlaneConfig(
            Map.of(
                "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
                "username", "username",
                "password", "password",
                "jdbc.prepare.threshold", "10",
                "jdbc.prepared.statement.cache.queries", "512",
                "jdbc.prepared.statement.cache.size.mib", "10",
                "jdbc.default.row.fetch.size", "500",
                "jdbc.tcp.keep.alive", "false",
                "jdbc.binary.transfer", "false"
            )
        );

        assertThat(config.jdbcPrepareThreshold()).isEqualTo(10);
        assertThat(config.jdbcPreparedStatementCacheQueries()).isEqualTo(512);
        assertThat(config.jdbcPreparedStatementCacheSizeMib()).isEqualTo(10);
        assertThat(config.jdbcDefaultRowFetchSize()).isEqualTo(500);
        assertThat(config.jdbcTcpKeepAlive()).isFalse();
        assertThat(config.jdbcBinaryTransfer()).isFalse();
    }

    @Test
    void jooqSettingsOverrides() {
        // Verify JOOQ settings can be overridden
        final var config = new PostgresControlPlaneConfig(
            Map.of(
                "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
                "username", "username",
                "password", "password",
                "jooq.execute.logging", "true",
                "jooq.render.catalog", "true",
                "jooq.render.schema", "true",
                "jooq.reflection.caching", "false",
                "jooq.cache.record.mappers", "false",
                "jooq.in.list.padding", "false"
            )
        );

        assertThat(config.jooqExecuteLogging()).isTrue();
        assertThat(config.jooqRenderCatalog()).isTrue();
        assertThat(config.jooqRenderSchema()).isTrue();
        assertThat(config.jooqReflectionCaching()).isFalse();
        assertThat(config.jooqCacheRecordMappers()).isFalse();
        assertThat(config.jooqInListPadding()).isFalse();
    }

    @Test
    void readWritePrefixedJdbcJooqOverrides() {
        // Verify read/write prefixed JDBC/JOOQ configs are properly parsed
        final Map<String, String> configs = new HashMap<>();
        configs.put("connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless");
        configs.put("username", "username");
        configs.put("password", "password");
        // Read-prefixed overrides
        configs.put("read.connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless-read");
        configs.put("read.username", "username-r");
        configs.put("read.password", "password-r");
        configs.put("read.jdbc.default.row.fetch.size", "1000");  // Higher for read replicas
        configs.put("read.jooq.execute.logging", "true");
        // Write-prefixed overrides
        configs.put("write.connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless-write");
        configs.put("write.username", "username-w");
        configs.put("write.password", "password-w");
        configs.put("write.jdbc.default.row.fetch.size", "50");  // Lower for write primary
        configs.put("write.jooq.execute.logging", "false");

        final var config = new PostgresControlPlaneConfig(configs);
        config.initializeReadWriteConfigs();

        // Base config has defaults
        assertThat(config.jdbcDefaultRowFetchSize()).isEqualTo(100);
        assertThat(config.jooqExecuteLogging()).isFalse();

        // Read config has overrides
        assertThat(config.readConfig()).isNotNull();
        assertThat(config.readConfig().jdbcDefaultRowFetchSize()).isEqualTo(1000);
        assertThat(config.readConfig().jooqExecuteLogging()).isTrue();

        // Write config has overrides
        assertThat(config.writeConfig()).isNotNull();
        assertThat(config.writeConfig().jdbcDefaultRowFetchSize()).isEqualTo(50);
        assertThat(config.writeConfig().jooqExecuteLogging()).isFalse();
    }
}
