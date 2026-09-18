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

import org.flywaydb.core.Flyway;

import java.util.Map;

class Migrations {
    /**
     * Applies pending schema migrations, bounded by the configured timeouts.
     *
     * <p>Flyway builds its own JDBC connection rather than borrowing one from the Hikari pool, so
     * it does not inherit the pool's driver properties. Without the ones set here it would fall
     * back to the pgjdbc defaults, where {@code socketTimeout} is {@code 0}: a database that
     * accepts the connection and then stops answering would block this thread forever.
     *
     * <p>Socket reads get {@code migration.timeout.ms} rather than {@code socket.timeout.ms}
     * because a migration that builds an index on a populated table runs far longer than any
     * query, and being cut off part-way through leaves the schema behind.
     */
    static void migrate(final PostgresControlPlaneConfig controlPlaneConfig) {
        final long migrationTimeoutSeconds = controlPlaneConfig.migrationTimeoutSeconds();
        // jdbcProperties MUST be set before dataSource: setting the data source builds the
        // DriverDataSource eagerly from whatever properties are configured at that moment, so
        // properties registered afterwards are silently dropped.
        final Flyway flyway = Flyway.configure()
            .jdbcProperties(Map.of(
                "connectTimeout", Long.toString(controlPlaneConfig.tcpConnectTimeoutSeconds()),
                "loginTimeout", Long.toString(controlPlaneConfig.tcpConnectTimeoutSeconds()),
                "socketTimeout", Long.toString(migrationTimeoutSeconds)))
            // Flyway takes an advisory lock so that only one broker migrates at a time, and retries
            // once a second while another broker holds it. One retry per second means the retry
            // count is the budget in seconds.
            .lockRetryCount((int) Math.min(Integer.MAX_VALUE, migrationTimeoutSeconds))
            .dataSource(
                controlPlaneConfig.connectionString(),
                controlPlaneConfig.username(),
                controlPlaneConfig.password())
            .load();
        flyway.migrate();
    }
}
