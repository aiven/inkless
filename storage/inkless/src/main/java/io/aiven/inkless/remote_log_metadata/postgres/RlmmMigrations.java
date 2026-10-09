/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
package io.aiven.inkless.remote_log_metadata.postgres;

import org.flywaydb.core.Flyway;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.SQLException;

class RlmmMigrations {
    static final String SCHEMA = "inkless_rlmm";
    static final String HISTORY_TABLE = "flyway_schema_history_rlmm";

    static void migrate(final PostgresRemoteLogMetadataConfig connectionConfig) throws SQLException {
        final PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(connectionConfig.connectionString());
        dataSource.setUser(connectionConfig.username());
        dataSource.setPassword(connectionConfig.password());
        dataSource.setConnectTimeout(
            PostgresRemoteLogMetadataManager.timeoutSeconds(connectionConfig.tcpConnectTimeoutMs()));
        dataSource.setSocketTimeout(
            PostgresRemoteLogMetadataManager.timeoutSeconds(connectionConfig.socketTimeoutMs()));
        dataSource.setLoginTimeout(
            PostgresRemoteLogMetadataManager.timeoutSeconds(connectionConfig.tcpConnectTimeoutMs()));
        final Flyway flyway = Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/rlmm/migration")
            .schemas(SCHEMA)
            .table(HISTORY_TABLE)
            .load();
        flyway.migrate();
    }
}
