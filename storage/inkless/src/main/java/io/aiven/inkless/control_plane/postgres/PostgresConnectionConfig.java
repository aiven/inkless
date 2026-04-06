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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;

import io.aiven.inkless.control_plane.AbstractControlPlaneConfig;

public class PostgresConnectionConfig extends AbstractControlPlaneConfig {
    public static final String CONNECTION_STRING_CONFIG = "connection.string";
    private static final String CONNECTION_STRING_DOC = "PostgreSQL connection string";

    public static final String USERNAME_CONFIG = "username";
    private static final String USERNAME_DOC = "Username";

    public static final String PASSWORD_CONFIG = "password";
    private static final String PASSWORD_DOC = "Password";

    public static final String MAX_CONNECTIONS_CONFIG = "max.connections";
    private static final String MAX_CONNECTIONS_DOC = "Maximum number of connections to the database";

    // JDBC driver tuning
    public static final String JDBC_PREPARE_THRESHOLD_CONFIG = "jdbc.prepare.threshold";
    private static final String JDBC_PREPARE_THRESHOLD_DOC =
        "Number of executions before a statement is server-prepared";

    public static final String JDBC_PREPARED_STATEMENT_CACHE_QUERIES_CONFIG = "jdbc.prepared.statement.cache.queries";
    private static final String JDBC_PREPARED_STATEMENT_CACHE_QUERIES_DOC =
        "Maximum number of queries to cache per connection";

    public static final String JDBC_PREPARED_STATEMENT_CACHE_SIZE_MIB_CONFIG = "jdbc.prepared.statement.cache.size.mib";
    private static final String JDBC_PREPARED_STATEMENT_CACHE_SIZE_MIB_DOC =
        "Maximum size of the prepared statement cache in MiB";

    public static final String JDBC_DEFAULT_ROW_FETCH_SIZE_CONFIG = "jdbc.default.row.fetch.size";
    private static final String JDBC_DEFAULT_ROW_FETCH_SIZE_DOC =
        "Number of rows to fetch at once from the database";

    public static final String JDBC_TCP_KEEP_ALIVE_CONFIG = "jdbc.tcp.keep.alive";
    private static final String JDBC_TCP_KEEP_ALIVE_DOC =
        "Enable TCP keepalive for long-lived connections";

    public static final String JDBC_BINARY_TRANSFER_CONFIG = "jdbc.binary.transfer";
    private static final String JDBC_BINARY_TRANSFER_DOC =
        "Enable binary transfer for better serialization performance";

    // JOOQ settings
    public static final String JOOQ_EXECUTE_LOGGING_CONFIG = "jooq.execute.logging";
    private static final String JOOQ_EXECUTE_LOGGING_DOC =
        "Enable JOOQ query logging";

    public static final String JOOQ_RENDER_CATALOG_CONFIG = "jooq.render.catalog";
    private static final String JOOQ_RENDER_CATALOG_DOC =
        "Render catalog prefix in SQL";

    public static final String JOOQ_RENDER_SCHEMA_CONFIG = "jooq.render.schema";
    private static final String JOOQ_RENDER_SCHEMA_DOC =
        "Render schema prefix in SQL";

    public static final String JOOQ_REFLECTION_CACHING_CONFIG = "jooq.reflection.caching";
    private static final String JOOQ_REFLECTION_CACHING_DOC =
        "Cache reflection operations for faster record mapping";

    public static final String JOOQ_CACHE_RECORD_MAPPERS_CONFIG = "jooq.cache.record.mappers";
    private static final String JOOQ_CACHE_RECORD_MAPPERS_DOC =
        "Cache record mappers for faster result mapping";

    public static final String JOOQ_IN_LIST_PADDING_CONFIG = "jooq.in.list.padding";
    private static final String JOOQ_IN_LIST_PADDING_DOC =
        "Pad IN lists to powers of 2 for better query plan cache reuse";

    public static ConfigDef configDef() {
        return baseConfigDef()
            .define(
                CONNECTION_STRING_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                CONNECTION_STRING_DOC
            )
            .define(
                USERNAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                USERNAME_DOC
            )
            .define(
                PASSWORD_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                null,  // can be empty
                ConfigDef.Importance.HIGH,
                PASSWORD_DOC
            )
            .define(
                MAX_CONNECTIONS_CONFIG,
                ConfigDef.Type.INT,
                10,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM,
                MAX_CONNECTIONS_DOC
            )
            // JDBC driver tuning
            .define(
                JDBC_PREPARE_THRESHOLD_CONFIG,
                ConfigDef.Type.INT,
                5,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                JDBC_PREPARE_THRESHOLD_DOC
            )
            .define(
                JDBC_PREPARED_STATEMENT_CACHE_QUERIES_CONFIG,
                ConfigDef.Type.INT,
                256,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                JDBC_PREPARED_STATEMENT_CACHE_QUERIES_DOC
            )
            .define(
                JDBC_PREPARED_STATEMENT_CACHE_SIZE_MIB_CONFIG,
                ConfigDef.Type.INT,
                5,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                JDBC_PREPARED_STATEMENT_CACHE_SIZE_MIB_DOC
            )
            .define(
                JDBC_DEFAULT_ROW_FETCH_SIZE_CONFIG,
                ConfigDef.Type.INT,
                100,
                ConfigDef.Range.atLeast(0),
                ConfigDef.Importance.LOW,
                JDBC_DEFAULT_ROW_FETCH_SIZE_DOC
            )
            .define(
                JDBC_TCP_KEEP_ALIVE_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                JDBC_TCP_KEEP_ALIVE_DOC
            )
            .define(
                JDBC_BINARY_TRANSFER_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                JDBC_BINARY_TRANSFER_DOC
            )
            // JOOQ settings
            .define(
                JOOQ_EXECUTE_LOGGING_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                JOOQ_EXECUTE_LOGGING_DOC
            )
            .define(
                JOOQ_RENDER_CATALOG_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                JOOQ_RENDER_CATALOG_DOC
            )
            .define(
                JOOQ_RENDER_SCHEMA_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                JOOQ_RENDER_SCHEMA_DOC
            )
            .define(
                JOOQ_REFLECTION_CACHING_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                JOOQ_REFLECTION_CACHING_DOC
            )
            .define(
                JOOQ_CACHE_RECORD_MAPPERS_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                JOOQ_CACHE_RECORD_MAPPERS_DOC
            )
            .define(
                JOOQ_IN_LIST_PADDING_CONFIG,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                JOOQ_IN_LIST_PADDING_DOC
            );
    }

    public PostgresConnectionConfig(final ConfigDef configDef, final Map<?, ?> originals) {
        super(configDef, originals);
    }

    public String connectionString() {
        return getString(CONNECTION_STRING_CONFIG);
    }

    public String username() {
        return getString(USERNAME_CONFIG);
    }

    public String password() {
        final Password configValue = getPassword(PASSWORD_CONFIG);
        return configValue == null ? null : configValue.value();
    }

    public int maxConnections() {
        return getInt(MAX_CONNECTIONS_CONFIG);
    }

    // JDBC driver tuning getters
    public int jdbcPrepareThreshold() {
        return getInt(JDBC_PREPARE_THRESHOLD_CONFIG);
    }

    public int jdbcPreparedStatementCacheQueries() {
        return getInt(JDBC_PREPARED_STATEMENT_CACHE_QUERIES_CONFIG);
    }

    public int jdbcPreparedStatementCacheSizeMib() {
        return getInt(JDBC_PREPARED_STATEMENT_CACHE_SIZE_MIB_CONFIG);
    }

    public int jdbcDefaultRowFetchSize() {
        return getInt(JDBC_DEFAULT_ROW_FETCH_SIZE_CONFIG);
    }

    public boolean jdbcTcpKeepAlive() {
        return getBoolean(JDBC_TCP_KEEP_ALIVE_CONFIG);
    }

    public boolean jdbcBinaryTransfer() {
        return getBoolean(JDBC_BINARY_TRANSFER_CONFIG);
    }

    // JOOQ settings getters
    public boolean jooqExecuteLogging() {
        return getBoolean(JOOQ_EXECUTE_LOGGING_CONFIG);
    }

    public boolean jooqRenderCatalog() {
        return getBoolean(JOOQ_RENDER_CATALOG_CONFIG);
    }

    public boolean jooqRenderSchema() {
        return getBoolean(JOOQ_RENDER_SCHEMA_CONFIG);
    }

    public boolean jooqReflectionCaching() {
        return getBoolean(JOOQ_REFLECTION_CACHING_CONFIG);
    }

    public boolean jooqCacheRecordMappers() {
        return getBoolean(JOOQ_CACHE_RECORD_MAPPERS_CONFIG);
    }

    public boolean jooqInListPadding() {
        return getBoolean(JOOQ_IN_LIST_PADDING_CONFIG);
    }
}
