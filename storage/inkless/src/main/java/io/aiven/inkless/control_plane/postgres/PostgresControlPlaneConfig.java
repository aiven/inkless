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

import java.util.Map;

public class PostgresControlPlaneConfig extends PostgresConnectionConfig {

    public static final String READ_CONFIG_PREFIX = "read.";
    public static final String WRITE_CONFIG_PREFIX = "write.";

    public static final String BATCH_COALESCING_ENABLED_CONFIG = "batch.coalescing.enabled";
    private static final String BATCH_COALESCING_ENABLED_DOC = "When true, commit_file_v2 is used to collapse contiguous "
        + "same-partition batch runs into a single batches row.";
    private static final boolean BATCH_COALESCING_ENABLED_DEFAULT = true;

    public static final String MIGRATION_TIMEOUT_MS_CONFIG = "migration.timeout.ms";
    private static final String MIGRATION_TIMEOUT_MS_DOC = "Maximum time in milliseconds that schema migration may "
        + "spend waiting on the database, applied both to socket reads and to the wait for the migration advisory "
        + "lock another broker may be holding. Distinct from " + SOCKET_TIMEOUT_MS_CONFIG + ", which bounds ordinary "
        + "queries: a migration that builds an index on a populated table legitimately runs far longer than a query, "
        + "so bounding it at the query timeout would kill it part-way through.";
    private static final long MIGRATION_TIMEOUT_MS_DEFAULT = 300_000;

    private PostgresConnectionConfig readConfig;
    private PostgresConnectionConfig writeConfig;

    public PostgresControlPlaneConfig(final Map<?, ?> originals) {
        super(
            configDef(),
            originals
        );
    }

    public static ConfigDef configDef() {
        return PostgresConnectionConfig.connectionConfigDef()
            .define(
                BATCH_COALESCING_ENABLED_CONFIG,
                ConfigDef.Type.BOOLEAN,
                BATCH_COALESCING_ENABLED_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                BATCH_COALESCING_ENABLED_DOC
            )
            .define(
                MIGRATION_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.LONG,
                MIGRATION_TIMEOUT_MS_DEFAULT,
                ConfigDef.Range.atLeast(1_000),
                ConfigDef.Importance.LOW,
                MIGRATION_TIMEOUT_MS_DOC
            );
    }

    public boolean batchCoalescingEnabled() {
        return getBoolean(BATCH_COALESCING_ENABLED_CONFIG);
    }

    public long migrationTimeoutMs() {
        return getLong(MIGRATION_TIMEOUT_MS_CONFIG);
    }

    public long migrationTimeoutSeconds() {
        return timeoutSeconds(migrationTimeoutMs());
    }

    public void initializeReadWriteConfigs() {
        final Map<String, Object> readConfigs = originalsWithPrefix(READ_CONFIG_PREFIX);
        if (!readConfigs.isEmpty()) {
            this.readConfig = new PostgresConnectionConfig(connectionConfigDef(), readConfigs);
        }

        final Map<String, Object> writeConfigs = originalsWithPrefix(WRITE_CONFIG_PREFIX);
        if (!writeConfigs.isEmpty()) {
            this.writeConfig = new PostgresConnectionConfig(connectionConfigDef(), writeConfigs);
        }
    }

    public PostgresConnectionConfig readConfig() {
        return readConfig;
    }

    public PostgresConnectionConfig writeConfig() {
        return writeConfig;
    }
}
