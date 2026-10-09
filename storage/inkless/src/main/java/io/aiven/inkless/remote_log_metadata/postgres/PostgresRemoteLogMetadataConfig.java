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

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import io.aiven.inkless.control_plane.postgres.PostgresConnectionConfig;

public class PostgresRemoteLogMetadataConfig extends PostgresConnectionConfig {
    public static final String MUTATION_THREADS_CONFIG = "mutation.threads";
    private static final String MUTATION_THREADS_DOC =
        "Number of threads that execute remote log metadata mutations against PostgreSQL. "
            + "Mutations for one topic partition serialize on the partition row lock; "
            + "mutations for different partitions run in parallel. Keep this at or below "
            + MAX_CONNECTIONS_CONFIG + " so mutation tasks never starve on pool checkout.";
    private static final int MUTATION_THREADS_DEFAULT = 4;

    public PostgresRemoteLogMetadataConfig(final Map<?, ?> originals) {
        super(
            configDef(),
            originals
        );
    }

    public static ConfigDef configDef() {
        return PostgresConnectionConfig.connectionConfigDef()
            .define(
                MUTATION_THREADS_CONFIG,
                ConfigDef.Type.INT,
                MUTATION_THREADS_DEFAULT,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM,
                MUTATION_THREADS_DOC
            );
    }

    public int mutationThreads() {
        return getInt(MUTATION_THREADS_CONFIG);
    }
}
