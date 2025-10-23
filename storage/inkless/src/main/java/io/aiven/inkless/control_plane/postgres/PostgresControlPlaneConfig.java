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

import java.util.Map;

public class PostgresControlPlaneConfig extends PostgresConnectionConfig {

    public static final String READ_CONFIG_PREFIX = "read.";
    public static final String WRITE_CONFIG_PREFIX = "write.";

    private PostgresConnectionConfig readConfig;
    private PostgresConnectionConfig writeConfig;

    public PostgresControlPlaneConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public void initializeReadWriteConfigs() {
        final Map<String, Object> readConfigs = originalsWithPrefix(READ_CONFIG_PREFIX);
        if (!readConfigs.isEmpty()) {
            this.readConfig = new PostgresConnectionConfig(configDef(), readConfigs);
        }

        final Map<String, Object> writeConfigs = originalsWithPrefix(WRITE_CONFIG_PREFIX);
        if (!writeConfigs.isEmpty()) {
            this.writeConfig = new PostgresConnectionConfig(configDef(), writeConfigs);
        }
    }

    public PostgresConnectionConfig readConfig() {
        return readConfig;
    }

    public PostgresConnectionConfig writeConfig() {
        return writeConfig;
    }
}
