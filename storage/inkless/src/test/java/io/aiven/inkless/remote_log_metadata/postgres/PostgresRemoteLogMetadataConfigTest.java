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

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PostgresRemoteLogMetadataConfigTest {
    @Test
    void defaults() {
        final PostgresRemoteLogMetadataConfig config =
            new PostgresRemoteLogMetadataConfig(baseConfigs());
        assertEquals(4, config.mutationThreads());
        assertEquals(10, config.maxConnections());
    }

    @Test
    void overrides() {
        final Map<String, String> configs = baseConfigs();
        configs.put("mutation.threads", "8");
        configs.put("max.connections", "16");
        final PostgresRemoteLogMetadataConfig config = new PostgresRemoteLogMetadataConfig(configs);
        assertEquals(8, config.mutationThreads());
        assertEquals(16, config.maxConnections());
    }

    @Test
    void mutationThreadsMustBePositive() {
        final Map<String, String> configs = baseConfigs();
        configs.put("mutation.threads", "0");
        assertThrows(ConfigException.class, () -> new PostgresRemoteLogMetadataConfig(configs));
    }

    @Test
    void connectionStringIsRequired() {
        final Map<String, String> configs = baseConfigs();
        configs.remove("connection.string");
        assertThrows(ConfigException.class, () -> new PostgresRemoteLogMetadataConfig(configs));
    }

    private static Map<String, String> baseConfigs() {
        final Map<String, String> configs = new HashMap<>();
        configs.put("connection.string", "jdbc:postgresql://localhost:5432/db");
        configs.put("username", "user");
        return configs;
    }
}
