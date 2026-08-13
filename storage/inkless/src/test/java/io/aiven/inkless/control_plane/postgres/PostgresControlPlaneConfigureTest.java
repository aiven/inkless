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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.MockTime;

import org.junit.jupiter.api.Test;

import java.util.Map;

import io.aiven.inkless.control_plane.ControlPlaneNotConfiguredException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Covers {@link PostgresControlPlane#configure(Map)} translating configuration errors, without a
 * database: every case here fails before the delegate opens a connection.
 */
class PostgresControlPlaneConfigureTest {
    @Test
    void missingConnectionStringIsReportedAsNotConfigured() {
        final var controlPlane = new PostgresControlPlane(new MockTime());

        final var e = assertThrows(ControlPlaneNotConfiguredException.class,
            () -> controlPlane.configure(Map.of("username", "username", "password", "password")));
        assertEquals("Missing required configuration \"connection.string\" which has no default value.",
            e.getMessage());
    }

    @Test
    void emptyReadConnectionStringIsReportedAsNotConfigured() {
        final var controlPlane = new PostgresControlPlane(new MockTime());

        final var e = assertThrows(ControlPlaneNotConfiguredException.class,
            () -> controlPlane.configure(Map.of(
                "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
                "username", "username",
                "password", "password",
                "read.connection.string", "")));
        assertEquals("Invalid value  for configuration connection.string: String must be non-empty",
            e.getMessage());
    }

    @Test
    void unrelatedInvalidConfigIsNotReportedAsNotConfigured() {
        final var controlPlane = new PostgresControlPlane(new MockTime());

        // A typo in a numeric key is a genuine misconfiguration, distinct from an unset control
        // plane: it must surface as a plain ConfigException so it is not mistaken for one.
        final ConfigException e = assertThrows(ConfigException.class,
            () -> controlPlane.configure(Map.of(
                "connection.string", "jdbc:postgresql://127.0.0.1:5432/inkless",
                "username", "username",
                "password", "password",
                "max.connections", "not-a-number")));
        assertEquals(ConfigException.class, e.getClass());
    }
}
