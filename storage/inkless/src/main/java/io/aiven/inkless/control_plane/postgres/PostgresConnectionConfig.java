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

    public static final String CONNECTION_POOL_TIMEOUT_MS_CONFIG = "connection.pool.timeout.ms";
    private static final String CONNECTION_POOL_TIMEOUT_MS_DOC =
        "Maximum time in milliseconds to wait for a PostgreSQL connection from the pool.";
    private static final long CONNECTION_POOL_TIMEOUT_MS_DEFAULT = 5_000;

    public static final String TCP_CONNECT_TIMEOUT_MS_CONFIG = "tcp.connect.timeout.ms";
    private static final String TCP_CONNECT_TIMEOUT_MS_DOC =
        "Maximum time in milliseconds to establish a PostgreSQL TCP connection.";
    private static final int TCP_CONNECT_TIMEOUT_MS_DEFAULT = 5_000;

    public static final String SOCKET_TIMEOUT_MS_CONFIG = "socket.timeout.ms";
    private static final String SOCKET_TIMEOUT_MS_DOC =
        "Maximum time in milliseconds to wait for PostgreSQL socket reads.";
    private static final int SOCKET_TIMEOUT_MS_DEFAULT = 5_000;

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
            .define(
                CONNECTION_POOL_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.LONG,
                CONNECTION_POOL_TIMEOUT_MS_DEFAULT,
                ConfigDef.Range.atLeast(250),
                ConfigDef.Importance.MEDIUM,
                CONNECTION_POOL_TIMEOUT_MS_DOC
            )
            .define(
                TCP_CONNECT_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                TCP_CONNECT_TIMEOUT_MS_DEFAULT,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM,
                TCP_CONNECT_TIMEOUT_MS_DOC
            )
            .define(
                SOCKET_TIMEOUT_MS_CONFIG,
                ConfigDef.Type.INT,
                SOCKET_TIMEOUT_MS_DEFAULT,
                ConfigDef.Range.atLeast(1),
                ConfigDef.Importance.MEDIUM,
                SOCKET_TIMEOUT_MS_DOC
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

    public long connectionPoolTimeoutMs() {
        return getLong(CONNECTION_POOL_TIMEOUT_MS_CONFIG);
    }

    public int tcpConnectTimeoutMs() {
        return getInt(TCP_CONNECT_TIMEOUT_MS_CONFIG);
    }

    public int socketTimeoutMs() {
        return getInt(SOCKET_TIMEOUT_MS_CONFIG);
    }
}
