// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;

import io.aiven.inkless.control_plane.AbstractControlPlaneConfig;

public class PostgresControlPlaneConfig extends AbstractControlPlaneConfig {
    public static final String CONNECTION_STRING_CONFIG = "connection.string";
    private static final String CONNECTION_STRING_DOC = "PostgreSQL connection string";

    public static final String USERNAME_CONFIG = "username";
    private static final String USERNAME_DOC = "Username";

    public static final String PASSWORD_CONFIG = "password";
    private static final String PASSWORD_DOC = "Password";

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
            );
    }

    public PostgresControlPlaneConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
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
}
