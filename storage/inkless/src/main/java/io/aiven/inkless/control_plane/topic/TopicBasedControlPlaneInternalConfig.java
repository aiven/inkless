package io.aiven.inkless.control_plane.topic;

import java.nio.file.Path;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.inkless.control_plane.AbstractControlPlaneConfig;

public class TopicBasedControlPlaneInternalConfig extends AbstractControlPlaneConfig {
    public static final String DATABASE_DIR_CONFIG = "database.dir";
    private static final String DATABASE_DIR_DOC = "The database file directory.";

    public static ConfigDef configDef() {
        return baseConfigDef()
            .define(
                DATABASE_DIR_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                DATABASE_DIR_DOC
            );
    }

    public TopicBasedControlPlaneInternalConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }

    public Path dbDir() {
        final String pathDir = getString(DATABASE_DIR_CONFIG);
        return Path.of(pathDir);
    }
}
