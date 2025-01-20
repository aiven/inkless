// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class InMemoryControlPlaneConfig extends AbstractControlPlaneConfig {
    public static ConfigDef configDef() {
        return baseConfigDef();
    }

    public InMemoryControlPlaneConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
    }
}
