// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.config;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.storage_backend.common.StorageBackend;

public class InklessConfig extends AbstractConfig {
    private static final String PREFIX = "inkless.";

    private static final String STORAGE_PREFIX = "storage.";
    private static final String STORAGE_BACKEND_CLASS_CONFIG = STORAGE_PREFIX + "backend.class";
    private static final String STORAGE_BACKEND_CLASS_DOC = "The storage backend implementation class";

    public static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();

        configDef.define(
            STORAGE_BACKEND_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            STORAGE_BACKEND_CLASS_DOC
        );

        return configDef;
    }

    public InklessConfig(final AbstractConfig config) {
        this(config.originalsWithPrefix(InklessConfig.PREFIX));
    }

    // Visible for testing
    InklessConfig(final Map<String, ?> props) {
        super(configDef(), props);
        System.out.println(props);
    }

    public StorageBackend storage() {
        final Class<?> storageClass = getClass(STORAGE_BACKEND_CLASS_CONFIG);
        final StorageBackend storage = Utils.newInstance(storageClass, StorageBackend.class);
        storage.configure(this.originalsWithPrefix(STORAGE_PREFIX));
        return storage;
    }
}
