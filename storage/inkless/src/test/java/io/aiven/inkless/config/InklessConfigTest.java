// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.config;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InklessConfigTest {
    @Test
    void objectStorageConfiguration() {
        final String backendClass = ConfigTestStorageBackend.class.getCanonicalName();
        final var config = new InklessConfig(
            Map.of(
                "storage.backend.class", backendClass,
                "storage.a", "1",
                "storage.b", "str",
                "unrelated", "x"
            )
        );
        assertThat(config.storage()).isInstanceOf(ConfigTestStorageBackend.class);
        final var storage = (ConfigTestStorageBackend) config.storage();
        assertThat(storage.passedConfig).isEqualTo(Map.of(
            "backend.class", backendClass,
            "a", "1",
            "b", "str"));
    }
}
