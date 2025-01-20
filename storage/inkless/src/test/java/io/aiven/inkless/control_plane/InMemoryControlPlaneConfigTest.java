// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryControlPlaneConfigTest {
    @Test
    void fullConfig() {
        final var config = new InMemoryControlPlaneConfig(
            Map.of(
                "file.merge.size.threshold.bytes", "1234"
            )
        );

        assertThat(config.fileMergeSizeThresholdBytes()).isEqualTo(1234);
    }

    @Test
    void minimalConfig() {
        final var config = new InMemoryControlPlaneConfig(
            Map.of()
        );

        assertThat(config.fileMergeSizeThresholdBytes()).isEqualTo(100 * 1024 * 1024);
    }
}
