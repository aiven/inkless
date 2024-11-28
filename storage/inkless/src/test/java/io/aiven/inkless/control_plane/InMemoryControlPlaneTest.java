// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.junit.jupiter.api.BeforeEach;

class InMemoryControlPlaneTest extends AbstractControlPlaneTest {
    @BeforeEach
    void createControlPlane() {
        controlPlane = new InMemoryControlPlane(time, metadataView);
    }
}
