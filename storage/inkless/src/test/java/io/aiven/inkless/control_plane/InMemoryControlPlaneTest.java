// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.junit.jupiter.api.TestInfo;


class InMemoryControlPlaneTest extends AbstractControlPlaneTest {
    @Override
    protected ControlPlaneAndConfigs createControlPlane(final TestInfo testInfo) {
        return new ControlPlaneAndConfigs(new InMemoryControlPlane(time), BASE_CONFIG);
    }

    @Override
    protected void tearDownControlPlane() {
        // no-op
    }
}
