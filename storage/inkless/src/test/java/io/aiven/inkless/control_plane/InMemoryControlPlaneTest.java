// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.junit.jupiter.api.TestInfo;

class InMemoryControlPlaneTest extends AbstractControlPlaneTest {
    @Override
    protected ControlPlane createControlPlane(final TestInfo testInfo) {
        return new InMemoryControlPlane(time);
    }
}
