// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

class InMemoryControlPlaneTest extends AbstractControlPlaneTest {
    @Override
    protected ControlPlane createControlPlane() {
        return new InMemoryControlPlane(time, metadataView);
    }
}
