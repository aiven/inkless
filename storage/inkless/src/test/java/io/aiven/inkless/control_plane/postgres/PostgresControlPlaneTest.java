// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.aiven.inkless.control_plane.AbstractControlPlaneTest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

@Testcontainers
class PostgresControlPlaneTest extends AbstractControlPlaneTest {
    @Container
    static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    @Override
    protected ControlPlaneAndConfigs createControlPlane(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);

        final var controlPlane = new PostgresControlPlane(time);
        final Map<String, String> configs = new HashMap<>(BASE_CONFIG);
        configs.putAll(Map.of(
            "connection.string", pgContainer.getUserJdbcUrl(),
            "username", pgContainer.getUsername(),
            "password", pgContainer.getPassword()
        ));
        return new ControlPlaneAndConfigs(controlPlane, configs);
    }

    @Override
    protected void tearDownControlPlane() throws IOException {
        controlPlane.close();
        pgContainer.tearDown();
    }
}
