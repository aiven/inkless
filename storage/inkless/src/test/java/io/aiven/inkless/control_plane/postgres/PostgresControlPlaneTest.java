// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

import io.aiven.inkless.control_plane.AbstractControlPlaneTest;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.test_utils.PostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

@Testcontainers
class PostgresControlPlaneTest extends AbstractControlPlaneTest {
    @Container
    static PostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    @Override
    protected ControlPlane createControlPlane(final TestInfo testInfo) {
        final var dbName = PostgreSQLContainer.dbNameFromTestInfo(testInfo);

        pgContainer.createDatabase(dbName);

        final var controlPlane = new PostgresControlPlane(time, metadataView);
        controlPlane.configure(Map.of(
            "connection.string", pgContainer.getJdbcUrl(dbName),
            "username", pgContainer.getUsername(),
            "password", pgContainer.getPassword()
        ));
        return controlPlane;
    }
}
