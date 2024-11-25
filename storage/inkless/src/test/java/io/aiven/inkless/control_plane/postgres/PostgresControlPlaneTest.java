// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.test.TestUtils;

import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
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
        String dbName = testInfo.getDisplayName()
            .toLowerCase()
            .replace(" ", "")
            .replace(",", "-")
            .replace("(", "")
            .replace(")", "")
            .replace("[", "")
            .replace("]", "");
        dbName += "_" + TestUtils.randomString(20);
        dbName = dbName.toLowerCase();

        try (final Connection connection = DriverManager.getConnection(
            pgContainer.getJdbcUrl(), PostgreSQLTestContainer.USERNAME, PostgreSQLTestContainer.PASSWORD);
             final Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + dbName);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }

        final var controlPlane = new PostgresControlPlane(time, metadataView);
        controlPlane.configure(Map.of(
            "connection.string", pgContainer.getJdbcUrl(dbName),
            "username", pgContainer.getUsername(),
            "password", pgContainer.getPassword()
        ));
        return controlPlane;
    }
}
