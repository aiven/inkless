// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import org.apache.kafka.common.test.TestUtils;

import org.junit.jupiter.api.TestInfo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreSQLContainer extends org.testcontainers.containers.PostgreSQLContainer<PostgreSQLContainer> {
    public PostgreSQLContainer(final String dockerImageName) {
        super(dockerImageName);
    }

    public String getJdbcUrl(final String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return (
            "jdbc:postgresql://" +
                getHost() +
                ":" +
                getMappedPort(POSTGRESQL_PORT) +
                "/" +
                databaseName +
                additionalUrlParams
        );
    }

    public void createDatabase(final String dbName) {
        try (
            final Connection connection = DriverManager.getConnection(
                getJdbcUrl(),
                PostgreSQLTestContainer.USERNAME,
                PostgreSQLTestContainer.PASSWORD);
            final Statement statement = connection.createStatement()
        ) {
            statement.execute("CREATE DATABASE \"" + dbName + "\"");
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String dbNameFromTestInfo(final TestInfo testInfo) {
        String dbName = testInfo.getDisplayName()
            .toLowerCase()
            .replace(" ", "")
            .replace("\"", "")
            .replace(",", "_")
            .replace(".", "_")
            .replace("=", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("[", "")
            .replace("]", "");
        dbName = dbName.substring(0, Math.min(40, dbName.length()));
        dbName += "_" + TestUtils.randomString(20);
        return dbName.toLowerCase();
    }
}
