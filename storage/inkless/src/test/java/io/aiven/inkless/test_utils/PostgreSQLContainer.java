// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class PostgreSQLContainer extends org.testcontainers.containers.PostgreSQLContainer<PostgreSQLContainer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgreSQLContainer.class);

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

    // synchronized to not give PG a reason to complain about too many simultaneous connections.
    public synchronized void createDatabase(final String dbName) {
        try (
            final Connection connection = DriverManager.getConnection(
                getJdbcUrl(),
                PostgreSQLTestContainer.USERNAME,
                PostgreSQLTestContainer.PASSWORD);
            final Statement statement = connection.createStatement()
        ) {
            LOGGER.info("Creating DB {}", dbName);
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
            .replace("-", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("[", "")
            .replace("]", "");
        dbName = dbName.substring(0, Math.min(40, dbName.length()));
        dbName = "d" + dbName;  // handle preceding digits
        dbName += "_" + TestUtils.randomString(20);
        return dbName.toLowerCase();
    }
}
