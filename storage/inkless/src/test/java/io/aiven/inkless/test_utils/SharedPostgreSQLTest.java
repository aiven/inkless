// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;


import org.apache.kafka.common.test.TestUtils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.IsolationLevel;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Testcontainers
public abstract class SharedPostgreSQLTest {
    @Container
    protected static PostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    protected String dbName;
    protected HikariDataSource hikariDataSource;

    @BeforeEach
    void setupConnectionPool() {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(pgContainer.getJdbcUrl(dbName));
        config.setUsername(PostgreSQLTestContainer.USERNAME);
        config.setPassword(PostgreSQLTestContainer.PASSWORD);
        config.setTransactionIsolation(IsolationLevel.TRANSACTION_REPEATABLE_READ.name());
        config.setAutoCommit(false);
        hikariDataSource = new HikariDataSource(config);
    }

    @BeforeEach
    void createDBForTest(final TestInfo testInfo) {
        dbName = testInfo.getDisplayName()
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

        final Flyway flyway = Flyway.configure().dataSource(
            pgContainer.getJdbcUrl(dbName),
            PostgreSQLTestContainer.USERNAME,
            PostgreSQLTestContainer.PASSWORD).load();
        flyway.migrate();
    }
}
