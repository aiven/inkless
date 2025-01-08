// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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
        config.setAutoCommit(false);
        hikariDataSource = new HikariDataSource(config);
    }

    @BeforeEach
    void createDBForTest(final TestInfo testInfo) {
        dbName = PostgreSQLContainer.dbNameFromTestInfo(testInfo);

        pgContainer.createDatabase(dbName);

        final Flyway flyway = Flyway.configure().dataSource(
            pgContainer.getJdbcUrl(dbName),
            PostgreSQLTestContainer.USERNAME,
            PostgreSQLTestContainer.PASSWORD).load();
        flyway.migrate();
    }
}
