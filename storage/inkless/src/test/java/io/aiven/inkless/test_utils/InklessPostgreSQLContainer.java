// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import org.apache.kafka.test.TestUtils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class InklessPostgreSQLContainer extends PostgreSQLContainer<InklessPostgreSQLContainer> {
    private static final Logger LOGGER = LoggerFactory.getLogger(InklessPostgreSQLContainer.class);

    private HikariDataSource hikariDataSource;
    private DSLContext jooqCtx;
    private String userDatabaseName;

    public InklessPostgreSQLContainer(final String dockerImageName) {
        super(dockerImageName);
    }

    public void createDatabase(final TestInfo testInfo) {
        final var dbName = dbNameFromTestInfo(testInfo);
        createDatabase(dbName);
    }

    // synchronized to not give PG a reason to complain about too many simultaneous connections.
    public synchronized void createDatabase(final String databaseName) {
        userDatabaseName = databaseName;
        try (
            final Connection connection = DriverManager.getConnection(
                getJdbcUrl(),
                PostgreSQLTestContainer.USERNAME,
                PostgreSQLTestContainer.PASSWORD);
            final Statement statement = connection.createStatement()
        ) {
            LOGGER.info("Creating DB {}", databaseName);
            statement.execute("CREATE DATABASE \"" + databaseName + "\"");
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void migrate(){
        setupConnectionPool();
        final Flyway flyway = Flyway.configure().dataSource(getDataSource()).load();
        flyway.migrate();
    }

    private void setupConnectionPool() {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(getUserJdbcUrl());
        config.setUsername(PostgreSQLTestContainer.USERNAME);
        config.setPassword(PostgreSQLTestContainer.PASSWORD);
        config.setAutoCommit(false);
        hikariDataSource = new HikariDataSource(config);
        jooqCtx = DSL.using(hikariDataSource, SQLDialect.POSTGRES);
    }

    private static String dbNameFromTestInfo(final TestInfo testInfo) {
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

    public DSLContext getJooqCtx() {
        return jooqCtx;
    }

    public HikariDataSource getDataSource() {
        return hikariDataSource;
    }

    public void tearDown() {
        if (hikariDataSource != null) {
            hikariDataSource.close();
        }
    }

    public String getUserJdbcUrl() {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return (
            "jdbc:postgresql://" +
                getHost() +
                ":" +
                getMappedPort(POSTGRESQL_PORT) +
                "/" +
                userDatabaseName +
                additionalUrlParams
        );
    }
}
