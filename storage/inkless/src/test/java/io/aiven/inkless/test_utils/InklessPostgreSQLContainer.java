/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
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
