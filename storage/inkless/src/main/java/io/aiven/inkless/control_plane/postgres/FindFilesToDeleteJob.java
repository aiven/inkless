// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariDataSource;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

import io.aiven.inkless.control_plane.FileToDelete;

import static org.jooq.generated.Tables.FILES;
import static org.jooq.generated.Tables.FILES_TO_DELETE;

public class FindFilesToDeleteJob implements Callable<List<FileToDelete>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindFilesToDeleteJob.class);

    private final Time time;
    private final HikariDataSource hikariDataSource;

    public FindFilesToDeleteJob(final Time time, final HikariDataSource hikariDataSource) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
    }

    @Override
    public List<FileToDelete> call() {
        // TODO add retry
        try {
            return runOnce();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<FileToDelete> runOnce() throws SQLException {
        final Connection connection;
        try {
            connection = hikariDataSource.getConnection();
            // Since we're calling a function here.
            connection.setAutoCommit(true);
        } catch (final SQLException e) {
            LOGGER.error("Cannot get Postgres connection", e);
            throw e;
        }

        try (connection) {
            return runWithConnection(connection);
        } catch (final Exception e) {
            LOGGER.error("Error executing query", e);
            try {
                connection.rollback();
            } catch (final SQLException ex) {
                LOGGER.error("Error rolling back transaction", e);
            }
            throw e;
        }
    }

    private List<FileToDelete> runWithConnection(final Connection connection) throws SQLException {
        final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);

        final var fetchResult = ctx.select(
                FILES.FILE_ID,
                FILES.OBJECT_KEY,
                FILES_TO_DELETE.MARKED_FOR_DELETION_AT
            ).from(FILES_TO_DELETE)
            .innerJoin(FILES)
            .using(FILES.FILE_ID)
            .fetchStream();
        return fetchResult.map(r -> new FileToDelete(
                r.get(FILES.OBJECT_KEY),
                r.get(FILES_TO_DELETE.MARKED_FOR_DELETION_AT)
            ))
            .toList();
    }
}
