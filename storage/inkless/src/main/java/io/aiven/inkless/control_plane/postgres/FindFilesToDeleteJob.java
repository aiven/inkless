// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

import io.aiven.inkless.control_plane.FileToDelete;

public class FindFilesToDeleteJob implements Callable<List<FileToDelete>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindFilesToDeleteJob.class);

    private static final String SELECT_FILES_TO_DELETE = """
        SELECT files.file_id, files.object_key, files_to_delete.marked_for_deletion_at
        FROM files_to_delete
            INNER JOIN files USING (file_id)
        """;

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
        Objects.requireNonNull(connection, "connection cannot be null");
        final List<FileToDelete> result = new ArrayList<>();
        try (final PreparedStatement statement = connection.prepareStatement(SELECT_FILES_TO_DELETE)) {
            try (final ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    final String objectKey = resultSet.getString("object_key");
                    final Instant markedForDeletionAt = resultSet.getTimestamp("marked_for_deletion_at").toInstant();
                    result.add(new FileToDelete(objectKey, markedForDeletionAt));
                }
            }
        }
        return result;
    }
}
