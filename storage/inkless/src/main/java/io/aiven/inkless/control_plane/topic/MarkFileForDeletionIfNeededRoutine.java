/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
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
package io.aiven.inkless.control_plane.topic;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.control_plane.FileState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MarkFileForDeletionIfNeededRoutine implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MarkFileForDeletionIfNeededRoutine.class);

    private final Time time;
    private final Connection connection;
    private final PreparedStatement getOneBatchFromFilePreparedStatement;
    private final PreparedStatement markFileForDeletionPreparedStatement;

    MarkFileForDeletionIfNeededRoutine(final Time time, final Connection connection) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");

        this.getOneBatchFromFilePreparedStatement = connection.prepareStatement(
            "SELECT batch_id " +
                "FROM batches " +
                "WHERE file_id = ? " +
                "LIMIT 1");
        this.markFileForDeletionPreparedStatement = connection.prepareStatement(
            "UPDATE files " +
                "SET state = ?, marked_for_deletion_at = ? " +
                "WHERE file_id = ? ");
    }

    void run(final long now, final int fileId) throws SQLException {
        getOneBatchFromFilePreparedStatement.clearParameters();
        getOneBatchFromFilePreparedStatement.setInt(1, fileId);
        try (final ResultSet resultSet = getOneBatchFromFilePreparedStatement.executeQuery()) {
            if (!resultSet.next()) {
                markFileForDeletionPreparedStatement.clearParameters();
                markFileForDeletionPreparedStatement.setString(1, FileState.DELETING.toString());
                markFileForDeletionPreparedStatement.setLong(2, now);
                markFileForDeletionPreparedStatement.setInt(3, fileId);
                markFileForDeletionPreparedStatement.executeUpdate();
            }
        }
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(getOneBatchFromFilePreparedStatement, "getOneBatchFromFilePreparedStatement");
        Utils.closeQuietly(markFileForDeletionPreparedStatement, "markFileForDeletionPreparedStatement");
    }
}
