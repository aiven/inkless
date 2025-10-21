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
import java.sql.SQLException;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.control_plane.DeleteFilesRequest;
import io.aiven.inkless.control_plane.FileState;
import io.aiven.inkless.control_plane.postgres.JobUtils;
import io.aiven.inkless.generated.CoordinatorDeleteFileEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DeleteFilesJob implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListOffsetsJob.class);

    private final Time time;
    private final Connection connection;
    private final PreparedStatement preparedStatement;

    DeleteFilesJob(final Time time,
                   final Connection connection) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");

        this.preparedStatement = connection.prepareStatement(
            "DELETE FROM files " +
                "WHERE object_key = ? " +
                "  AND state = ?"
        );
    }

    CoordinatorDeleteFileEventReplayResult replay(
        final CoordinatorDeleteFileEvent event,
        final Consumer<Long> durationCallback
    ) {
        Objects.requireNonNull(event, "event cannot be null");
        Objects.requireNonNull(durationCallback, "durationCallback cannot be null");
        JobUtils.run(() -> runOnce(event.objectKeys()), time, durationCallback);
        return new CoordinatorDeleteFileEventReplayResult();
    }

    void runOnce(final Collection<String> objectKeys) {
        try {
            for (final String objectKey : objectKeys) {
                preparedStatement.clearParameters();
                preparedStatement.setString(1, objectKey);
                preparedStatement.setString(2, FileState.DELETING.toString());
                preparedStatement.executeUpdate();
            }
            connection.commit();
        } catch (final SQLException e) {
            try {
                connection.rollback();
            } catch (final SQLException sqlEx) {
                LOGGER.error("Error rolling back", sqlEx);
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(preparedStatement, "preparedStatement");
    }
}
