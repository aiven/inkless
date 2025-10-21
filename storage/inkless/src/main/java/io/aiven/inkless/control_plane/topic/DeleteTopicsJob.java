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
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.control_plane.postgres.JobUtils;
import io.aiven.inkless.generated.CoordinatorDeleteTopicEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DeleteTopicsJob implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteRecordsJobs.class);

    private final Time time;
    private final Connection connection;
    private final PreparedStatement deleteBatchesPreparedStatement;
    private final PreparedStatement deleteLogsPreparedStatement;
    private final MarkFileForDeletionIfNeededRoutine markFileForDeletionIfNeededRoutine;

    DeleteTopicsJob(final Time time,
                    final Connection connection,
                    final MarkFileForDeletionIfNeededRoutine markFileForDeletionIfNeededRoutine) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");
        this.markFileForDeletionIfNeededRoutine = Objects.requireNonNull(markFileForDeletionIfNeededRoutine, "markFileForDeletionIfNeededRoutine cannot be null");

        this.deleteBatchesPreparedStatement = connection.prepareStatement(
            "DELETE FROM batches " +
                "WHERE topic_id = ? " +
                "RETURNING file_id"
        );
        this.deleteLogsPreparedStatement = connection.prepareStatement(
            "DELETE FROM logs " +
                "WHERE topic_id = ? "
        );
    }

    CoordinatorDeleteTopicEventReplayResult replay(
        final CoordinatorDeleteTopicEvent event,
        final Consumer<Long> durationCallback
    ) {
        Objects.requireNonNull(event, "event cannot be null");
        Objects.requireNonNull(durationCallback, "durationCallback cannot be null");
        JobUtils.run(() -> runOnce(event.topicIds()), time, durationCallback);
        return new CoordinatorDeleteTopicEventReplayResult();
    }

    private void runOnce(final Collection<Uuid> topicIds) {
        final long now = time.milliseconds();

        try {
            final Set<Integer> affectedFiles = new HashSet<>();
            for (final Uuid topicId : topicIds) {
                affectedFiles.addAll(deleteTopic(topicId));
            }
            for (final Integer fileId : affectedFiles) {
                markFileForDeletionIfNeededRoutine.run(now, fileId);
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

    private Set<Integer> deleteTopic(final Uuid topicId) throws SQLException {
        final Set<Integer> affectedFiles = new HashSet<>();
        deleteBatchesPreparedStatement.clearParameters();
        deleteBatchesPreparedStatement.setString(1, topicId.toString());
        try (final ResultSet resultSet = deleteBatchesPreparedStatement.executeQuery()) {
            while (resultSet.next()) {
                affectedFiles.add(resultSet.getInt("file_id"));
            }
        }

        deleteLogsPreparedStatement.clearParameters();
        deleteLogsPreparedStatement.setString(1, topicId.toString());
        deleteLogsPreparedStatement.executeUpdate();

        return affectedFiles;
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(deleteBatchesPreparedStatement, "deleteBatchesPreparedStatement");
        Utils.closeQuietly(deleteLogsPreparedStatement, "deleteLogsPreparedStatement");
    }
}
