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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;
import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.postgres.JobUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DeleteRecordsJobs implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteRecordsJobs.class);

    private final Time time;
    private final Connection connection;
    private final GetLogInfoJob getLogInfoJob;
    private final PreparedStatement setLogStartOffsetPreparedStatement;
    private final PreparedStatement deleteBatchesPreparedStatement;
    private final PreparedStatement subtractByteSizePreparedStatement;
    private final MarkFileForDeletionIfNeededRoutine markFileForDeletionIfNeededRoutine;

    DeleteRecordsJobs(final Time time,
                      final Connection connection,
                      final GetLogInfoJob getLogInfoJob,
                      final MarkFileForDeletionIfNeededRoutine markFileForDeletionIfNeededRoutine) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");
        this.getLogInfoJob = Objects.requireNonNull(getLogInfoJob, "getLogInfoJob cannot be null");
        this.markFileForDeletionIfNeededRoutine = Objects.requireNonNull(markFileForDeletionIfNeededRoutine, "markFileForDeletionIfNeededRoutine cannot be null");

        this.setLogStartOffsetPreparedStatement = connection.prepareStatement(
            "UPDATE logs " +
                "SET log_start_offset = ? " +
                "WHERE topic_id = ? " +
                "  AND partition = ? "
        );
        this.deleteBatchesPreparedStatement = connection.prepareStatement(
            "DELETE FROM batches " +
                "WHERE topic_id = ? " +
                "  AND partition = ? " +
                "  AND last_offset < ? " +
                "RETURNING file_id, byte_size"
        );
        this.subtractByteSizePreparedStatement = connection.prepareStatement(
            "UPDATE logs " +
                "SET byte_size = byte_size - ? " +
                "WHERE topic_id = ? " +
                "  AND partition = ? "
        );
    }

    List<DeleteRecordsResponse> call(final List<DeleteRecordsRequest> requests,
                                     final Consumer<Long> durationCallback) {
        Objects.requireNonNull(requests, "requests cannot be null");
        Objects.requireNonNull(durationCallback, "durationCallback cannot be null");

        if (requests.isEmpty()) {
            return List.of();
        }
        return JobUtils.run(() -> runOnce(requests), time, durationCallback);
    }

    private List<DeleteRecordsResponse> runOnce(final List<DeleteRecordsRequest> requests) {
        final long now = time.milliseconds();
        try {
            final List<DeleteRecordsResponse> result = new ArrayList<>();
            for (final var r : requests) {
                result.add(deleteRecordsForPartition(now, r));
            }
            connection.commit();
            return result;
        } catch (final SQLException e) {
            try {
                connection.rollback();
            } catch (final SQLException sqlEx) {
                LOGGER.error("Error rolling back", sqlEx);
            }
            throw new RuntimeException(e);
        }
    }

    private DeleteRecordsResponse deleteRecordsForPartition(final long now, final DeleteRecordsRequest request) throws SQLException {
        final GetLogInfoResponse logInfo = getLogInfoJob.call(
            List.of(new GetLogInfoRequest(request.topicIdPartition().topicId(), request.topicIdPartition().partition())), d -> {}).get(0);
        if (logInfo.errors() == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            return DeleteRecordsResponse.unknownTopicOrPartition();
        }

        final long convertedOffset = request.offset() == org.apache.kafka.common.requests.DeleteRecordsRequest.HIGH_WATERMARK
            ? logInfo.highWatermark()
            : request.offset();
        if (convertedOffset < 0 || convertedOffset > logInfo.highWatermark()) {
            return DeleteRecordsResponse.offsetOutOfRange();
        }

        long logStartOffset = logInfo.logStartOffset();
        if (convertedOffset > logInfo.logStartOffset()) {
            setLogStartOffsetPreparedStatement.clearParameters();
            setLogStartOffsetPreparedStatement.setLong(1, convertedOffset);
            setLogStartOffsetPreparedStatement.setString(2, request.topicIdPartition().topicId().toString());
            setLogStartOffsetPreparedStatement.setInt(3, request.topicIdPartition().partition());
            setLogStartOffsetPreparedStatement.executeUpdate();
            logStartOffset = convertedOffset;
        }

        deleteBatchesPreparedStatement.clearParameters();
        deleteBatchesPreparedStatement.setString(1, request.topicIdPartition().topicId().toString());
        deleteBatchesPreparedStatement.setInt(2, request.topicIdPartition().partition());
        deleteBatchesPreparedStatement.setLong(3, logStartOffset);
        final Set<Integer> affectedFiles = new HashSet<>();
        long deletedBytes = 0;
        try (final ResultSet resultSet = deleteBatchesPreparedStatement.executeQuery()) {
            while (resultSet.next()) {
                affectedFiles.add(resultSet.getInt("file_id"));
                deletedBytes += resultSet.getLong("byte_size");
            }
        }
        for (final Integer fileId : affectedFiles) {
            markFileForDeletionIfNeededRoutine.run(now, fileId);
        }

        subtractByteSizePreparedStatement.clearParameters();
        subtractByteSizePreparedStatement.setLong(1, deletedBytes);
        subtractByteSizePreparedStatement.setString(2, request.topicIdPartition().topicId().toString());
        subtractByteSizePreparedStatement.setInt(3, request.topicIdPartition().partition());
        subtractByteSizePreparedStatement.executeUpdate();

        return DeleteRecordsResponse.success(logStartOffset);
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(setLogStartOffsetPreparedStatement, "setLogStartOffsetPreparedStatement");
        Utils.closeQuietly(deleteBatchesPreparedStatement, "deleteBatchesPreparedStatement");
        Utils.closeQuietly(subtractByteSizePreparedStatement, "setByteSizePreparedStatement");
    }
}
