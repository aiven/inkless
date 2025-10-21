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
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.control_plane.postgres.JobUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

class ListOffsetsJob implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListOffsetsJob.class);

    private final Time time;
    private final Connection connection;
    private final GetLogInfoJob getLogInfoJob;
    private final PreparedStatement findMaxTimestampPreparedStatement;
    private final PreparedStatement findByTimestampPreparedStatement;

    ListOffsetsJob(final Time time,
                   final Connection connection,
                   final GetLogInfoJob getLogInfoJob) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");
        this.getLogInfoJob = Objects.requireNonNull(getLogInfoJob, "getLogInfoJob cannot be null");

        // See how timestamps are assigned in
        // https://github.com/aiven/inkless/blob/e124d3975bdb3a9ec85eee2fba7a1b0a6967d3a6/storage/src/main/java/org/apache/kafka/storage/internals/log/LogValidator.java#L271-L276
        this.findMaxTimestampPreparedStatement = connection.prepareStatement(
            "SELECT last_offset, CASE timestamp_type WHEN 1 THEN log_append_timestamp ELSE batch_max_timestamp END AS timestamp " +
                "FROM batches " +
                "WHERE topic_id = ?" +
                "  AND partition = ? " +
                "  AND CASE timestamp_type WHEN 1 THEN log_append_timestamp ELSE batch_max_timestamp END = (" +
                "    SELECT MAX (CASE timestamp_type WHEN 1 THEN log_append_timestamp ELSE batch_max_timestamp END) " +
                "    FROM batches " +
                "    WHERE topic_id = ? AND partition = ?" +
                "  )"
        );
        this.findByTimestampPreparedStatement = connection.prepareStatement(
            "SELECT CASE timestamp_type WHEN 1 THEN log_append_timestamp ELSE batch_max_timestamp END AS timestamp, base_offset " +
                "FROM batches " +
                "WHERE topic_id = ?" +
                "  AND partition = ? " +
                "  AND CASE timestamp_type WHEN 1 THEN log_append_timestamp ELSE batch_max_timestamp END >= ? " +
                "ORDER BY batch_id " +
                "LIMIT 1"
        );
    }

    List<ListOffsetsResponse> call(final List<ListOffsetsRequest> requests,
                                   final Consumer<Long> durationCallback) {
        Objects.requireNonNull(requests, "requests cannot be null");
        Objects.requireNonNull(durationCallback, "durationCallback cannot be null");
        if (requests.isEmpty()) {
            return List.of();
        }
        return JobUtils.run(() -> runOnce(requests), time, durationCallback);
    }

    private List<ListOffsetsResponse> runOnce(final List<ListOffsetsRequest> requests) {
        try {
            final List<ListOffsetsResponse> list = new ArrayList<>();
            for (ListOffsetsRequest request : requests) {
                list.add(listOffset(request));
            }
            return list;
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ListOffsetsResponse listOffset(final ListOffsetsRequest request) throws SQLException {
        final GetLogInfoResponse logInfo = getLogInfoJob.call(
            List.of(new GetLogInfoRequest(request.topicIdPartition().topicId(), request.topicIdPartition().partition())), d -> {}).get(0);
        if (logInfo.errors() == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            LOGGER.warn("Unexpected non-existing partition {}", request.topicIdPartition());
            return ListOffsetsResponse.unknownTopicOrPartition(request.topicIdPartition());
        }

        final long timestamp = request.timestamp();
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP || timestamp == ListOffsetsRequest.EARLIEST_LOCAL_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), NO_TIMESTAMP, logInfo.logStartOffset());
        } else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), NO_TIMESTAMP, logInfo.highWatermark());
        } else if (timestamp == ListOffsetsRequest.MAX_TIMESTAMP) {
            long maxTimestamp = NO_TIMESTAMP;
            long maxTimestampOffset = -1;
            findMaxTimestampPreparedStatement.clearParameters();
            findMaxTimestampPreparedStatement.setString(1, request.topicIdPartition().topicId().toString());
            findMaxTimestampPreparedStatement.setInt(2, request.topicIdPartition().partition());
            findMaxTimestampPreparedStatement.setString(3, request.topicIdPartition().topicId().toString());
            findMaxTimestampPreparedStatement.setInt(4, request.topicIdPartition().partition());
            try (final ResultSet resultSet = findMaxTimestampPreparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    maxTimestamp = resultSet.getLong("timestamp");
                    maxTimestampOffset = resultSet.getLong("last_offset");
                }
            }
            return ListOffsetsResponse.success(request.topicIdPartition(), maxTimestamp, maxTimestampOffset);
        } else if (timestamp == ListOffsetsRequest.LATEST_TIERED_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), NO_TIMESTAMP, -1);
        } else if (timestamp >= 0) {
            findByTimestampPreparedStatement.clearParameters();
            findByTimestampPreparedStatement.setString(1, request.topicIdPartition().topicId().toString());
            findByTimestampPreparedStatement.setInt(2, request.topicIdPartition().partition());
            findByTimestampPreparedStatement.setLong(3, timestamp);
            try (final ResultSet resultSet = findByTimestampPreparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    final long batchTimestamp = resultSet.getLong("timestamp");
                    final long baseOffset = resultSet.getLong("base_offset");
                    return ListOffsetsResponse.success(request.topicIdPartition(), batchTimestamp,
                        Math.max(logInfo.logStartOffset(), baseOffset));
                } else {
                    return ListOffsetsResponse.success(request.topicIdPartition(), NO_TIMESTAMP, -1);
                }
            }
        } else {
            LOGGER.error("listOffset request for timestamp {} in {} unsupported", timestamp, request.topicIdPartition());
            return ListOffsetsResponse.unknownServerError(request.topicIdPartition());
        }
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(findMaxTimestampPreparedStatement, "findMaxTimestampPreparedStatement");
        Utils.closeQuietly(findByTimestampPreparedStatement, "findByTimestampPreparedStatement");
    }
}
