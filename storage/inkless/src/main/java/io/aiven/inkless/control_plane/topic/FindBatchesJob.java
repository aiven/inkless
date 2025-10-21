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

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.postgres.converters.ShortToTimestampTypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FindBatchesJob implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindBatchesJob.class);

    private final Time time;
    private final Connection connection;
    private final GetLogInfoJob getLogInfoJob;
    private final PreparedStatement getBatchesPreparedStatement;
    private final ShortToTimestampTypeConverter shortToTimestampTypeConverter = new ShortToTimestampTypeConverter();

    FindBatchesJob(final Time time,
                   final Connection connection,
                   final GetLogInfoJob getLogInfoJob) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");
        this.getLogInfoJob = Objects.requireNonNull(getLogInfoJob, "getLogInfoJob cannot be null");

        this.getBatchesPreparedStatement = connection.prepareStatement(
            "SELECT batch_id, f.object_key AS object_key, magic, byte_offset, byte_size, base_offset, last_offset, log_append_timestamp, batch_max_timestamp, timestamp_type " +
                "FROM batches AS b " +
                "  INNER JOIN files AS f ON b.file_id = f.file_id " +
                "WHERE topic_id = ? AND partition = ? AND last_offset >= ? " +
                "ORDER BY base_offset"
        );
    }

    List<FindBatchResponse> call(final List<FindBatchRequest> requests,
                                 final int fetchMaxBytes,
                                 final int maxBatchesPerPartition,
                                 final Consumer<Long> durationCallback) {
        Objects.requireNonNull(requests, "requests cannot be null");
        Objects.requireNonNull(durationCallback, "durationCallback cannot be null");

        if (requests.isEmpty()) {
            return List.of();
        }
        return TimeUtils.measureDurationMsSupplier(time, () -> runOnce(requests, fetchMaxBytes, maxBatchesPerPartition), durationCallback);
    }

    private List<FindBatchResponse> runOnce(final List<FindBatchRequest> requests,
                                            final int fetchMaxBytes,
                                            final int maxBatchesPerPartition) {
        return requests.stream()
            .map(request -> findBatchesForExistingPartition(request, fetchMaxBytes))
            .toList();
    }

    private FindBatchResponse findBatchesForExistingPartition(
        final FindBatchRequest request,
        final int fetchMaxBytes
    ) {
        final GetLogInfoResponse logInfo = getLogInfoJob.call(
            List.of(new GetLogInfoRequest(request.topicIdPartition().topicId(), request.topicIdPartition().partition())), d -> {}).get(0);
        if (logInfo.errors() == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            LOGGER.warn("Unexpected non-existing partition {}", request.topicIdPartition());
            return FindBatchResponse.unknownTopicOrPartition();
        }

        if (request.offset() < 0) {
            LOGGER.debug("Invalid offset {} for {}", request.offset(), request.topicIdPartition());
            return FindBatchResponse.offsetOutOfRange(logInfo.logStartOffset(), logInfo.highWatermark());
        }

        // if offset requests is > end offset return out-of-range exception, otherwise return empty batch.
        // Similar to {@link LocalLog#read() L490}
        if (request.offset() > logInfo.highWatermark()) {
            return FindBatchResponse.offsetOutOfRange(logInfo.logStartOffset(), logInfo.highWatermark());
        }

        List<BatchInfo> batches = new ArrayList<>();
        long totalSize = 0;

        try {
            getBatchesPreparedStatement.clearParameters();
            getBatchesPreparedStatement.setString(1, request.topicIdPartition().topicId().toString());
            getBatchesPreparedStatement.setInt(2, request.topicIdPartition().partition());
            getBatchesPreparedStatement.setLong(3, request.offset());
            try (final ResultSet resultSet = getBatchesPreparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    final BatchInfo batch = new BatchInfo(
                        resultSet.getLong("batch_id"),
                        resultSet.getString("object_key"),
                        new BatchMetadata(
                            resultSet.getByte("magic"),
                            request.topicIdPartition(),
                            resultSet.getLong("byte_offset"),
                            resultSet.getLong("byte_size"),
                            resultSet.getLong("base_offset"),
                            resultSet.getLong("last_offset"),
                            resultSet.getLong("log_append_timestamp"),
                            resultSet.getLong("batch_max_timestamp"),
                            shortToTimestampTypeConverter.from(resultSet.getShort("timestamp_type"))
                        )
                    );
                    batches.add(batch);
                    totalSize += batch.metadata().byteSize();
                    if (totalSize > fetchMaxBytes) {
                        break;
                    }
                }
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }

        return FindBatchResponse.success(batches, logInfo.logStartOffset(), logInfo.highWatermark());
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(getBatchesPreparedStatement, "getBatchesPreparedStatement");
    }
}
