// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.aiven.inkless.common.UuidUtil;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

class FindBatchesJob implements Callable<List<FindBatchResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindBatchesJob.class);

    private static final String SELECT_BATCHES = """
        SELECT b.base_offset, b.last_offset, f.object_key, b.byte_offset,
            b.byte_size, b.number_of_records,
            b.timestamp_type, b.log_append_timestamp, b.batch_max_timestamp
        FROM batches AS b
            INNER JOIN files AS f ON b.file_id = f.file_id
        WHERE topic_id = ?
            AND partition = ?
            AND last_offset >= ?  -- offset to find
            AND last_offset < ?   -- high watermark
            AND base_offset >= ?  -- LSO
        ORDER BY base_offset
        """;

    private final Time time;
    private final HikariDataSource hikariDataSource;
    private final List<FindBatchRequest> requests;
    private final boolean minOneMessage;
    private final int fetchMaxBytes;

    FindBatchesJob(final Time time,
                   final HikariDataSource hikariDataSource,
                   final List<FindBatchRequest> requests,
                   final boolean minOneMessage,
                   final int fetchMaxBytes) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
        this.requests = requests;
        this.minOneMessage = minOneMessage;
        this.fetchMaxBytes = fetchMaxBytes;
    }

    @Override
    public List<FindBatchResponse> call() {
        // TODO add retry (or not, let the consumers do this?)
        try {
            return runOnce();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<FindBatchResponse> runOnce() throws SQLException {
        final Connection connection;
        try {
            connection = hikariDataSource.getConnection();
            // Mind this read-only setting.
            connection.setReadOnly(true);
        } catch (final SQLException e) {
            LOGGER.error("Cannot get Postgres connection", e);
            throw e;
        }

        // No need to explicitly commit or rollback.
        try (connection) {
            return runWithConnection(connection);
        } catch (final Exception e) {
            LOGGER.error("Error executing query", e);
            throw e;
        }
    }

    private List<FindBatchResponse> runWithConnection(final Connection connection) throws SQLException {
        final Map<TopicIdPartition, LogEntity> logInfos = getLogInfos(connection);
        final List<FindBatchResponse> result = new ArrayList<>();
        for (final FindBatchRequest request : requests) {
            result.add(
                findBatchPerPartition(connection, request, logInfos.get(request.topicIdPartition()))
            );
        }
        return result;
    }

    private FindBatchResponse findBatchPerPartition(final Connection connection,
                                                    final FindBatchRequest request,
                                                    final LogEntity logEntity) throws SQLException {
        if (logEntity == null) {
            return FindBatchResponse.unknownTopicOrPartition();
        }

        if (request.offset() < logEntity.logStartOffset()) {
            LOGGER.debug("Invalid offset {} for {}", request.offset(), request.topicIdPartition());
            return FindBatchResponse.offsetOutOfRange(logEntity.logStartOffset(), logEntity.highWatermark());
        }

        if (request.offset() > logEntity.highWatermark()) {
            return FindBatchResponse.offsetOutOfRange(logEntity.logStartOffset(), logEntity.highWatermark());
        }

        final List<BatchInfo> batches = new ArrayList<>();
        long totalSize = 0;

        try (final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_BATCHES)) {
            preparedStatement.setObject(1, UuidUtil.toJava(request.topicIdPartition().topicId()));
            preparedStatement.setInt(2, request.topicIdPartition().partition());
            preparedStatement.setLong(3, request.offset());
            preparedStatement.setLong(4, logEntity.highWatermark());
            preparedStatement.setLong(5, logEntity.logStartOffset());

            preparedStatement.setFetchSize(1000);  // fetch lazily

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    final BatchInfo batch = new BatchInfo(
                        resultSet.getString("object_key"),
                        resultSet.getLong("byte_offset"),
                        resultSet.getLong("byte_size"),
                        resultSet.getLong("base_offset"),
                        resultSet.getLong("number_of_records"),
                        timestampTypeFromId(resultSet.getShort("timestamp_type")),
                        resultSet.getLong("log_append_timestamp"),
                        resultSet.getLong("batch_max_timestamp")
                    );
                    batches.add(batch);
                    totalSize += batch.size();
                    if (totalSize > fetchMaxBytes) {
                        break;
                    }
                }
            }
        }

        return FindBatchResponse.success(batches, logEntity.logStartOffset(), logEntity.highWatermark());
    }

    private Map<TopicIdPartition, LogEntity> getLogInfos(final Connection connection) throws SQLException {
        if (requests.isEmpty()) {
            return Map.of();
        }

        final List<TopicIdPartition> tidps = requests.stream()
            .map(FindBatchRequest::topicIdPartition)
            .toList();
        return LogSelectQuery.execute(connection, tidps, false).stream()
            .collect(Collectors.toMap(LogEntity::topicIdPartition, Function.identity()));
    }

    private TimestampType timestampTypeFromId(short id) {
        return switch (id) {
            case -1 -> TimestampType.NO_TIMESTAMP_TYPE;
            case 0 -> TimestampType.CREATE_TIME;
            case 1 -> TimestampType.LOG_APPEND_TIME;
            default -> throw new IllegalStateException("Unexpected value: " + id);
        };
    }
}
