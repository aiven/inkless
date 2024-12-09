// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
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
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;

import io.aiven.inkless.common.UuidUtil;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;

class CommitFileJob implements Callable<List<CommitBatchResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitFileJob.class);

    private static final String UPDATE_LOG_HIGH_WATERMARK_QUERY = """
        UPDATE logs
        SET high_watermark = high_watermark + ?
        WHERE topic_id = ?
            AND partition = ?
        RETURNING topic_id, partition, topic_name, log_start_offset, high_watermark
        """;
    private static final String INSERT_BATCH_QUERY = """
        INSERT INTO batches (
            topic_id, partition, base_offset, last_offset, object_key,
            byte_offset, byte_size, number_of_records, timestamp_type, batch_timestamp
        )
        VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?
        )
        """;

    private final Time time;
    private final HikariDataSource hikariDataSource;
    private final String objectKey;
    private final List<CommitBatchRequestExtra> requests;

    CommitFileJob(final Time time,
                  final HikariDataSource hikariDataSource,
                  final String objectKey,
                  final List<CommitBatchRequestExtra> requests) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
        this.objectKey = objectKey;
        this.requests = requests;
    }

    @Override
    public List<CommitBatchResponse> call() {
        // TODO add retry
        try {
            return runOnce();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private List<CommitBatchResponse> runOnce() throws SQLException {
        final Connection connection;
        try {
            connection = hikariDataSource.getConnection();
        } catch (final SQLException e) {
            LOGGER.error("Cannot get Postgres connection", e);
            throw e;
        }

        final long now = time.milliseconds();
        try (connection) {
            final var updateHighWatermarkResults = updateHighWatermarks(connection);
            final var result = insertBatches(connection, updateHighWatermarkResults, now);
            connection.commit();
            return result;
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

    private List<Optional<UpdateHighWatermarkResult>> updateHighWatermarks(final Connection connection) throws SQLException {
        if (requests.isEmpty()) {
            return List.of();
        }

        // Execute this only to lock the rows.
        final List<TopicIdPartition> tidps = requests.stream()
            .map(CommitBatchRequestExtra::topicIdPartition)
            .toList();
        LogSelectQuery.execute(connection, tidps, true);

        final List<Optional<UpdateHighWatermarkResult>> result = new ArrayList<>();
        // TODO reduce the number of round-trips
        // This piece does a DB round-trip per topic-partitions. We should do 1 round trip instead.
        // The problem is that JDBC doesn't support returning result on batch execution.
        try (final PreparedStatement preparedStatement = connection.prepareStatement(UPDATE_LOG_HIGH_WATERMARK_QUERY)) {
            for (final CommitBatchRequestExtra request : requests) {
                preparedStatement.clearParameters();

                final Uuid topicId = request.topicId();
                final int partition = request.request().topicPartition().partition();

                preparedStatement.setLong(1, request.request().numberOfRecords());
                preparedStatement.setObject(2, UuidUtil.toJava(topicId));
                preparedStatement.setInt(3, partition);
                try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                    // This is an unexpected error, probably of the concurrency nature.
                    // It means, the row in `logs` doesn't exist for this topic-partition.
                    // We report the partition as non-existent in this case.
                    if (!resultSet.next()) {
                        LOGGER.error("Record not returned after UPDATING 'logs' for topic {} partition {}",
                            topicId, partition);
                        result.add(Optional.empty());
                    } else {
                        result.add(Optional.of(UpdateHighWatermarkResult.fromResultSet(resultSet)));
                    }
                }
            }
        }
        return result;
    }

    private List<CommitBatchResponse> insertBatches(
        final Connection connection,
        final List<Optional<UpdateHighWatermarkResult>> updateHighWatermarkResults,
        final long now
    ) throws SQLException {
        final List<CommitBatchResponse> responses = new ArrayList<>();

        try (final PreparedStatement preparedStatement = connection.prepareStatement(INSERT_BATCH_QUERY)) {
            for (int i = 0; i < requests.size(); i++) {
                if (updateHighWatermarkResults.get(i).isEmpty()) {
                    responses.add(CommitBatchResponse.unknownTopicOrPartition());
                    continue;
                }

                final var request = requests.get(i).request();
                final var updateHighWatermarkResult = updateHighWatermarkResults.get(i).get();
                preparedStatement.setObject(1, updateHighWatermarkResult.topicId());
                preparedStatement.setInt(2, updateHighWatermarkResult.partition());
                // We know that we increased the HWM by the number of records. Hence, to get the base offset we need to subtract.
                final long baseOffset = updateHighWatermarkResult.highWatermark() - request.numberOfRecords();
                preparedStatement.setLong(3, baseOffset);
                final long lastOffset = updateHighWatermarkResult.highWatermark() - 1;
                preparedStatement.setLong(4, lastOffset);
                preparedStatement.setString(5, objectKey);
                preparedStatement.setLong(6, request.byteOffset());
                preparedStatement.setLong(7, request.size());
                preparedStatement.setLong(8, request.numberOfRecords());
                preparedStatement.setShort(9, (short) requests.get(i).timestampType().id);
                preparedStatement.setLong(10, now);
                preparedStatement.addBatch();

                responses.add(CommitBatchResponse.success(baseOffset, now, updateHighWatermarkResult.logStartOffset()));
            }

            final int[] batchResults = preparedStatement.executeBatch();
            // This is not expected to happen, but checking just in case.
            if (Arrays.stream(batchResults).asLongStream().anyMatch(l -> l != 1)) {
                throw new RuntimeException("Unexpected executeBatch result");
            }
        }

        return responses;
    }

    record CommitBatchRequestExtra(CommitBatchRequest request,
                                   Uuid topicId,
                                   TimestampType timestampType) {
        TopicIdPartition topicIdPartition() {
            return new TopicIdPartition(topicId(), request().topicPartition());
        }
    }

    private record UpdateHighWatermarkResult(UUID topicId,
                                             int partition,
                                             String topicName,
                                             long logStartOffset,
                                             long highWatermark) {
        static UpdateHighWatermarkResult fromResultSet(final ResultSet resultSet) throws SQLException {
            return new UpdateHighWatermarkResult(
                resultSet.getObject("topic_id", UUID.class),
                resultSet.getInt("partition"),
                resultSet.getString("topic_name"),
                resultSet.getLong("log_start_offset"),
                resultSet.getLong("high_watermark")
            );
        }
    }
}
