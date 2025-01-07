// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.UuidUtil;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;

class CommitFileJob implements Callable<List<CommitBatchResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitFileJob.class);

    private static final ObjectMapper MAPPER = JsonMapper.builder()
        .disable(
            MapperFeature.AUTO_DETECT_CREATORS,
            MapperFeature.AUTO_DETECT_FIELDS,
            MapperFeature.AUTO_DETECT_GETTERS,
            MapperFeature.AUTO_DETECT_IS_GETTERS)
        .build();

    private static final String CALL_COMMIT_FUNCTION = """
        SELECT topic_id, partition, log_exists, assigned_offset, log_start_offset
        FROM commit_file_v1(?, ?, ?, ?, ?::JSONB)
        """;

    private final Time time;
    private final HikariDataSource hikariDataSource;
    private final String objectKey;
    private final int uploaderBrokerId;
    private final long fileSize;
    private final List<CommitBatchRequestExtra> requests;
    private final Consumer<Long> durationCallback;

    CommitFileJob(final Time time,
                  final HikariDataSource hikariDataSource,
                  final String objectKey,
                  final int uploaderBrokerId,
                  final long fileSize,
                  final List<CommitBatchRequestExtra> requests,
                  final Consumer<Long> durationCallback) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
        this.objectKey = objectKey;
        this.uploaderBrokerId = uploaderBrokerId;
        this.fileSize = fileSize;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<CommitBatchResponse> call() {
        if (requests.isEmpty()) {
            return List.of();
        }

        // TODO add retry
        try {
            return runOnce();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<CommitBatchResponse> runOnce() throws Exception {
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
            return TimeUtils.measureDurationMs(time, () -> callCommitFunction(connection), durationCallback);
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

    private List<CommitBatchResponse> callCommitFunction(final Connection connection) throws SQLException {
        final Instant now = TimeUtils.now(time);
        try (final PreparedStatement preparedStatement = connection.prepareStatement(CALL_COMMIT_FUNCTION)) {
            preparedStatement.setString(1, objectKey);
            preparedStatement.setInt(2, uploaderBrokerId);
            preparedStatement.setLong(3, fileSize);
            preparedStatement.setTimestamp(4, java.sql.Timestamp.from(now));
            preparedStatement.setString(5, requestsAsJsonString());

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                return processCommitFunctionResult(now.toEpochMilli(), resultSet);
            }
        }
    }

    private List<CommitBatchResponse> processCommitFunctionResult(final long now,
                                                                  final ResultSet resultSet) throws SQLException {
        final List<CommitBatchResponse> responses = new ArrayList<>();

        final Iterator<CommitBatchRequestExtra> iterator = requests.iterator();
        while (resultSet.next()) {
            if (!iterator.hasNext()) {
                throw new RuntimeException("More records returned than expected");
            }
            final CommitBatchRequestExtra request = iterator.next();

            final Uuid topicId = UuidUtil.fromJava(resultSet.getObject("topic_id", UUID.class));
            final int partition = resultSet.getInt("partition");
            if (!topicId.equals(request.topicId()) || partition != request.partition()) {
                throw new RuntimeException(String.format(
                    "Returned topic ID or partition doesn't match: expected %s-%d, got %s-%d",
                    request.topicId(), request.partition(),
                    topicId, partition
                ));
            }

            if (!resultSet.getBoolean("log_exists")) {
                responses.add(CommitBatchResponse.unknownTopicOrPartition());
            } else {
                final long assignedOffset = resultSet.getLong("assigned_offset");
                final long logStartOffset = resultSet.getLong("log_start_offset");
                responses.add(CommitBatchResponse.success(assignedOffset, now, logStartOffset));
            }
        }

        if (iterator.hasNext()) {
            throw new RuntimeException("Fewer records returned than expected");
        }

        return responses;
    }

    record CommitBatchRequestExtra(CommitBatchRequest request,
                                   Uuid topicId) {

        @JsonProperty("topic_id")
        UUID topicIdJson() {
            return UuidUtil.toJava(topicId());
        }

        @JsonProperty("partition")
        int partition() {
            return request().topicPartition().partition();
        }

        @JsonProperty("byte_offset")
        int byteOffset() {
            return request().byteOffset();
        }

        @JsonProperty("byte_size")
        int byteSize() {
            return request().size();
        }

        @JsonProperty("number_of_records")
        long numberOfRecords() {
            return request().numberOfRecords();
        }

        @JsonProperty("timestamp_type")
        short timestampTypeJson() {
            return (short) request().messageTimestampType().id;
        }

        @JsonProperty("batch_max_timestamp")
        long batchMaxTimestamp() {
            return request().batchMaxTimestamp();
        }

    }

    private String requestsAsJsonString() {
        try {
            return MAPPER.writeValueAsString(requests);
        } catch (final JsonProcessingException e) {
            // We validate our JSONs in tests, so this should never happen.
            throw new RuntimeException(e);
        }
    }
}
