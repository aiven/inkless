// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
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

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.UuidUtil;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;

public class DeleteRecordsJob implements Callable<List<DeleteRecordsResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteRecordsJob.class);

    private static final ObjectMapper MAPPER = JsonMapper.builder()
        .disable(
            MapperFeature.AUTO_DETECT_CREATORS,
            MapperFeature.AUTO_DETECT_FIELDS,
            MapperFeature.AUTO_DETECT_GETTERS,
            MapperFeature.AUTO_DETECT_IS_GETTERS)
        .build();

    private static final String CALL_DELETE_FUNCTION = """
        SELECT * FROM delete_records_v1(?, ?::JSONB)
        """;

    private final Time time;
    private final HikariDataSource hikariDataSource;
    private final List<DeleteRecordsRequest> requests;

    public DeleteRecordsJob(final Time time, final HikariDataSource hikariDataSource, final List<DeleteRecordsRequest> requests) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
        this.requests = requests;
    }

    @Override
    public List<DeleteRecordsResponse> call() {
        if (requests.isEmpty()) {
            return List.of();
        }

        // TODO add retry (or not, let the consumers do this?)
        try {
            return runOnce();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<DeleteRecordsResponse> runOnce() throws SQLException {
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

    private List<DeleteRecordsResponse> runWithConnection(final Connection connection) throws SQLException {
        final Instant now = TimeUtils.now(time);
        try (final PreparedStatement preparedStatement = connection.prepareStatement(CALL_DELETE_FUNCTION)) {
            preparedStatement.setTimestamp(1, java.sql.Timestamp.from(now));
            preparedStatement.setString(2, requestsAsJsonString());
            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                return processFunctionResult(resultSet);
            }
        }
    }

    private List<DeleteRecordsResponse> processFunctionResult(final ResultSet resultSet) throws SQLException {
        final List<DeleteRecordsResponse> responses = new ArrayList<>();

        final Iterator<DeleteRecordsRequest> iterator = requests.iterator();
        while (resultSet.next()) {
            if (!iterator.hasNext()) {
                throw new RuntimeException("More records returned than expected");
            }
            final DeleteRecordsRequest request = iterator.next();

            final Uuid requestTopicId = request.topicIdPartition().topicId();
            final int requestPartition = request.topicIdPartition().partition();
            final Uuid resultTopicId = UuidUtil.fromJava(resultSet.getObject("topic_id", UUID.class));
            final int resultPartition = resultSet.getInt("partition");
            if (!resultTopicId.equals(requestTopicId) || resultPartition != requestPartition) {
                throw new RuntimeException(String.format(
                    "Returned topic ID or partition doesn't match: expected %s-%d, got %s-%d",
                    requestTopicId, requestPartition,
                    resultTopicId, resultPartition
                ));
            }

            final String error = resultSet.getString("error");
            if (error == null) {
                final long logStartOffset = resultSet.getLong("log_start_offset");
                responses.add(DeleteRecordsResponse.success(logStartOffset));
            } else if (error.equals("unknown_topic_or_partition")) {
                responses.add(DeleteRecordsResponse.unknownTopicOrPartition());
            } else if (error.equals("offset_out_of_range")) {
                responses.add(DeleteRecordsResponse.offsetOutOfRange());
            } else {
                throw new RuntimeException(String.format("Unknown error '%s' returned for %s-%d",
                    error, resultTopicId, resultPartition));
            }
        }

        if (iterator.hasNext()) {
            throw new RuntimeException("Fewer records returned than expected");
        }

        return responses;
    }

    private String requestsAsJsonString() {
        try {
            return MAPPER.writeValueAsString(requests.stream().map(DeleteRecordsRequestJson::new).toList());
        } catch (final JsonProcessingException e) {
            // We validate our JSONs in tests, so this should never happen.
            throw new RuntimeException(e);
        }
    }

    private record DeleteRecordsRequestJson(DeleteRecordsRequest request) {
        @JsonProperty("topic_id")
        UUID topicId() {
            return UuidUtil.toJava(request().topicIdPartition().topicId());
        }

        @JsonProperty("partition")
        int partition() {
            return request().topicIdPartition().partition();
        }

        @JsonProperty("offset")
        long offset() {
            return request().offset();
        }
    }
}
