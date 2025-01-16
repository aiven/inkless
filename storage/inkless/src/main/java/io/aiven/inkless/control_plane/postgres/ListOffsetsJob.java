// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;

public class ListOffsetsJob implements Callable<List<ListOffsetsResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListOffsetsJob.class);

    private final Time time;
    private final HikariDataSource hikariDataSource;
    private final List<ListOffsetsRequest> requests;
    private final Consumer<Long> getLogsDurationCallback;

    public ListOffsetsJob(Time time, HikariDataSource hikariDataSource, List<ListOffsetsRequest> requests, Consumer<Long> getLogsDurationCallback) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
        this.requests = requests;
        this.getLogsDurationCallback = getLogsDurationCallback;
    }

    @Override
    public List<ListOffsetsResponse> call() {
        // TODO add retry (or not, let the consumers do this?)
        try {
            return runOnce();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<ListOffsetsResponse> runOnce() throws Exception {
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

    private List<ListOffsetsResponse> runWithConnection(final Connection connection) throws Exception {
        final Map<TopicIdPartition, LogEntity> logInfos = getLogInfos(connection);
        final List<ListOffsetsResponse> result = new ArrayList<>();
        for (final ListOffsetsRequest request : requests) {
            result.add(listOffset(request, logInfos));
        }
        return result;
    }

    private ListOffsetsResponse listOffset(ListOffsetsRequest request, Map<TopicIdPartition, LogEntity> data) {
        LogEntity logInfo = data.get(request.topicIdPartition());

        if (logInfo == null) {
            LOGGER.warn("Unexpected non-existing partition {}", request.topicIdPartition());
            return ListOffsetsResponse.unknownTopicOrPartition(request.topicIdPartition());
        }

        long timestamp = request.timestamp();
        if (timestamp == ListOffsetsRequest.EARLIEST_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), timestamp, logInfo.logStartOffset());
        } else if (timestamp == ListOffsetsRequest.LATEST_TIMESTAMP) {
            return ListOffsetsResponse.success(request.topicIdPartition(), timestamp, logInfo.logStartOffset());
        }
        LOGGER.error("listOffset request for timestamp {} in {} unsupported", timestamp, request.topicIdPartition());
        return new ListOffsetsResponse(Errors.UNKNOWN_SERVER_ERROR, request.topicIdPartition(), -1, -1);
    }

    private Map<TopicIdPartition, LogEntity> getLogInfos(final Connection connection) throws Exception {
        if (requests.isEmpty()) {
            return Map.of();
        }

        final List<TopicIdPartition> tidps = requests.stream()
                .map(ListOffsetsRequest::topicIdPartition)
                .toList();
        return LogSelectQuery.execute(time, connection, tidps, false, getLogsDurationCallback).stream()
                .collect(Collectors.toMap(LogEntity::topicIdPartition, Function.identity()));
    }
}
