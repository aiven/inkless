// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.UuidUtil;

class DeleteTopicJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTopicJob.class);

    private static final ObjectMapper MAPPER = JsonMapper.builder()
        .disable(
            MapperFeature.AUTO_DETECT_CREATORS,
            MapperFeature.AUTO_DETECT_FIELDS,
            MapperFeature.AUTO_DETECT_GETTERS,
            MapperFeature.AUTO_DETECT_IS_GETTERS)
        .build();

    private static final String CALL_DELETE_FUNCTION = """
        SELECT delete_topic_v1(?, ?::JSONB)
        """;

    private final Time time;
    private final HikariDataSource hikariDataSource;
    private final Set<Uuid> topicIds;
    private final Consumer<Long> durationCallback;

    DeleteTopicJob(final Time time,
                   final HikariDataSource hikariDataSource,
                   final Set<Uuid> topicIds,
                   final Consumer<Long> durationCallback) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
        this.topicIds = topicIds;
        this.durationCallback = durationCallback;
    }

    @Override
    public void run() {
        if (topicIds.isEmpty()) {
            return;
        }

        // TODO add retry
        try {
            runOnce();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void runOnce() throws SQLException {
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
            callDeleteFunction(connection);
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

    private void callDeleteFunction(final Connection connection) throws SQLException {
        final Instant now = TimeUtils.now(time);
        try (final PreparedStatement preparedStatement = connection.prepareStatement(CALL_DELETE_FUNCTION)) {
            preparedStatement.setTimestamp(1, java.sql.Timestamp.from(now));
            preparedStatement.setString(2, requestsAsJsonString());
            preparedStatement.execute();
        }
    }

    private String requestsAsJsonString() {
        try {
            return MAPPER.writeValueAsString(topicIds.stream().map(UuidUtil::toJava).toList());
        } catch (final JsonProcessingException e) {
            // We validate our JSONs in tests, so this should never happen.
            throw new RuntimeException(e);
        }
    }
}
