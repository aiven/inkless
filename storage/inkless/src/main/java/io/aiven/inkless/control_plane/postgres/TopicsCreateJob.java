// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.TopicDelta;

import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import io.aiven.inkless.common.UuidUtil;

public class TopicsCreateJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicsCreateJob.class);

    private static final String INSERT_LOG_ROW_QUERY = """
        INSERT INTO logs (topic_id, partition, topic_name, log_start_offset, high_watermark)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT DO NOTHING
        """;

    private final Time time;
    private final HikariDataSource hikariDataSource;
    private final Map<Uuid, TopicDelta> changedTopics;

    TopicsCreateJob(final Time time,
                    final HikariDataSource hikariDataSource,
                    final Map<Uuid, TopicDelta> changedTopics) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
        this.changedTopics = changedTopics;
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (runOnce()) {
                    return;
                } else {
                    // TODO configurable backoff
                    time.sleep(1000);
                }
            } catch (final Exception e) {
                LOGGER.error("Unexpected exception, exiting", e);
            }
        }
    }

    private boolean runOnce() {
        final Connection connection;
        try {
            connection = hikariDataSource.getConnection();
        } catch (final SQLException e) {
            LOGGER.error("Cannot get Postgres connection", e);
            return false;
        }

        try (connection) {
            runWithConnection(connection);
            return true;
        } catch (final Exception e) {
            LOGGER.error("Error executing query", e);
            try {
                connection.rollback();
            } catch (final SQLException ex) {
                LOGGER.error("Error rolling back transaction", e);
            }
            return false;
        }
    }

    private void runWithConnection(final Connection connection) throws SQLException {
        try (final PreparedStatement preparedStatement = connection.prepareStatement(INSERT_LOG_ROW_QUERY)) {
            for (final var topicEntry : changedTopics.entrySet()) {
                for (final var partitionEntry : topicEntry.getValue().newPartitions().entrySet()) {
                    preparedStatement.setObject(1, UuidUtil.toJava(topicEntry.getValue().id()));
                    preparedStatement.setInt(2, partitionEntry.getKey());
                    preparedStatement.setString(3, topicEntry.getValue().name());
                    // log_start_offset
                    preparedStatement.setLong(4, 0);
                    // high_watermark
                    preparedStatement.setLong(5, 0);
                    preparedStatement.addBatch();
                }
            }
            final int[] batchResults = preparedStatement.executeBatch();
            // This is not expected to happen, but checking just in case.
            if (Arrays.stream(batchResults).asLongStream().anyMatch(l -> l != 0 && l != 1)) {
                throw new RuntimeException("Unexpected executeBatch result");
            }

            connection.commit();
        }
    }
}
