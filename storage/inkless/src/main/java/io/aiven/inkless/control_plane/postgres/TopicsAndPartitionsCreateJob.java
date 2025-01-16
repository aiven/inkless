// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariDataSource;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;

import static org.jooq.generated.Tables.LOGS;

public class TopicsAndPartitionsCreateJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicsAndPartitionsCreateJob.class);

    private final Time time;
    private final HikariDataSource hikariDataSource;
    private final Set<CreateTopicAndPartitionsRequest> requests;
    private final Consumer<Long> durationCallback;

    TopicsAndPartitionsCreateJob(final Time time,
                                 final HikariDataSource hikariDataSource,
                                 final Set<CreateTopicAndPartitionsRequest> requests,
                                 final Consumer<Long> durationCallback) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
        this.requests = requests;
        this.durationCallback = durationCallback;
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
            TimeUtils.measureDurationMs(time, () -> {
                runWithConnection(connection);
                return null;
            }, durationCallback);
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
        // See how topics are created in ReplicationControlManager.createTopic.
        // It's ordered so that ConfigRecords go after TopicRecord but before PartitionRecord(s).
        // So it means we will see topic configs before any partition.

        final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
        var insertStep = ctx.insertInto(LOGS,
            LOGS.TOPIC_ID,
            LOGS.PARTITION,
            LOGS.TOPIC_NAME,
            LOGS.LOG_START_OFFSET,
            LOGS.HIGH_WATERMARK);
        for (final var request : requests) {
            for (int partition = 0; partition < request.numPartitions(); partition++) {
                insertStep = insertStep.values(request.topicId(), partition, request.topicName(), 0L, 0L);
            }
        }
        final int rowsInserted = insertStep.onConflictDoNothing().execute();

        // This is not expected to happen, but checking just in case.
        final int maxInserts = requests.stream().mapToInt(CreateTopicAndPartitionsRequest::numPartitions).sum();
        if (rowsInserted < 0 || rowsInserted > maxInserts) {
            throw new RuntimeException(
                String.format("Unexpected number of inserted rows: expected max %d, got %d", maxInserts, rowsInserted));
        }

        connection.commit();
    }
}
