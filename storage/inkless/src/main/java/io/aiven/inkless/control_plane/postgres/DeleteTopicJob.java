// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariDataSource;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.generated.Routines;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.UuidUtil;

class DeleteTopicJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTopicJob.class);

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

    private void callDeleteFunction(final Connection connection) {
        final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
        final Instant now = TimeUtils.now(time);
        final UUID[] topicIds = this.topicIds.stream().map(UuidUtil::toJava).toArray(UUID[]::new);
        Routines.deleteTopicV1(ctx.configuration(), now, topicIds);
    }
}
