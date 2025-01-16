// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariDataSource;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
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

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

import static org.jooq.generated.Tables.BATCHES;
import static org.jooq.generated.Tables.FILES;

class FindBatchesJob implements Callable<List<FindBatchResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindBatchesJob.class);

    private final Time time;
    private final HikariDataSource hikariDataSource;
    private final List<FindBatchRequest> requests;
    private final boolean minOneMessage;
    private final int fetchMaxBytes;
    private final Consumer<Long> durationCallback;
    private final Consumer<Long> getLogsDurationCallback;

    FindBatchesJob(final Time time,
                   final HikariDataSource hikariDataSource,
                   final List<FindBatchRequest> requests,
                   final boolean minOneMessage,
                   final int fetchMaxBytes,
                   final Consumer<Long> durationCallback,
                   final Consumer<Long> getLogsDurationCallback) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
        this.requests = requests;
        this.minOneMessage = minOneMessage;
        this.fetchMaxBytes = fetchMaxBytes;
        this.durationCallback = durationCallback;
        this.getLogsDurationCallback = getLogsDurationCallback;
    }

    @Override
    public List<FindBatchResponse> call() {
        // TODO add retry (or not, let the consumers do this?)
        try {
            return runOnce();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<FindBatchResponse> runOnce() throws Exception {
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

    private List<FindBatchResponse> runWithConnection(final Connection connection) throws Exception {
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
                                                    final LogEntity logEntity) throws Exception {
        if (logEntity == null) {
            return FindBatchResponse.unknownTopicOrPartition();
        }

        if (request.offset() < 0) {
            LOGGER.debug("Invalid offset {} for {}", request.offset(), request.topicIdPartition());
            return FindBatchResponse.offsetOutOfRange(logEntity.logStartOffset(), logEntity.highWatermark());
        }

        if (request.offset() > logEntity.highWatermark()) {
            return FindBatchResponse.offsetOutOfRange(logEntity.logStartOffset(), logEntity.highWatermark());
        }

        final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
        return TimeUtils.measureDurationMs(time, () -> getBatchResponse(ctx, request, logEntity), durationCallback);
    }

    private FindBatchResponse getBatchResponse(final DSLContext ctx, final FindBatchRequest request, final LogEntity logEntity) throws SQLException {
        final var select = ctx.select(
                BATCHES.BASE_OFFSET,
                BATCHES.LAST_OFFSET,
                FILES.OBJECT_KEY,
                BATCHES.BYTE_OFFSET,
                BATCHES.BYTE_SIZE,
                BATCHES.REQUEST_BASE_OFFSET,
                BATCHES.REQUEST_LAST_OFFSET,
                BATCHES.TIMESTAMP_TYPE,
                BATCHES.LOG_APPEND_TIMESTAMP,
                BATCHES.BATCH_MAX_TIMESTAMP
            ).from(BATCHES)
            .innerJoin(FILES).on(BATCHES.FILE_ID.eq(FILES.FILE_ID))
            .where(BATCHES.TOPIC_ID.eq(request.topicIdPartition().topicId()))
            .and(BATCHES.PARTITION.eq(request.topicIdPartition().partition()))
            .and(BATCHES.LAST_OFFSET.ge(request.offset()))  // offset to find
            .and(BATCHES.LAST_OFFSET.lt(logEntity.highWatermark()))
            .orderBy(BATCHES.BASE_OFFSET);

        final List<BatchInfo> batches = new ArrayList<>();
        long totalSize = 0;
        try (final var cursor = select.fetchSize(1000).fetchLazy()) {
            for (final var record : cursor) {
                final BatchInfo batch = new BatchInfo(
                    record.get(FILES.OBJECT_KEY),
                    record.get(BATCHES.BYTE_OFFSET),
                    record.get(BATCHES.BYTE_SIZE),
                    record.get(BATCHES.BASE_OFFSET),
                    record.get(BATCHES.REQUEST_BASE_OFFSET),
                    record.get(BATCHES.REQUEST_LAST_OFFSET),
                    record.get(BATCHES.LOG_APPEND_TIMESTAMP),
                    record.get(BATCHES.BATCH_MAX_TIMESTAMP),
                    record.get(BATCHES.TIMESTAMP_TYPE)
                );
                batches.add(batch);
                totalSize += batch.size();
                if (totalSize > fetchMaxBytes) {
                    break;
                }

            }
        }
        return FindBatchResponse.success(batches, logEntity.logStartOffset(), logEntity.highWatermark());
    }

    private Map<TopicIdPartition, LogEntity> getLogInfos(final Connection connection) throws Exception {
        if (requests.isEmpty()) {
            return Map.of();
        }

        final List<TopicIdPartition> tidps = requests.stream()
            .map(FindBatchRequest::topicIdPartition)
            .toList();
        return LogSelectQuery.execute(time, connection, tidps, false, getLogsDurationCallback).stream()
            .collect(Collectors.toMap(LogEntity::topicIdPartition, Function.identity()));
    }
}
