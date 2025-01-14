// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariDataSource;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.generated.udt.CommitBatchResponseV1;
import org.jooq.generated.udt.records.CommitBatchRequestV1Record;
import org.jooq.generated.udt.records.CommitBatchResponseV1Record;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;

import static org.jooq.generated.Tables.COMMIT_FILE_V1;

class CommitFileJob implements Callable<List<CommitBatchResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitFileJob.class);

    private final Time time;
    private final HikariDataSource hikariDataSource;
    private final String objectKey;
    private final int uploaderBrokerId;
    private final long fileSize;
    private final List<CommitBatchRequest> requests;
    private final Consumer<Long> durationCallback;

    CommitFileJob(final Time time,
                  final HikariDataSource hikariDataSource,
                  final String objectKey,
                  final int uploaderBrokerId,
                  final long fileSize,
                  final List<CommitBatchRequest> requests,
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
        final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);
        final Instant now = TimeUtils.now(time);

        final CommitBatchRequestV1Record[] jooqRequests = requests.stream().map(r ->
            new CommitBatchRequestV1Record(
                r.topicIdPartition().topicId(),
                r.topicIdPartition().partition(),
                (long) r.byteOffset(),
                (long) r.size(),
                r.baseOffset(),
                r.lastOffset(),
                r.messageTimestampType(),
                r.batchMaxTimestamp()
            )
        ).toArray(CommitBatchRequestV1Record[]::new);

        final List<CommitBatchResponseV1Record> functionResult = ctx.select(
            CommitBatchResponseV1.TOPIC_ID,
            CommitBatchResponseV1.PARTITION,
            CommitBatchResponseV1.LOG_EXISTS,
            CommitBatchResponseV1.ASSIGNED_OFFSET,
            CommitBatchResponseV1.LOG_START_OFFSET
        ).from(COMMIT_FILE_V1.call(
            objectKey,
            uploaderBrokerId,
            fileSize,
            now,
            jooqRequests
        )).fetchInto(CommitBatchResponseV1Record.class);
        return processFunctionResult(now, functionResult);
    }

    private List<CommitBatchResponse> processFunctionResult(final Instant now,
                                                            final List<CommitBatchResponseV1Record> functionResult) {
        final List<CommitBatchResponse> responses = new ArrayList<>();
        final Iterator<CommitBatchRequest> iterator = requests.iterator();
        for (final var record : functionResult) {
            if (!iterator.hasNext()) {
                throw new RuntimeException("More records returned than expected");
            }
            final CommitBatchRequest request = iterator.next();

            // Sanity check to match returned and requested partitions (they should go in order). Maybe excessive?
            final Uuid requestTopicId = request.topicIdPartition().topicId();
            final int requestPartition = request.topicIdPartition().partition();
            final Uuid resultTopicId = record.getTopicId();
            final int resultPartition = record.get(CommitBatchResponseV1.PARTITION);
            if (!resultTopicId.equals(requestTopicId) || resultPartition != requestPartition) {
                throw new RuntimeException(String.format(
                    "Returned topic ID or resultPartition doesn't match: expected %s-%d, got %s-%d",
                    requestTopicId, requestPartition,
                    resultTopicId, resultPartition
                ));
            }

            if (!record.get(CommitBatchResponseV1.LOG_EXISTS)) {
                responses.add(CommitBatchResponse.unknownTopicOrPartition());
            } else {
                final long assignedOffset = record.get(CommitBatchResponseV1.ASSIGNED_OFFSET);
                final long logStartOffset = record.get(CommitBatchResponseV1.LOG_START_OFFSET);
                responses.add(CommitBatchResponse.success(assignedOffset, now.toEpochMilli(), logStartOffset));
            }
        }

        if (iterator.hasNext()) {
            throw new RuntimeException("Fewer records returned than expected");
        }

        return responses;
    }
}
