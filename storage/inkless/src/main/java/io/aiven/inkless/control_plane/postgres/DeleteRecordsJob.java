// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariDataSource;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.generated.udt.CommitBatchResponseV1;
import org.jooq.generated.udt.DeleteRecordsResponseV1;
import org.jooq.generated.udt.records.DeleteRecordsRequestV1Record;
import org.jooq.generated.udt.records.DeleteRecordsResponseV1Record;
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

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;

import static org.jooq.generated.Tables.DELETE_RECORDS_V1;

public class DeleteRecordsJob implements Callable<List<DeleteRecordsResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteRecordsJob.class);

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
        final DSLContext ctx = DSL.using(connection, SQLDialect.POSTGRES);

        final Instant now = TimeUtils.now(time);
        final DeleteRecordsRequestV1Record[] jooqRequests = requests.stream().map(r ->
                new DeleteRecordsRequestV1Record(
                    r.topicIdPartition().topicId(),
                    r.topicIdPartition().partition(),
                    r.offset()))
            .toArray(DeleteRecordsRequestV1Record[]::new);

        final List<DeleteRecordsResponseV1Record> functionResult = ctx.select(
                DeleteRecordsResponseV1.TOPIC_ID,
                DeleteRecordsResponseV1.PARTITION,
                DeleteRecordsResponseV1.ERROR,
                DeleteRecordsResponseV1.LOG_START_OFFSET
            ).from(DELETE_RECORDS_V1.call(now, jooqRequests))
            .fetchInto(DeleteRecordsResponseV1Record.class);
        return processFunctionResult(functionResult);
    }

    private List<DeleteRecordsResponse> processFunctionResult(List<DeleteRecordsResponseV1Record> functionResult) throws SQLException {
        final List<DeleteRecordsResponse> responses = new ArrayList<>();

        final Iterator<DeleteRecordsRequest> iterator = requests.iterator();
        for (final var record : functionResult) {
            if (!iterator.hasNext()) {
                throw new RuntimeException("More records returned than expected");
            }
            final DeleteRecordsRequest request = iterator.next();

            final Uuid requestTopicId = request.topicIdPartition().topicId();
            final int requestPartition = request.topicIdPartition().partition();
            final Uuid resultTopicId = record.getTopicId();
            final int resultPartition = record.get(CommitBatchResponseV1.PARTITION);
            if (!resultTopicId.equals(requestTopicId) || resultPartition != requestPartition) {
                throw new RuntimeException(String.format(
                    "Returned topic ID or partition doesn't match: expected %s-%d, got %s-%d",
                    requestTopicId, requestPartition,
                    resultTopicId, resultPartition
                ));
            }

            if (record.getError() == null) {
                responses.add(DeleteRecordsResponse.success(record.getLogStartOffset()));
            } else {
                final var response = switch (record.getError()) {
                    case unknown_topic_or_partition ->
                        DeleteRecordsResponse.unknownTopicOrPartition();
                    case offset_out_of_range ->
                        DeleteRecordsResponse.offsetOutOfRange();
                    default ->
                        throw new RuntimeException(String.format("Unknown error '%s' returned for %s-%d",
                            record.getError(), resultTopicId, resultPartition));
                };
                responses.add(response);
            }
        }

        if (iterator.hasNext()) {
            throw new RuntimeException("Fewer records returned than expected");
        }

        return responses;
    }
}
