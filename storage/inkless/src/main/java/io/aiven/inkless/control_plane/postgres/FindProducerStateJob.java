// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.BatchMetadata;

import com.zaxxer.hikari.HikariDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.UuidUtil;
import io.aiven.inkless.control_plane.FindProducerStateRequest;
import io.aiven.inkless.control_plane.FindProducerStateResponse;

/**
 * Find batch metadata from latest batches for a producer id-epoch pair, up to 5 batches.
 */
public class FindProducerStateJob implements Callable<List<FindProducerStateResponse>> {
    static final Logger LOGGER = LoggerFactory.getLogger(FindProducerStateJob.class);

    private static final String SELECT_BATCHES = """
        SELECT request_base_offset, request_last_offset, batch_max_timestamp, last_sequence
        FROM batches
        WHERE topic_id = ?
            AND partition = ?
            AND producer_id = ?
            AND producer_epoch = ?
            AND batch_max_timestamp >= ?
        ORDER BY base_offset DESC
        LIMIT 5
        """;

    final Time time;
    final HikariDataSource hikariDataSource;
    final List<FindProducerStateRequest> requests;
    final Instant minTimestamp;
    final Consumer<Long> durationCallback;

    FindProducerStateJob(final Time time,
                         final HikariDataSource hikariDataSource,
                         final List<FindProducerStateRequest> requests,
                         final Instant minTimestamp,
                         final Consumer<Long> durationCallback) {
        this.time = time;
        this.hikariDataSource = hikariDataSource;
        this.requests = requests;
        this.minTimestamp = minTimestamp;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<FindProducerStateResponse> call() {
        // TODO add retry (or not, let the consumers do this?)
        try {
            return runOnce();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<FindProducerStateResponse> runOnce() throws Exception {
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
            return TimeUtils.measureDurationMs(time, () -> runWithConnection(connection), durationCallback);
        } catch (final Exception e) {
            LOGGER.error("Error executing query", e);
            throw e;
        }
    }

    private List<FindProducerStateResponse> runWithConnection(final Connection connection) throws SQLException {
        final List<FindProducerStateResponse> result = new ArrayList<>();
        for (final var request : requests) {
            result.add(
                findProducerStatePerPartition(connection, request)
            );
        }
        return result;
    }

    private FindProducerStateResponse findProducerStatePerPartition(Connection connection, FindProducerStateRequest request) throws SQLException {
        final List<ProducerStateBatchMetadata> batches = new ArrayList<>();
        try (final PreparedStatement preparedStatement = connection.prepareStatement(SELECT_BATCHES)) {
            preparedStatement.setObject(1, UuidUtil.toJava(request.topicIdPartition().topicId()));
            preparedStatement.setInt(2, request.topicIdPartition().partition());
            preparedStatement.setLong(3, request.producerId());
            preparedStatement.setShort(4, request.producerEpoch());
            preparedStatement.setLong(5, minTimestamp.toEpochMilli());

            try (final ResultSet resultSet = preparedStatement.executeQuery()) {
                while (resultSet.next()) {
                    final var batch = new ProducerStateBatchMetadata(
                        resultSet.getLong("request_base_offset"),
                        resultSet.getLong("request_last_offset"),
                        resultSet.getLong("batch_max_timestamp"),
                        resultSet.getInt("last_sequence")
                    );
                    batches.add(batch);
                }
            }
        }

        final LinkedList<BatchMetadata> entries = new LinkedList<>();
        for (final var batch : batches) {
            entries.addFirst(batch.toBatchMetadata());
        }
        return FindProducerStateResponse.of(request, entries);
    }

    private record ProducerStateBatchMetadata(long requestBaseOffset, long requestLastOffset, long maxTimestamp, int lastSequence) {
        BatchMetadata toBatchMetadata() {
            final int offsetDelta = (int) (requestLastOffset - requestBaseOffset);
            return new BatchMetadata(lastSequence, requestLastOffset, offsetDelta, maxTimestamp);
        }
    }
}
