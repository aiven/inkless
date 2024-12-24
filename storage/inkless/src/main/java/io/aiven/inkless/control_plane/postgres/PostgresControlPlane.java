// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.IsolationLevel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import io.aiven.inkless.control_plane.AbstractControlPlane;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.control_plane.FindProducerStateRequest;
import io.aiven.inkless.control_plane.FindProducerStateResponse;

public class PostgresControlPlane extends AbstractControlPlane {
    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresControlPlane.class);

    private final PostgresControlPlaneMetrics metrics;

    private HikariDataSource hikariDataSource;

    public PostgresControlPlane(final Time time) {
        super(time);
        this.metrics = new PostgresControlPlaneMetrics(time);
    }

    @Override
    public void createTopicAndPartitions(final Set<CreateTopicAndPartitionsRequest> requests) {
        // Expected to be performed synchronously
        new TopicsAndPartitionsCreateJob(time, hikariDataSource, requests, metrics::onTopicCreateCompleted).run();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        super.configure(configs);

        final PostgresControlPlaneConfig controlPlaneConfig = new PostgresControlPlaneConfig(configs);
        Migrations.migrate(controlPlaneConfig);

        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(controlPlaneConfig.connectionString());
        config.setUsername(controlPlaneConfig.username());
        config.setPassword(controlPlaneConfig.password());

        config.setTransactionIsolation(IsolationLevel.TRANSACTION_READ_COMMITTED.name());

        // We're doing interactive transactions.
        config.setAutoCommit(false);

        hikariDataSource = new HikariDataSource(config);
    }

    @Override
    protected Iterator<CommitBatchResponse> commitFileForValidRequests(
        final String objectKey,
        final int uploaderBrokerId,
        final long fileSize,
        final Stream<CommitBatchRequest> requests) {
        final var requestsJson = requests.map(CommitFileJob.CommitBatchRequestJson::new).toList();
        final CommitFileJob job = new CommitFileJob(
            time, hikariDataSource,
            objectKey, uploaderBrokerId, fileSize, requestsJson,
            metrics::onCommitFileCompleted);
        return job.call().iterator();
    }

    @Override
    protected Iterator<FindBatchResponse> findBatchesForExistingPartitions(
        final Stream<FindBatchRequest> requests,
        final boolean minOneMessage, final int fetchMaxBytes) {
        final FindBatchesJob job = new FindBatchesJob(
            time, hikariDataSource,
            requests.toList(), minOneMessage, fetchMaxBytes,
            metrics::onFindBatchesCompleted, metrics::onGetLogsCompleted);
        return job.call().iterator();
    }

    @Override
    public void deleteTopics(final Set<Uuid> topicIds) {
        final DeleteTopicJob job = new DeleteTopicJob(time, hikariDataSource, topicIds, metrics::onTopicDeleteCompleted);
        job.run();
    }

    @Override
    public List<FileToDelete> getFilesToDelete() {
        final FindFilesToDeleteJob job = new FindFilesToDeleteJob(time, hikariDataSource);
        return job.call();
    }

    public List<FindProducerStateResponse> findProducerStates(
        final List<FindProducerStateRequest> findProducerStateRequests
    ) {
        final FindProducerStateJob job = new FindProducerStateJob(
            time, hikariDataSource,
            findProducerStateRequests,
            producerStateCache.minimumTimestamp(),
            metrics::onFindProducerStateCompleted);
        return job.call();
    }

    private Connection readOnlyConnection() throws SQLException {
        final Connection connection;
        try {
            connection = hikariDataSource.getConnection();
            // Mind this read-only setting.
            connection.setReadOnly(true);
        } catch (final SQLException e) {
            LOGGER.error("Cannot get Postgres connection", e);
            throw e;
        }
        return connection;
    }

    @Override
    public long logStartOffset(TopicIdPartition topicIdPartition) {
        try {
            final Connection connection = readOnlyConnection();

            final var topicIdPartitions = List.of(topicIdPartition);
            final var logs = LogSelectQuery.execute(time, connection, topicIdPartitions, false, metrics::onGetLogsCompleted);
            return logs.stream().findAny().map(LogEntity::logStartOffset).orElse(-1L);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        hikariDataSource.close();
    }
}
