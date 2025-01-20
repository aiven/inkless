// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.IsolationLevel;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import io.aiven.inkless.control_plane.AbstractControlPlane;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;

public class PostgresControlPlane extends AbstractControlPlane {

    private final PostgresControlPlaneMetrics metrics;

    private HikariDataSource hikariDataSource;
    private DSLContext jooqCtx;

    public PostgresControlPlane(final Time time) {
        super(time);
        this.metrics = new PostgresControlPlaneMetrics(time);
    }

    @Override
    public void createTopicAndPartitions(final Set<CreateTopicAndPartitionsRequest> requests) {
        // Expected to be performed synchronously
        new TopicsAndPartitionsCreateJob(time, jooqCtx, requests, metrics::onTopicCreateCompleted).run();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
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
        jooqCtx = DSL.using(hikariDataSource, SQLDialect.POSTGRES);
    }

    @Override
    protected Iterator<CommitBatchResponse> commitFileForExistingPartitions(
        final String objectKey,
        final int uploaderBrokerId,
        final long fileSize,
        final Stream<CommitBatchRequest> requests) {
        final CommitFileJob job = new CommitFileJob(
            time, jooqCtx,
            objectKey, uploaderBrokerId, fileSize, requests.toList(),
            metrics::onCommitFileCompleted);
        return job.call().iterator();
    }

    @Override
    protected Iterator<FindBatchResponse> findBatchesForExistingPartitions(
        final Stream<FindBatchRequest> requests,
        final boolean minOneMessage, final int fetchMaxBytes) {
        final FindBatchesJob job = new FindBatchesJob(
            time, jooqCtx,
            requests.toList(), minOneMessage, fetchMaxBytes,
            metrics::onFindBatchesCompleted, metrics::onGetLogsCompleted);
        return job.call().iterator();
    }

    @Override
    protected Iterator<ListOffsetsResponse> listOffsetsForExistingPartitions(Stream<ListOffsetsRequest> requests) {
            final ListOffsetsJob job = new ListOffsetsJob(
                    time, jooqCtx,
                    requests.toList(),
                    metrics::onGetLogsCompleted);
            return job.call().iterator();
    }

    @Override
    public void deleteTopics(final Set<Uuid> topicIds) {
        final DeleteTopicJob job = new DeleteTopicJob(time, jooqCtx, topicIds, metrics::onTopicDeleteCompleted);
        job.run();
    }

    @Override
    public List<DeleteRecordsResponse> deleteRecords(final List<DeleteRecordsRequest> requests) {
        final DeleteRecordsJob job = new DeleteRecordsJob(time, jooqCtx, requests);
        return job.call();
    }

    @Override
    public List<FileToDelete> getFilesToDelete() {
        final FindFilesToDeleteJob job = new FindFilesToDeleteJob(time, jooqCtx);
        return job.call();
    }

    @Override
    public void close() throws IOException {
        hikariDataSource.close();
    }
}
