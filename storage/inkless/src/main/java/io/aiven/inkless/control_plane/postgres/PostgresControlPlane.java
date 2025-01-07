// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.TopicsDelta;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.IsolationLevel;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import io.aiven.inkless.control_plane.AbstractControlPlane;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.control_plane.MetadataView;

public class PostgresControlPlane extends AbstractControlPlane {

    private final ExecutorService executor;
    private final PostgresControlPlaneMetrics metrics;

    private HikariDataSource hikariDataSource;

    public PostgresControlPlane(final Time time,
                                final MetadataView metadataView) {
        this(time, metadataView, Executors.newCachedThreadPool());
    }

    // Visible for testing
    PostgresControlPlane(final Time time,
                         final MetadataView metadataView,
                         final ExecutorService executor) {
        super(time, metadataView);
        this.executor = executor;
        this.metrics = new PostgresControlPlaneMetrics(time);
    }

    public Future<?> onTopicMetadataChanges(final TopicsDelta topicsDelta) {
        // Create.
        return executor.submit(new TopicsCreateJob(
            time, metadataView, hikariDataSource,
            topicsDelta.changedTopics(),
            metrics::onTopicCreateCompleted));
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
    }

    @Override
    protected Iterator<CommitBatchResponse> commitFileForExistingPartitions(
        final String objectKey,
        final int uploaderBrokerId,
        final long fileSize,
        final Stream<CommitBatchRequest> requests) {
        final var requestExtras = requests.map(r -> new CommitFileJob.CommitBatchRequestExtra(
            r,
            metadataView.getTopicId(r.topicPartition().topic())
        )).toList();
        final CommitFileJob job = new CommitFileJob(
            time, hikariDataSource,
            objectKey, uploaderBrokerId, fileSize, requestExtras,
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

    @Override
    public void close() throws IOException {
        hikariDataSource.close();
    }
}
