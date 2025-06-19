/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.IsolationLevel;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.generated.udt.records.CommitFileMergeWorkItemBatchV1Record;
import org.jooq.generated.udt.records.CommitFileMergeWorkItemResponseV1Record;
import org.jooq.impl.DSL;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.AbstractControlPlane;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteFilesRequest;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;
import io.aiven.inkless.control_plane.EnforceRetentionRequest;
import io.aiven.inkless.control_plane.EnforceRetentionResponse;
import io.aiven.inkless.control_plane.FileMergeWorkItem;
import io.aiven.inkless.control_plane.FileMergeWorkItemNotExist;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.control_plane.MergedFileBatch;

public class PostgresControlPlane extends AbstractControlPlane {
    private static final String POOL_NAME = "inkless-control-plane";

    private final PostgresControlPlaneMetrics metrics;

    private HikariDataSource hikariDataSource;
    private DSLContext jooqCtx;
    private PostgresControlPlaneConfig controlPlaneConfig;

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
        controlPlaneConfig = new PostgresControlPlaneConfig(configs);

        Migrations.migrate(controlPlaneConfig);

        final HikariConfig config = new HikariConfig();
        config.setPoolName(POOL_NAME);
        config.setJdbcUrl(controlPlaneConfig.connectionString());
        config.setUsername(controlPlaneConfig.username());
        config.setPassword(controlPlaneConfig.password());
        config.setMetricsTrackerFactory(HikariMetricsTracker::new);
        config.setTransactionIsolation(IsolationLevel.TRANSACTION_READ_COMMITTED.name());

        config.setMaximumPoolSize(controlPlaneConfig.maxConnections());

        // We're doing interactive transactions.
        config.setAutoCommit(false);

        hikariDataSource = new HikariDataSource(config);

        // Avoid merger/cleaner waiting on class loading deadlocks between threads
        try {
            Class.forName("org.jooq.generated.DefaultSchema");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        jooqCtx = DSL.using(hikariDataSource, SQLDialect.POSTGRES);
    }

    @Override
    protected Iterator<CommitBatchResponse> commitFileForValidRequests(
        final String objectKey,
        final ObjectFormat format,
        final int uploaderBrokerId,
        final long fileSize,
        final Stream<CommitBatchRequest> requests) {
        final CommitFileJob job = new CommitFileJob(
            time, jooqCtx,
            objectKey, format, uploaderBrokerId, fileSize, requests.toList(),
            metrics::onCommitFileCompleted);
        return job.call().iterator();
    }

    @Override
    protected Iterator<FindBatchResponse> findBatchesForExistingPartitions(
        final Stream<FindBatchRequest> requests,
        final int fetchMaxBytes,
        final int maxBatchesPerPartition
    ) {
        final FindBatchesJob job = new FindBatchesJob(
            time, jooqCtx,
            requests.toList(), fetchMaxBytes, maxBatchesPerPartition,
            metrics::onFindBatchesCompleted);
        return job.call().iterator();
    }

    @Override
    protected Iterator<ListOffsetsResponse> listOffsetsForExistingPartitions(Stream<ListOffsetsRequest> requests) {
            final ListOffsetsJob job = new ListOffsetsJob(
                    time, jooqCtx,
                    requests.toList(), metrics::onListOffsetsCompleted);
            return job.call().iterator();
    }

    @Override
    public void deleteTopics(final Set<Uuid> topicIds) {
        final DeleteTopicJob job = new DeleteTopicJob(time, jooqCtx, topicIds, metrics::onTopicDeleteCompleted);
        job.run();
    }

    @Override
    public List<DeleteRecordsResponse> deleteRecords(final List<DeleteRecordsRequest> requests) {
        final DeleteRecordsJob job = new DeleteRecordsJob(time, jooqCtx, requests, metrics::onDeleteRecordsCompleted);
        return job.call();
    }

    @Override
    public List<EnforceRetentionResponse> enforceRetention(final List<EnforceRetentionRequest> requests, final int maxBatchesPerRequest) {
        try {
            final EnforceRetentionJob job = new EnforceRetentionJob(time, jooqCtx, requests, maxBatchesPerRequest, metrics::onEnforceRetentionCompleted);
            return job.call();
        } catch (final Exception e) {
            throw new ControlPlaneException("Failed to enforce retention", e);
        }
    }

    @Override
    public List<FileToDelete> getFilesToDelete() {
        try {
            final FindFilesToDeleteJob job = new FindFilesToDeleteJob(time, jooqCtx, metrics::onGetFilesToDeleteCompleted);
            return job.call();
        } catch (final Exception e) {
            throw new ControlPlaneException("Failed to get files to delete", e);
        }
    }

    @Override
    public void deleteFiles(DeleteFilesRequest request) {
        try {
            final DeleteFilesJob job = new DeleteFilesJob(time, jooqCtx, request, metrics::onFilesDeleteCompleted);
            job.run();
        } catch (final Exception e) {
            throw new ControlPlaneException("Failed to delete files", e);
        }
    }

    @Override
    public FileMergeWorkItem getFileMergeWorkItem() {
        final GetFileMergeWorkItemJob job = new GetFileMergeWorkItemJob(
            time,
            controlPlaneConfig.fileMergeLockPeriod(),
            controlPlaneConfig.fileMergeSizeThresholdBytes(),
            jooqCtx,
            metrics::onGetFileMergeWorkItemCompleted
        );
        return job.call();
    }

    @Override
    public void commitFileMergeWorkItem(
            final long workItemId,
            final String objectKey,
            final ObjectFormat format,
            final int uploaderBrokerId,
            final long fileSize,
            final List<MergedFileBatch> batches) {
        final CommitFileMergeWorkItemJob job = new CommitFileMergeWorkItemJob(
            time,
            workItemId,
            objectKey,
            format,
            uploaderBrokerId,
            fileSize,
            batches,
            jooqCtx,
            metrics::onCommitFileMergeWorkItemCompleted
        );
        final var result = job.call();
        switch (result.getError()) {
            case none:
                break;
            case file_merge_work_item_not_found:
                throw new FileMergeWorkItemNotExist(workItemId);
            case invalid_parent_batch_count: {
                final MergedFileBatch mergedFileBatch = getMergedFileBatch(result);
                throw new ControlPlaneException(
                    String.format("Invalid parent batch count %d in %s",
                        mergedFileBatch.parentBatches().size(),
                        mergedFileBatch
                    )
                );
            }
            case batch_not_part_of_work_item: {
                final MergedFileBatch mergedFileBatch = getMergedFileBatch(result);
                throw new ControlPlaneException(
                    String.format(
                        "Batch %d is not part of work item in %s",
                            mergedFileBatch.parentBatches().get(0),
                            mergedFileBatch
                        )
                );
            }
        }
    }

    private static MergedFileBatch getMergedFileBatch(CommitFileMergeWorkItemResponseV1Record result) {
        final CommitFileMergeWorkItemBatchV1Record errorBatch = result.getErrorBatch();
        return new MergedFileBatch(
            new BatchMetadata(
                errorBatch.getMetadata().getMagic().byteValue(),
                new TopicIdPartition(
                    errorBatch.getMetadata().getTopicId(),
                    errorBatch.getMetadata().getPartition(),
                    errorBatch.getMetadata().getTopicName()
                ),
                errorBatch.getMetadata().getByteOffset(),
                errorBatch.getMetadata().getByteSize(),
                errorBatch.getMetadata().getBaseOffset(),
                errorBatch.getMetadata().getLastOffset(),
                errorBatch.getMetadata().getLogAppendTimestamp(),
                errorBatch.getMetadata().getBatchMaxTimestamp(),
                errorBatch.getMetadata().getTimestampType()
            ),
            Arrays.asList(errorBatch.getParentBatchIds())
        );
    }

    @Override
    public void releaseFileMergeWorkItem(final long workItemId) {
        final ReleaseFileMergeWorkItemJob job =
            new ReleaseFileMergeWorkItemJob( time, workItemId, jooqCtx, metrics::onReleaseFileMergeWorkItemCompleted);
        final var result = job.call();
        switch (result.getError()) {
            case none:
                break;
            case file_merge_work_item_not_found:
                throw new FileMergeWorkItemNotExist(workItemId);
        }
    }

    @Override
    public boolean isSafeToDeleteFile(String objectKeyPath) {
        try {
            final SafeDeleteFileCheckJob job =
                new SafeDeleteFileCheckJob(time, jooqCtx, objectKeyPath, metrics::onSafeDeleteFileCheckCompleted);
            return job.call();
        } catch (Exception e) {
            throw new ControlPlaneException("Error when checking if safe to delete file " + objectKeyPath, e);
        }
    }

    @Override
    public List<GetLogInfoResponse> getLogInfo(final List<GetLogInfoRequest> requests) {
        try {
            final GetLogInfoJob job = new GetLogInfoJob(time, jooqCtx, requests, metrics::onGetLogInfoCompleted);
            return job.call();
        } catch (final Exception e) {
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new ControlPlaneException("Failed to get log info", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        hikariDataSource.close();
    }
}
