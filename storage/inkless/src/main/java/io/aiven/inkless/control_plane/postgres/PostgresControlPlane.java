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
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

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
import io.aiven.inkless.control_plane.GetDisklessLogRequest;
import io.aiven.inkless.control_plane.GetDisklessLogResponse;
import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.control_plane.MergedFileBatch;

public class PostgresControlPlane extends AbstractControlPlane {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PostgresControlPlane.class);

    private static final String POOL_NAME = "pg-control-plane";

    private final KafkaMetricsGroup metrics = new KafkaMetricsGroup(PostgresConnectionPoolMetrics.class);
    private final PostgresControlPlaneMetrics pgMetrics;

    private HikariDataSource jobsDataSource;
    private HikariDataSource readDataSource;
    private HikariDataSource writeDataSource;
    private PostgresControlPlaneConfig controlPlaneConfig;

    private DSLContext readJooqCtx;
    private DSLContext writeJooqCtx;
    private DSLContext jobsJooqCtx;

    public PostgresControlPlane(final Time time) {
        super(time);

        this.pgMetrics = new PostgresControlPlaneMetrics(time);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        controlPlaneConfig = new PostgresControlPlaneConfig(configs);
        LOGGER.info("Configuring PostgresControlPlane");

        controlPlaneConfig.initializeReadWriteConfigs();
        LOGGER.info("Initialized read/write configurations");

        Migrations.migrate(controlPlaneConfig);
        LOGGER.info("Database migrations completed");

        jobsDataSource = new HikariDataSource(dataSourceConfig(metrics, POOL_NAME, controlPlaneConfig));

        // Avoid merger/cleaner waiting on class loading deadlocks between threads
        try {
            Class.forName("org.jooq.generated.DefaultSchema");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        jobsJooqCtx = DSL.using(jobsDataSource, SQLDialect.POSTGRES);

        // Set up read and write contexts if configured
        if (controlPlaneConfig.writeConfig() != null) {
            LOGGER.info("Using separate write configuration");
            writeDataSource = new HikariDataSource(dataSourceConfig(metrics, POOL_NAME + "-write", controlPlaneConfig.writeConfig()));
            writeJooqCtx = DSL.using(writeDataSource, SQLDialect.POSTGRES);
        } else {
            LOGGER.info("No separate write configuration found, using jobs context for writes");
            writeJooqCtx = jobsJooqCtx;
        }
        if (controlPlaneConfig.readConfig() != null) {
            LOGGER.info("Using separate read configuration");
            readDataSource = new HikariDataSource(dataSourceConfig(metrics, POOL_NAME + "-read", controlPlaneConfig.readConfig()));
            readJooqCtx = DSL.using(readDataSource, SQLDialect.POSTGRES);
        } else {
            LOGGER.info("No separate write configuration found, using jobs context for reads");
            readJooqCtx = jobsJooqCtx;
        }
    }

    private static HikariConfig dataSourceConfig(final KafkaMetricsGroup metrics, final String name, final PostgresConnectionConfig connectionConfig) {
        final HikariConfig config = new HikariConfig();
        config.setPoolName(name);
        config.setJdbcUrl(connectionConfig.connectionString());
        config.setUsername(connectionConfig.username());
        config.setPassword(connectionConfig.password());
        config.setMetricsTrackerFactory((poolName, poolStats) -> new PostgresConnectionPoolMetrics(metrics, poolName, poolStats));
        config.setTransactionIsolation(IsolationLevel.TRANSACTION_READ_COMMITTED.name());

        config.setMaximumPoolSize(connectionConfig.maxConnections());

        // We're doing interactive transactions.
        config.setAutoCommit(false);
        return config;
    }

    @Override
    public void createTopicAndPartitions(final Set<CreateTopicAndPartitionsRequest> requests) {
        // Expected to be performed synchronously
        new TopicsAndPartitionsCreateJob(time, jobsJooqCtx, requests, pgMetrics::onTopicCreateCompleted).run();
    }

    @Override
    public void initDisklessLog(final Set<InitDisklessLogRequest> requests) {
        // Expected to be performed synchronously
        new InitDisklessLogJob(time, jobsJooqCtx, requests, pgMetrics::onInitDisklessLogCompleted).call();
    }

    @Override
    protected Iterator<CommitBatchResponse> commitFileForValidRequests(
        final String objectKey,
        final ObjectFormat format,
        final int uploaderBrokerId,
        final long fileSize,
        final Stream<CommitBatchRequest> requests) {
        final CommitFileJob job = new CommitFileJob(
            time, writeJooqCtx, // use specific write context when committing files
            objectKey, format, uploaderBrokerId, fileSize, requests.toList(),
            pgMetrics::onCommitFileCompleted);
        return job.call().iterator();
    }

    @Override
    protected Iterator<FindBatchResponse> findBatchesForExistingPartitions(
        final Stream<FindBatchRequest> requests,
        final int fetchMaxBytes,
        final int maxBatchesPerPartition
    ) {
        final FindBatchesJob job = new FindBatchesJob(
            time, readJooqCtx, // use specific read context when finding batches
            requests.toList(), fetchMaxBytes, maxBatchesPerPartition,
            pgMetrics::onFindBatchesCompleted);
        return job.call().iterator();
    }

    @Override
    protected Iterator<ListOffsetsResponse> listOffsetsForExistingPartitions(Stream<ListOffsetsRequest> requests) {
        final ListOffsetsJob job = new ListOffsetsJob(
            time, readJooqCtx,
            requests.toList(),
            pgMetrics::onListOffsetsCompleted);
        return job.call().iterator();
    }

    @Override
    public void deleteTopics(final Set<Uuid> topicIds) {
        final DeleteTopicJob job = new DeleteTopicJob(time, jobsJooqCtx, topicIds, pgMetrics::onTopicDeleteCompleted);
        job.run();
    }

    @Override
    public List<DeleteRecordsResponse> deleteRecords(final List<DeleteRecordsRequest> requests) {
        final DeleteRecordsJob job = new DeleteRecordsJob(time, jobsJooqCtx, requests, pgMetrics::onDeleteRecordsCompleted);
        return job.call();
    }

    @Override
    public List<EnforceRetentionResponse> enforceRetention(final List<EnforceRetentionRequest> requests, final int maxBatchesPerRequest) {
        try {
            final EnforceRetentionJob job = new EnforceRetentionJob(time, jobsJooqCtx, requests, maxBatchesPerRequest, pgMetrics::onEnforceRetentionCompleted);
            return job.call();
        } catch (final Exception e) {
            throw new ControlPlaneException("Failed to enforce retention", e);
        }
    }

    @Override
    public List<FileToDelete> getFilesToDelete() {
        try {
            final FindFilesToDeleteJob job = new FindFilesToDeleteJob(time, jobsJooqCtx, pgMetrics::onGetFilesToDeleteCompleted);
            return job.call();
        } catch (final Exception e) {
            throw new ControlPlaneException("Failed to get files to delete", e);
        }
    }

    @Override
    public void deleteFiles(DeleteFilesRequest request) {
        try {
            final DeleteFilesJob job = new DeleteFilesJob(time, jobsJooqCtx, request, pgMetrics::onFilesDeleteCompleted);
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
            jobsJooqCtx,
            pgMetrics::onGetFileMergeWorkItemCompleted
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
        final List<MergedFileBatch> batches
    ) {
        final CommitFileMergeWorkItemJob job = new CommitFileMergeWorkItemJob(
            time,
            workItemId,
            objectKey,
            format,
            uploaderBrokerId,
            fileSize,
            batches,
            jobsJooqCtx,
            pgMetrics::onCommitFileMergeWorkItemCompleted
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
            new ReleaseFileMergeWorkItemJob( time, workItemId, jobsJooqCtx, pgMetrics::onReleaseFileMergeWorkItemCompleted);
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
                new SafeDeleteFileCheckJob(time, jobsJooqCtx, objectKeyPath, pgMetrics::onSafeDeleteFileCheckCompleted);
            return job.call();
        } catch (Exception e) {
            throw new ControlPlaneException("Error when checking if safe to delete file " + objectKeyPath, e);
        }
    }

    @Override
    public List<GetDisklessLogResponse> getDisklessLog(final List<GetDisklessLogRequest> requests) {
        try {
            final GetDisklessLogJob job = new GetDisklessLogJob(time, readJooqCtx, requests, pgMetrics::onGetDisklessLogCompleted);
            return job.call();
        } catch (final Exception e) {
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new ControlPlaneException("Failed to get diskless log", e);
            }
        }
    }

    @Override
    public List<GetLogInfoResponse> getLogInfo(final List<GetLogInfoRequest> requests) {
        try {
            // used for testing purposes only, so using jobs connection pool is fine
            final GetLogInfoJob job = new GetLogInfoJob(time, jobsJooqCtx, requests, pgMetrics::onGetLogInfoCompleted);
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
        jobsDataSource.close();
        if (writeDataSource != null) {
            writeDataSource.close();
        }
        if (readDataSource != null) {
            readDataSource.close();
        }
        pgMetrics.close();
    }
}
