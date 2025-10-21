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
package io.aiven.inkless.control_plane.topic;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.AbstractControlPlane;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteFilesRequest;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;
import io.aiven.inkless.control_plane.EnforceRetentionRequest;
import io.aiven.inkless.control_plane.EnforceRetentionResponse;
import io.aiven.inkless.control_plane.FileMergeWorkItem;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.control_plane.MergedFileBatch;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The part of the topic-based control plane that directly performs commands.
 */
public class TopicBasedControlPlaneInternal extends AbstractControlPlane {
    private final Logger LOGGER = LoggerFactory.getLogger(TopicBasedControlPlaneInternal.class);

    private final ReentrantLock lock = new ReentrantLock();

    private final RecordWriter recordWriter;
    private TopicBasedControlPlaneInternalConfig controlPlaneConfig;
    private Connection dbConnection;
    private TopicsAndPartitionsCreateJob topicsAndPartitionsCreateJob;
    private CommitFileJob commitFileJob;
    private FindBatchesJob findBatchesJob;
    private GetFilesToDeleteJob getFilesToDeleteJob;
    private DeleteRecordsJobs deleteRecordsJobs;
    private DeleteTopicsJob deleteTopicsJob;
    private ListOffsetsJob listOffsetsJob;
    private GetLogInfoJob getLogInfoJob;
    private MarkFileForDeletionIfNeededRoutine markFileForDeletionIfNeededRoutine;

    public TopicBasedControlPlaneInternal(final Time time,
                                          final RecordWriter recordWriter) {
        super(time);
        this.recordWriter = Objects.requireNonNull(recordWriter, "recordWriter cannot be null");
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        lock.lock();
        try {
            this.controlPlaneConfig = new TopicBasedControlPlaneInternalConfig(configs);
            final String dbUrl = dbUrl();
            final Flyway flyway = Flyway.configure()
                .dataSource(dbUrl, null, null)
                .locations("classpath:partition_db/migration")
                .load();
            flyway.migrate();

            this.dbConnection = java.sql.DriverManager.getConnection(dbUrl);
            dbConnection.setAutoCommit(false);

            try (var stmt = dbConnection.createStatement()) {
                stmt.execute("PRAGMA foreign_keys = ON;");
            }

            this.topicsAndPartitionsCreateJob = new TopicsAndPartitionsCreateJob(time, dbConnection);
            this.getLogInfoJob = new GetLogInfoJob(time, dbConnection);
            this.markFileForDeletionIfNeededRoutine = new MarkFileForDeletionIfNeededRoutine(time, dbConnection);
            this.commitFileJob = new CommitFileJob(time, dbConnection, getLogInfoJob, markFileForDeletionIfNeededRoutine);
            this.findBatchesJob = new FindBatchesJob(time, dbConnection, getLogInfoJob);
            this.getFilesToDeleteJob = new GetFilesToDeleteJob(time, dbConnection);
            this.deleteRecordsJobs = new DeleteRecordsJobs(time, dbConnection, getLogInfoJob, markFileForDeletionIfNeededRoutine);
            this.deleteTopicsJob = new DeleteTopicsJob(time, dbConnection, markFileForDeletionIfNeededRoutine);
            this.listOffsetsJob = new ListOffsetsJob(time, dbConnection, getLogInfoJob);
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    private String dbUrl() {
        return "jdbc:sqlite:" + controlPlaneConfig.dbDir().resolve("diskless_state.db");
    }

    @Override
    public void createTopicAndPartitions(final Set<CreateTopicAndPartitionsRequest> requests) {
        lock.lock();
        try {
            this.topicsAndPartitionsCreateJob.run(requests, d -> {});  // TODO duration
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected Iterator<CommitBatchResponse> commitFileForValidRequests(
        final String objectKey,
        final ObjectFormat format,
        final int uploaderBrokerId,
        final long fileSize,
        final Stream<CommitBatchRequest> requests
    ) {
        lock.lock();
        try {
            return this.commitFileJob.call(objectKey, format, uploaderBrokerId, fileSize, requests.toList(),
                d -> {}  // TODO duration
                ).iterator();
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected Iterator<FindBatchResponse> findBatchesForExistingPartitions(
        final Stream<FindBatchRequest> requests,
        final int fetchMaxBytes,
        final int maxBatchesPerPartition
    ) {
        lock.lock();
        try {
            return findBatchesJob.call(requests.toList(), fetchMaxBytes, maxBatchesPerPartition,
                d -> {}  // TODO duration
                ).iterator();
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected Iterator<ListOffsetsResponse> listOffsetsForExistingPartitions(final Stream<ListOffsetsRequest> requests) {
        lock.lock();
        try {
            return listOffsetsJob.call(requests.toList(),
                d -> {}  // TODO duration
                ).iterator();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void deleteTopics(final Set<Uuid> topicIds) {
        lock.lock();
        try {
            deleteTopicsJob.run(topicIds,
                d -> {}  // TODO duration
                );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<DeleteRecordsResponse> deleteRecords(final List<DeleteRecordsRequest> requests) {
        lock.lock();
        try {
            return deleteRecordsJobs.call(requests,
                d -> {}  // TODO duration
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<EnforceRetentionResponse> enforceRetention(
        final List<EnforceRetentionRequest> requests,
        final int maxBatchesPerRequest
    ) {
        lock.lock();
        try {
            return List.of();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<FileToDelete> getFilesToDelete() {
        lock.lock();
        try {
            return this.getFilesToDeleteJob.call(
                d -> {}  // TODO duration
            );
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void deleteFiles(final DeleteFilesRequest request) {
        lock.lock();
        try {

        } finally {
            lock.unlock();
        }
    }

    @Override
    public FileMergeWorkItem getFileMergeWorkItem() {
        lock.lock();
        try {
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commitFileMergeWorkItem(final long workItemId,
                                        final String objectKey,
                                        final ObjectFormat format,
                                        final int uploaderBrokerId,
                                        final long fileSize,
                                        final List<MergedFileBatch> batches) {
        lock.lock();
        try {
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void releaseFileMergeWorkItem(final long workItemId) {
        lock.lock();
        try {

        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isSafeToDeleteFile(final String objectKeyPath) {
        lock.lock();
        try {
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<GetLogInfoResponse> getLogInfo(final List<GetLogInfoRequest> requests) {
        lock.lock();
        try {
            return this.getLogInfoJob.call(requests, d -> {});  // TODO duration
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(topicsAndPartitionsCreateJob, "topicsAndPartitionsCreateJob");
        Utils.closeQuietly(commitFileJob, "commitFileJob");
        Utils.closeQuietly(findBatchesJob, "findBatchesJob");
        Utils.closeQuietly(getFilesToDeleteJob, "getFilesToDeleteJob");
        Utils.closeQuietly(deleteRecordsJobs, "deleteRecordsJobs");
        Utils.closeQuietly(deleteTopicsJob, "deleteTopicsJob");
        Utils.closeQuietly(getLogInfoJob, "getLogInfoJob");
        Utils.closeQuietly(listOffsetsJob, "listOffsetsJob");
        Utils.closeQuietly(markFileForDeletionIfNeededRoutine, "markFileForDeletionIfNeededRoutine");
        Utils.closeQuietly(dbConnection, "dbConnection");
    }
}
