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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

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

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final RecordWriter recordWriter;
    private TopicBasedControlPlaneInternalConfig controlPlaneConfig;
    private Connection dbConnection;
    private TopicsAndPartitionsCreateJob topicsAndPartitionsCreateJob;
    private GetLogInfoJob getLogInfoJob;

    public TopicBasedControlPlaneInternal(final Time time,
                                          final RecordWriter recordWriter) {
        super(time);
        this.recordWriter = Objects.requireNonNull(recordWriter, "recordWriter cannot be null");
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        lock.writeLock().lock();
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
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private String dbUrl() {
        return "jdbc:sqlite:" + controlPlaneConfig.dbDir().resolve("diskless_state.db");
    }

    @Override
    public void createTopicAndPartitions(final Set<CreateTopicAndPartitionsRequest> requests) {
        lock.writeLock().lock();
        try {
            this.topicsAndPartitionsCreateJob.run(requests, d -> {});  // TODO duration
        } finally {
            lock.writeLock().unlock();
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
        lock.writeLock().lock();
        try {
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    protected Iterator<FindBatchResponse> findBatchesForExistingPartitions(
        final Stream<FindBatchRequest> requests,
        final int fetchMaxBytes,
        final int maxBatchesPerPartition
    ) {
        lock.readLock().lock();
        try {
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    protected Iterator<ListOffsetsResponse> listOffsetsForExistingPartitions(final Stream<ListOffsetsRequest> truerequestsIn) {
        lock.readLock().lock();
        try {
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void deleteTopics(final Set<Uuid> topicIds) {
        lock.writeLock().lock();
        try {

        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<DeleteRecordsResponse> deleteRecords(final List<DeleteRecordsRequest> requests) {
        lock.writeLock().lock();
        try {
            return List.of();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<EnforceRetentionResponse> enforceRetention(
        final List<EnforceRetentionRequest> requests,
        final int maxBatchesPerRequest
    ) {
        lock.writeLock().lock();
        try {
            return List.of();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<FileToDelete> getFilesToDelete() {
        lock.writeLock().lock();
        try {
            return List.of();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void deleteFiles(final DeleteFilesRequest request) {
        lock.writeLock().lock();
        try {

        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public FileMergeWorkItem getFileMergeWorkItem() {
        lock.writeLock().lock();
        try {
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void commitFileMergeWorkItem(final long workItemId,
                                        final String objectKey,
                                        final ObjectFormat format,
                                        final int uploaderBrokerId,
                                        final long fileSize,
                                        final List<MergedFileBatch> batches) {
        lock.writeLock().lock();
        try {
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void releaseFileMergeWorkItem(final long workItemId) {
        lock.writeLock().lock();
        try {

        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public boolean isSafeToDeleteFile(final String objectKeyPath) {
        lock.readLock().lock();
        try {
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<GetLogInfoResponse> getLogInfo(final List<GetLogInfoRequest> requests) {
        lock.readLock().lock();
        try {
            return this.getLogInfoJob.call(requests, d -> {});  // TODO duration
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {

    }
}
