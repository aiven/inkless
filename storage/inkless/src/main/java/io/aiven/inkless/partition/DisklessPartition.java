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
package io.aiven.inkless.partition;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.DisklessCommitRequestData;
import org.apache.kafka.common.message.DisklessCommitResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.generated.CoordinatorBatchRecord;
import io.aiven.inkless.generated.CoordinatorFileRecord;

import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DisklessPartition implements Closeable {
    private final Logger LOGGER = LoggerFactory.getLogger(DisklessPartition.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final Time time;
    private final TopicIdPartition topicIdPartition;
    private final int brokerId;
    private final UnifiedLog log;
    private final Connection dbConnection;

    private final PreparedStatement getHighWatermarkStmt;
    private final PreparedStatement setHighWatermarkStmt;
    private final PreparedStatement insertFileStmt;
    private final PreparedStatement insertBatchStmt;

    DisklessPartition(final Time time,
                      final TopicIdPartition topicIdPartition,
                      final int brokerId,
                      final UnifiedLog log) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition cannot be null");
        this.brokerId = brokerId;
        this.log = Objects.requireNonNull(log, "log cannot be null");

        final var dbUrl = getDbUrl();
        this.dbConnection = java.sql.DriverManager.getConnection(dbUrl);
        dbConnection.setAutoCommit(false);

        initDBIfNeeded();

        this.getHighWatermarkStmt = dbConnection.prepareStatement(
            "SELECT high_watermark FROM log WHERE id = 0");
        this.setHighWatermarkStmt = dbConnection.prepareStatement(
            "UPDATE log " +
                "SET high_watermark = ? " +
                "WHERE id = 0");
        this.insertFileStmt = dbConnection.prepareStatement(
            "INSERT INTO files (object_key, format, reason, state, uploader_broker_id, committed_at, size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "RETURNING file_id");
        this.insertBatchStmt = dbConnection.prepareStatement(
            "INSERT INTO batches (magic, base_offset, last_offset, file_id, byte_offset, byte_size, timestamp_type, log_append_timestamp, batch_max_timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "RETURNING batch_id");
    }

    private void initDBIfNeeded() {
        lock.writeLock().lock();
        try {
            final Flyway flyway = Flyway.configure()
                .dataSource(getDbUrl(), null, null)
                .locations("classpath:partition_db/migration")
                .load();
            flyway.migrate();
        } finally {
            lock.writeLock().unlock();
        }
    }

    void commit(final String objectKey,
                final ObjectFormat format,
                final int fileSize,
                final DisklessCommitRequestData.PartitionDisklessCommitData partitionData) {
        Objects.requireNonNull(objectKey, "objectKey cannot be null");
        Objects.requireNonNull(format, "format cannot be null");
        Objects.requireNonNull(partitionData, "partitionData cannot be null");

        final long now = time.milliseconds();

        final var response = new DisklessCommitResponseData.PartitionDisklessCommitResponse();
        response.setPartition(topicIdPartition.partition());
        response.setErrorCode(Errors.NONE.code());
        response.setErrorMessage(null);
//        response.setBaseOffset()

        final List<ApiMessageAndVersion> records = new ArrayList<>();

        lock.writeLock().lock();
        try {
            final CoordinatorFileRecord fileRecord = insertFile(now, objectKey, format, fileSize);

            records.add(new ApiMessageAndVersion(fileRecord, (short) 0));

            for (final var batch : partitionData.batches()) {
                final long firstOffset = getHighWatermark();

                // TODO idempotence

                final long offsetDelta = batch.lastOffset() - batch.baseOffset();
                final long lastOffset = firstOffset + offsetDelta;
                setHighWatermark(lastOffset + 1);

                final CoordinatorBatchRecord coordinatorBatchRecord =
                    insertBatch(now, batch, firstOffset, lastOffset, fileRecord.fileId());
                records.add(new ApiMessageAndVersion(coordinatorBatchRecord, (short) 0));
            }
            dbConnection.commit();

            final SimpleRecord[] recordsArr = records.toArray(new SimpleRecord[0]);
            final MemoryRecords memoryRecords = MemoryRecords.withRecords(Compression.zstd().build(), recordsArr);
//            final LogAppendInfo logAppendInfo =
                log.appendAsLeader(memoryRecords, log.latestEpoch().get());  // TODO can we just get epoch?
        } catch (final SQLException | IOException e) {
            // TODO handle
            throw new RuntimeException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private long getHighWatermark() throws SQLException {
        try (final ResultSet resultSet = getHighWatermarkStmt.executeQuery()) {
            if (!resultSet.next()) {
                throw new IllegalStateException();
            }
            return resultSet.getLong("high_watermark");
        }
    }

    private void setHighWatermark(final long newHighWatermark) throws SQLException {
        setHighWatermarkStmt.setLong(1, newHighWatermark);
        if (setHighWatermarkStmt.execute()) {
            throw new RuntimeException();
        }
    }

    private CoordinatorFileRecord insertFile(final long now,
                                             final String objectKey,
                                             final ObjectFormat format,
                                             final int size) throws SQLException {
        final CoordinatorFileRecord record = new CoordinatorFileRecord();

        insertFileStmt.setString(1, objectKey);
        record.setObjectKey(objectKey);

        insertFileStmt.setByte(2, format.id);
        record.setFormat(format.id);

        final byte reason = (byte) 1;
        insertFileStmt.setByte(3, reason);  // TODO enum for reason
        record.setReason(reason);

        final byte state = (byte) 1;
        insertFileStmt.setByte(4, state);  // TODO enum for state
        record.setReason(state);

        insertFileStmt.setInt(5, brokerId);
        record.setUploaderBrokerId(brokerId);

        insertFileStmt.setLong(6, now);
        record.setCommittedAt(now);

        insertFileStmt.setInt(7, size);
        record.setSize(size);

        record.setMarkedForDeletionAt(-1);

        final int fileId;
        try (final ResultSet resultSet = insertFileStmt.executeQuery()) {
            if (!resultSet.next()) {
                throw new IllegalStateException();
            }
            fileId = resultSet.getInt(1);
        }
        record.setFileId(fileId);

        return record;
    }

    private CoordinatorBatchRecord insertBatch(final long now,
                                               final DisklessCommitRequestData.BatchDisklessCommitData batch,
                                               final long baseOffset,
                                               final long lastOffset,
                                               final int fileId) throws SQLException {
        final CoordinatorBatchRecord record = new CoordinatorBatchRecord();

        insertBatchStmt.setByte(1, batch.magic());
        record.setMagic(batch.magic());

        insertBatchStmt.setLong(2, baseOffset);
        record.setBaseOffset(baseOffset);

        insertBatchStmt.setLong(3, lastOffset);
        record.setLastOffset(lastOffset);

        insertBatchStmt.setInt(4, fileId);
        record.setFileId(fileId);

        insertBatchStmt.setInt(5, batch.byteOffset());
        record.setByteOffset(batch.byteOffset());

        insertBatchStmt.setInt(6, batch.byteSize());
        record.setByteSize(batch.byteSize());

        insertBatchStmt.setInt(7, batch.timestampType());
        record.setTimestampType(batch.timestampType());

        insertBatchStmt.setLong(8, now);
        record.setLogAppendTimestamp(now);

        insertBatchStmt.setLong(9, batch.batchMaxTimestamp());
        record.setBatchMaxTimestamp(batch.batchMaxTimestamp());

        final long batchId;
        try (final ResultSet resultSet = insertBatchStmt.executeQuery()) {
            if (!resultSet.next()) {
                throw new IllegalStateException();
            }
            batchId = resultSet.getLong(1);
        }
        record.setBatchId(batchId);

        return record;
    }

    void delete() {
        lock.writeLock().lock();
        try {
            if (!dbConnection.isClosed()) {
                throw new IllegalStateException("DB connection must be closed first");
            }
            if (!dbFilePath().toFile().delete()) {
                LOGGER.warn("{} wasn't deleted", dbFilePath().toFile());
            }
        } catch (final SQLException e) {
            LOGGER.error("Cannot delete DB file in {}", topicIdPartition.topicPartition(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private String getDbUrl() {
        return "jdbc:sqlite:" + dbFilePath();
    }

    private Path dbFilePath() {
        return this.log.dir().toPath().resolve("state.db");
    }

    @Override
    public void close() {
        Utils.closeQuietly(
            dbConnection,
            String.format("SQLite connection in %s", topicIdPartition.topicPartition()),
            LOGGER
        );
    }
}
