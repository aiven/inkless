// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import io.aiven.inkless.common.UuidUtil;
import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.control_plane.FileState;

public class DBUtils {
    static Set<Log> getAllLogs(final HikariDataSource hikariDataSource) {
        final Set<DBUtils.Log> result = new HashSet<>();
        try (final Connection connection = hikariDataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement("SELECT * FROM logs");
             final ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                final Uuid topicId = UuidUtil.fromJava(resultSet.getObject("topic_id", UUID.class));
                final int partition = resultSet.getInt("partition");
                final String topicName = resultSet.getString("topic_name");
                final long logStartOffset = resultSet.getLong("log_start_offset");
                final long highWatermark = resultSet.getLong("high_watermark");
                result.add(new DBUtils.Log(topicId, partition, topicName, logStartOffset, highWatermark));
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    record Log(Uuid topicId, int partition, String topicName, long logStartOffset, long highWatermark) {
    }

    static Set<File> getAllFiles(final HikariDataSource hikariDataSource) {
        final Set<DBUtils.File> result = new HashSet<>();
        try (final Connection connection = hikariDataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement("SELECT * FROM files");
             final ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                final long id = resultSet.getLong("file_id");
                final String objectKey = resultSet.getString("object_key");
                final FileReason reason = FileReason.fromName(resultSet.getString("reason"));
                final FileState state = FileState.fromName(resultSet.getString("state"));
                final int uploaderBrokerId = resultSet.getInt("uploader_broker_id");
                final Instant committedAt = resultSet.getTimestamp("committed_at").toInstant();
                final long size = resultSet.getLong("size");
                final long usedSize = resultSet.getLong("used_size");
                result.add(new DBUtils.File(
                    id, objectKey, reason, state, uploaderBrokerId, committedAt, size, usedSize));
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    static Set<FileToDelete> getAllFilesToDelete(final HikariDataSource hikariDataSource) {
        final Set<DBUtils.FileToDelete> result = new HashSet<>();
        try (final Connection connection = hikariDataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement("SELECT * FROM files_to_delete");
             final ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                final long id = resultSet.getLong("file_id");
                final Instant markedForDeletionAt = resultSet.getTimestamp("marked_for_deletion_at").toInstant();
                result.add(new DBUtils.FileToDelete(id, markedForDeletionAt));
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    static Set<Batch> getAllBatches(final HikariDataSource hikariDataSource) {
        final Set<DBUtils.Batch> result = new HashSet<>();
        try (final Connection connection = hikariDataSource.getConnection();
             final PreparedStatement statement = connection.prepareStatement("SELECT * FROM batches");
             final ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                final Uuid topicId = UuidUtil.fromJava(resultSet.getObject("topic_id", UUID.class));
                final int partition = resultSet.getInt("partition");
                final long baseOffset = resultSet.getLong("base_offset");
                final long lastOffset = resultSet.getLong("last_offset");
                final long fileId = resultSet.getLong("file_id");
                final long byteOffset = resultSet.getLong("byte_offset");
                final long byteSize = resultSet.getLong("byte_size");
                final long numberOfRecords = resultSet.getLong("number_of_records");
                result.add(new DBUtils.Batch(topicId, partition, baseOffset, lastOffset, fileId, byteOffset, byteSize, numberOfRecords));
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    record File(long id,
                String objectKey,
                FileReason reason,
                FileState state,
                int uploaderBrokerId,
                Instant committedAt,
                long size,
                long usedSize) {
    }

    record Batch(Uuid topicId,
                 int partition,
                 long baseOffset,
                 long lastOffset,
                 long fileId,
                 long byteOffset,
                 long byteSize,
                 long numberOfRecords) {
    }

    record FileToDelete(long id,
                        Instant markedForDeletionAt) {
    }
}
