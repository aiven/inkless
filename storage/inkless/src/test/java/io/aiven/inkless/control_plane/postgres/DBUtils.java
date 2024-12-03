package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;

import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import io.aiven.inkless.common.UuidUtil;

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
                final String objectKey = resultSet.getString("object_key");
                final long byteOffset = resultSet.getLong("byte_offset");
                final long byteSize = resultSet.getLong("byte_size");
                final long numberOfRecords = resultSet.getLong("number_of_records");
                result.add(new DBUtils.Batch(topicId, partition, baseOffset, lastOffset, objectKey, byteOffset, byteSize, numberOfRecords));
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    record Batch(Uuid topicId,
                 int partition,
                 long baseOffset,
                 long lastOffset,
                 String objectKey,
                 long byteOffset,
                 long byteSize,
                 long numberOfRecords) {
    }
}
