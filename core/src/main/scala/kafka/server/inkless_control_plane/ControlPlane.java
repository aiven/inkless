package kafka.server.inkless_control_plane;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.TopicPartition;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import kafka.server.inkless_common.CommitFileRequest;
import kafka.server.inkless_common.CommitFileResponse;
import kafka.server.inkless_common.FindBatchRequest;
import kafka.server.inkless_common.FindBatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ControlPlane {
    private static final Logger logger = LoggerFactory.getLogger(ControlPlane.class);

    // TODO double check Postgresql-side statement caching
    private final HikariDataSource hikari;

//    private final HashMap<TopicPartition, TreeMap<Long, Batch>> batchCoordinates = new HashMap<>();

    public ControlPlane() {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://127.0.0.1:5432/postgres");
        config.setUsername("postgres");
        config.setPassword("pwd");
        hikari = new HikariDataSource(config);
    }

    public synchronized CommitFileResponse commitFile(final CommitFileRequest request) {
        logger.error("Committing file");

        final List<Long> assignedOffsets = new ArrayList<>();

        // TODO correct transactions and isolation or Postgresql functions
        try (final Connection connection = hikari.getConnection()) {
            for (final CommitFileRequest.Batch batch : request.batches) {
                long assignedOffset = processBatch(connection, request.filePath, batch);
                assignedOffsets.add(assignedOffset);
            }
        } catch (final SQLException e) {
            // TODO handle
            logger.error("Error", e);
            throw new RuntimeException(e);
        }

        return new CommitFileResponse(assignedOffsets);
    }

    private long processBatch(final Connection connection,
                              final String filePath,
                              final CommitFileRequest.Batch batch) throws SQLException {
        final Long oldHighWatermark = getHighWatermark(connection, batch.topicPartition);
        final long assignedOffset = oldHighWatermark == null
            ? 0
            : oldHighWatermark;

        final long newHighWatermark = assignedOffset + batch.numberOfRecords;
        final BatchCoordinates batchCoordinates = new BatchCoordinates(
            assignedOffset, filePath, batch.byteOffset, batch.sizeInBytes, batch.numberOfRecords);
        setHighWatermark(connection, batch.topicPartition, newHighWatermark);
        insertBatchCoordinates(connection, batch.topicPartition, batchCoordinates);

        return assignedOffset;
    }

    private Long getHighWatermark(final Connection connection, final TopicPartition topicPartition) throws SQLException {
        try (final PreparedStatement stmt = connection.prepareStatement(
            "SELECT high_watermark "
                + "FROM logs "
                + "WHERE topic = ? "
                + "  AND partition = ?"
        )) {
            stmt.setString(1, topicPartition.topic());
            stmt.setInt(2, topicPartition.partition());
            final ResultSet resultSet = stmt.executeQuery();
            if (!resultSet.next()) {
                return null;
            } else {
                return resultSet.getLong("high_watermark");
            }
        }
    }

    private void insertBatchCoordinates(final Connection connection,
                                        final TopicPartition topicPartition,
                                        final BatchCoordinates batchCoordinates) throws SQLException {
        try (final PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO batch_coordinates (topic, partition, base_offset, file_path, byte_offset, byte_size, number_of_records) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?) "
        )) {
            stmt.setString(1, topicPartition.topic());
            stmt.setInt(2, topicPartition.partition());
            stmt.setLong(3, batchCoordinates.baseOffset);
            stmt.setString(4, batchCoordinates.filePath);
            stmt.setInt(5, batchCoordinates.byteOffset);
            stmt.setInt(6, batchCoordinates.byteSize);
            stmt.setLong(7, batchCoordinates.numberOfRecords);
            stmt.execute();
        }
    }

    public synchronized FindBatchResponse findBatch(final FindBatchRequest request) {
        final Long highWatermark;
        final BatchCoordinates batchCoordinates;
        try (final Connection connection = hikari.getConnection()) {
            highWatermark = getHighWatermark(connection, request.topicPartition);
            if (highWatermark == null) {
                // TODO handle
                throw new RuntimeException("highWatermark is null");
            }

            batchCoordinates = findPossibleBatch(connection, request.topicPartition, request.kafkaOffset);
            if (batchCoordinates == null) {
                return new FindBatchResponse(null, highWatermark);
            }
        } catch (final SQLException e) {
            // TODO handle
            logger.error("Error", e);
            throw new RuntimeException(e);
        }

        if (request.kafkaOffset >= batchCoordinates.baseOffset + batchCoordinates.numberOfRecords) {
            return new FindBatchResponse(null, highWatermark);
        }

        return new FindBatchResponse(
            new FindBatchResponse.BatchInfo(
                batchCoordinates.filePath,
                batchCoordinates.byteOffset,
                batchCoordinates.byteSize,
                batchCoordinates.baseOffset,
                batchCoordinates.numberOfRecords
            ),
            highWatermark
        );
    }

    private void setHighWatermark(final Connection connection,
                                  final TopicPartition topicPartition,
                                  final long newHighWatermark) throws SQLException {
        try (final PreparedStatement stmt = connection.prepareStatement(
            "INSERT INTO logs (topic, partition, high_watermark) "
                + "VALUES (?, ?, ?) "
                + "ON CONFLICT (topic, partition) DO UPDATE"
                + "  SET high_watermark = EXCLUDED.high_watermark"
        )) {
            stmt.setString(1, topicPartition.topic());
            stmt.setInt(2, topicPartition.partition());
            stmt.setLong(3, newHighWatermark);
            stmt.execute();
        }
    }

    private BatchCoordinates findPossibleBatch(final Connection connection,
                                               final TopicPartition topicPartition,
                                               final long offset) throws SQLException {
        try (final PreparedStatement stmt = connection.prepareStatement(
            "SELECT base_offset, file_path, byte_offset, byte_size, number_of_records "
                + "FROM batch_coordinates "
                + "WHERE topic = ? "
                + "  AND partition = ? "
                + "  AND base_offset <= ? "
                + "ORDER BY base_offset DESC "
                + "LIMIT 1"
        )) {
            stmt.setString(1, topicPartition.topic());
            stmt.setInt(2, topicPartition.partition());
            stmt.setLong(3, offset);
            final ResultSet resultSet = stmt.executeQuery();
            if (!resultSet.next()) {
                return null;
            } else {
                return new BatchCoordinates(
                    resultSet.getLong("base_offset"),
                    resultSet.getString("file_path"),
                    resultSet.getInt("byte_offset"),
                    resultSet.getInt("byte_size"),
                    resultSet.getInt("number_of_records")
                );
            }
        }
    }

    private static class BatchCoordinates {
        final long baseOffset;
        final String filePath;
        final int byteOffset;
        final int byteSize;
        final long numberOfRecords;

        private BatchCoordinates(final long baseOffset,
                                 final String filePath,
                                 final int byteOffset,
                                 final int byteSize,
                                 final long numberOfRecords) {
            this.baseOffset = baseOffset;
            this.filePath = filePath;
            this.byteOffset = byteOffset;
            this.byteSize = byteSize;
            this.numberOfRecords = numberOfRecords;
        }

        @Override
        public String toString() {
            return "Batch["
                + "baseOffset=" + this.baseOffset
                + ", filePath=" + this.filePath
                + ", byteOffset=" + this.byteOffset
                + ", byteSize=" + this.byteSize
                + ", numberOfRecords=" + this.numberOfRecords
                + "]";
        }
    }
}
