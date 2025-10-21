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

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.control_plane.FileState;
import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.postgres.JobUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteErrorCode;

class CommitFileJob implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitFileJob.class);

    private final Time time;
    private final Connection connection;
    private final GetLogInfoJob getLogInfoJob;
    private final MarkFileForDeletionIfNeededRoutine markFileForDeletionIfNeededRoutine;
    private final PreparedStatement insertFilePreparedStatement;
    private final PreparedStatement checkProducerEpochPreparedStatement;
    private final PreparedStatement getLastSequenceInProducerEpochPreparedStatement;
    private final PreparedStatement getProducerStatePreparedStatement;
    private final PreparedStatement insertProducerStatePreparedStatement;
    private final PreparedStatement keepOnly5ProducerStatesPreparedStatement;
    private final PreparedStatement insertBatchPreparedStatement;
    private final PreparedStatement updateHighWatermarkAndAddByteSizePreparedStatement;

    CommitFileJob(final Time time,
                  final Connection connection,
                  final GetLogInfoJob getLogInfoJob,
                  final MarkFileForDeletionIfNeededRoutine markFileForDeletionIfNeededRoutine) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");
        this.getLogInfoJob = Objects.requireNonNull(getLogInfoJob, "getLogInfoJob cannot be null");
        this.markFileForDeletionIfNeededRoutine = Objects.requireNonNull(markFileForDeletionIfNeededRoutine, "markFileForDeletionIfNeededJob cannot be null");

        this.insertFilePreparedStatement = connection.prepareStatement(
            "INSERT OR ABORT INTO files (object_key, format, reason, state, uploader_broker_id, committed_at, size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "RETURNING file_id"
        );
        this.checkProducerEpochPreparedStatement = connection.prepareStatement(
            "SELECT 1 " +
                "FROM producer_state " +
                "WHERE topic_id = ? " +
                "  AND partition = ? " +
                "  AND producer_id = ? " +
                "  AND producer_epoch > ?"
        );
        this.getLastSequenceInProducerEpochPreparedStatement = connection.prepareStatement(
            "SELECT MAX(last_sequence) AS last_sequence_in_producer_epoch " +
                "FROM producer_state " +
                "WHERE topic_id = ? " +
                "  AND partition = ? " +
                "  AND producer_id = ? " +
                "  AND producer_epoch = ?"
        );
        this.getProducerStatePreparedStatement = connection.prepareStatement(
            "SELECT assigned_offset, batch_max_timestamp " +
                "FROM producer_state " +
                "WHERE topic_id = ? " +
                "  AND partition = ? " +
                "  AND producer_id = ? " +
                "  AND producer_epoch = ? " +
                "  AND base_sequence = ? " +
                "  AND last_sequence = ?"
        );
        this.insertProducerStatePreparedStatement = connection.prepareStatement(
            "INSERT INTO producer_state (topic_id, partition, producer_id, producer_epoch, base_sequence, last_sequence, assigned_offset, batch_max_timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        );
        this.keepOnly5ProducerStatesPreparedStatement = connection.prepareStatement(
            "DELETE FROM producer_state " +
                "WHERE topic_id = ? " +
                "  AND partition = ? " +
                "  AND producer_id = ? " +
                "  AND row_id <= (" +
                "    SELECT row_id " +
                "    FROM producer_state " +
                "    WHERE topic_id = ? " +
                "      AND partition = ? " +
                "      AND producer_id = ? " +
                "    ORDER BY row_id DESC " +
                "    LIMIT 1 " +
                "    OFFSET 5" +
                "  )"
        );
        this.insertBatchPreparedStatement = connection.prepareStatement(
            "INSERT INTO batches (magic, topic_id, partition, base_offset, last_offset, file_id, byte_offset, byte_size, timestamp_type, log_append_timestamp, batch_max_timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "RETURNING batch_id");
        this.updateHighWatermarkAndAddByteSizePreparedStatement = connection.prepareStatement(
            "UPDATE logs " +
            "SET high_watermark = ?, byte_size = byte_size + ? " +
            "WHERE topic_id = ? AND partition = ?");
    }

    public List<CommitBatchResponse> call(final String objectKey,
                                          final ObjectFormat format,
                                          final int uploaderBrokerId,
                                          final long fileSize,
                                          final List<CommitBatchRequest> requests,
                                          final Consumer<Long> durationCallback) {
        Objects.requireNonNull(objectKey, "objectKey cannot be null");
        Objects.requireNonNull(format, "format cannot be null");
        Objects.requireNonNull(requests, "requests cannot be null");
        Objects.requireNonNull(durationCallback, "durationCallback cannot be null");

        if (requests.isEmpty()) {
            return List.of();
        }
        return JobUtils.run(() -> runOnce(objectKey, format, uploaderBrokerId, fileSize, requests), time, durationCallback);
    }

    private List<CommitBatchResponse> runOnce(final String objectKey,
                                              final ObjectFormat format,
                                              final int uploaderBrokerId,
                                              final long fileSize,
                                              final List<CommitBatchRequest> requests) {
        final long now = time.milliseconds();
        final List<CommitBatchResponse> responses = new ArrayList<>();

        try {
            this.insertFilePreparedStatement.clearParameters();
            this.insertFilePreparedStatement.setString(1, objectKey);
            this.insertFilePreparedStatement.setString(2, format.toString());
            this.insertFilePreparedStatement.setString(3, FileReason.PRODUCE.toString());
            this.insertFilePreparedStatement.setString(4, FileState.UPLOADED.toString());
            this.insertFilePreparedStatement.setInt(5, uploaderBrokerId);
            this.insertFilePreparedStatement.setLong(6, now);
            this.insertFilePreparedStatement.setLong(7, fileSize);

            final int fileId;
            try (final ResultSet resultSet = insertFilePreparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new IllegalStateException();
                }
                fileId = resultSet.getInt(1);
            } catch (final org.sqlite.SQLiteException sqlEx) {
                if (sqlEx.getResultCode() == SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE) {
                    throw new ControlPlaneException("Error committing file");
                } else {
                    throw sqlEx;
                }
            }

            for (final CommitBatchRequest request : requests) {
                responses.add(commitFileForValidRequest(now, fileId, request));
            }

            // Mark the file for deletion if after all operations there aren't any batches in it.
            markFileForDeletionIfNeededRoutine.run(now, fileId);

            connection.commit();
        } catch (final Exception e) {
            try {
                connection.rollback();
            } catch (final SQLException sqlEx) {
                LOGGER.error("Error rolling back", sqlEx);
            }
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new RuntimeException(e);
            }
        }

        return responses;
    }


    private CommitBatchResponse commitFileForValidRequest(
        final long now,
        final int fileId,
        final CommitBatchRequest request
    ) throws SQLException {
        final GetLogInfoResponse logInfo = getLogInfoJob.call(
            List.of(new GetLogInfoRequest(request.topicIdPartition().topicId(), request.topicIdPartition().partition())), d -> {}).get(0);
        if (logInfo.errors() == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            return CommitBatchResponse.unknownTopicOrPartition();
        }

        final long firstOffset = logInfo.highWatermark();

        // Update the producer state
        if (request.hasProducerId() && request.hasProducerEpoch()) {
            // If there are previous batches for the producer, check that the producer epoch is not smaller than the last batch.
            checkProducerEpochPreparedStatement.clearParameters();
            checkProducerEpochPreparedStatement.setString(1, request.topicIdPartition().topicId().toString());
            checkProducerEpochPreparedStatement.setInt(2, request.topicIdPartition().partition());
            checkProducerEpochPreparedStatement.setLong(3, request.producerId());
            checkProducerEpochPreparedStatement.setShort(4, request.producerEpoch());
            try (final ResultSet resultSet = checkProducerEpochPreparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    return CommitBatchResponse.invalidProducerEpoch();
                }
            }

            getLastSequenceInProducerEpochPreparedStatement.clearParameters();
            getLastSequenceInProducerEpochPreparedStatement.setString(1, request.topicIdPartition().topicId().toString());
            getLastSequenceInProducerEpochPreparedStatement.setInt(2, request.topicIdPartition().partition());
            getLastSequenceInProducerEpochPreparedStatement.setLong(3, request.producerId());
            getLastSequenceInProducerEpochPreparedStatement.setShort(4, request.producerEpoch());
            try (final ResultSet resultSet = getLastSequenceInProducerEpochPreparedStatement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new RuntimeException();
                }

                if (resultSet.getObject("last_sequence_in_producer_epoch") == null) {
                    // If there are no previous batches for the producer, the base sequence must be 0.
                    if (request.baseSequence() != 0) {
                        LOGGER.warn("Producer request with base sequence {} is not 0. Rejecting request", request.baseSequence());
                        return CommitBatchResponse.sequenceOutOfOrder(request);
                    }
                } else {
                    final int lastSequenceInProducerEpoch = resultSet.getInt("last_sequence_in_producer_epoch");

                    // Check for duplicates.
                    getProducerStatePreparedStatement.clearParameters();
                    getProducerStatePreparedStatement.setString(1, request.topicIdPartition().topicId().toString());
                    getProducerStatePreparedStatement.setInt(2, request.topicIdPartition().partition());
                    getProducerStatePreparedStatement.setLong(3, request.producerId());
                    getProducerStatePreparedStatement.setShort(4, request.producerEpoch());
                    getProducerStatePreparedStatement.setInt(5, request.baseSequence());
                    getProducerStatePreparedStatement.setInt(6, request.lastSequence());
                    try (final ResultSet resultSetProducerState = getProducerStatePreparedStatement.executeQuery()) {
                        if (resultSetProducerState.next()) {
                            final long assignedOffset = resultSetProducerState.getLong("assigned_offset");
                            final long batchMaxTimestamp = resultSetProducerState.getLong("batch_max_timestamp");
                            return CommitBatchResponse.ofDuplicate(assignedOffset, batchMaxTimestamp, logInfo.logStartOffset());
                        }
                    }

                    // Check that the sequence is not out of order.
                    // A sequence is out of order if the base sequence is not a continuation of the last sequence
                    // or, in case of wraparound, the base sequence must be 0 and the last sequence must be 2147483647 (Integer.MAX_VALUE).
                    if (request.baseSequence() - 1 != lastSequenceInProducerEpoch || (lastSequenceInProducerEpoch == Integer.MAX_VALUE && request.baseSequence() != 0)) {
                        LOGGER.warn("Producer request with base sequence {} is not the next sequence after the last sequence {}. Rejecting request",
                            request.baseSequence(), lastSequenceInProducerEpoch);
                        return CommitBatchResponse.sequenceOutOfOrder(request);
                    }
                }

                insertProducerStatePreparedStatement.clearParameters();
                insertProducerStatePreparedStatement.setString(1, request.topicIdPartition().topicId().toString());
                insertProducerStatePreparedStatement.setInt(2, request.topicIdPartition().partition());
                insertProducerStatePreparedStatement.setLong(3, request.producerId());
                insertProducerStatePreparedStatement.setShort(4, request.producerEpoch());
                insertProducerStatePreparedStatement.setInt(5, request.baseSequence());
                insertProducerStatePreparedStatement.setInt(6, request.lastSequence());
                insertProducerStatePreparedStatement.setLong(7, firstOffset);
                insertProducerStatePreparedStatement.setLong(8, request.batchMaxTimestamp());
                insertProducerStatePreparedStatement.executeUpdate();

                keepOnly5ProducerStatesPreparedStatement.clearParameters();
                keepOnly5ProducerStatesPreparedStatement.setString(1, request.topicIdPartition().topicId().toString());
                keepOnly5ProducerStatesPreparedStatement.setInt(2, request.topicIdPartition().partition());
                keepOnly5ProducerStatesPreparedStatement.setLong(3, request.producerId());
                keepOnly5ProducerStatesPreparedStatement.setString(4, request.topicIdPartition().topicId().toString());
                keepOnly5ProducerStatesPreparedStatement.setInt(5, request.topicIdPartition().partition());
                keepOnly5ProducerStatesPreparedStatement.setLong(6, request.producerId());
                keepOnly5ProducerStatesPreparedStatement.executeUpdate();
            }
        }

        final long lastOffset = firstOffset + request.offsetDelta();
        updateHighWatermarkAndAddByteSizePreparedStatement.clearParameters();
        updateHighWatermarkAndAddByteSizePreparedStatement.setLong(1, lastOffset + 1);  // high watermark
        updateHighWatermarkAndAddByteSizePreparedStatement.setInt(2, request.size());  // byte size
        updateHighWatermarkAndAddByteSizePreparedStatement.setString(3, request.topicIdPartition().topicId().toString());
        updateHighWatermarkAndAddByteSizePreparedStatement.setInt(4, request.topicIdPartition().partition());
        updateHighWatermarkAndAddByteSizePreparedStatement.executeUpdate();

        insertBatchPreparedStatement.clearParameters();
        insertBatchPreparedStatement.setByte(1, request.magic());
        insertBatchPreparedStatement.setString(2, request.topicIdPartition().topicId().toString());
        insertBatchPreparedStatement.setInt(3, request.topicIdPartition().partition());
        insertBatchPreparedStatement.setLong(4, firstOffset);
        insertBatchPreparedStatement.setLong(5, lastOffset);
        insertBatchPreparedStatement.setInt(6, fileId);
        insertBatchPreparedStatement.setInt(7, request.byteOffset());
        insertBatchPreparedStatement.setInt(8, request.size());
        insertBatchPreparedStatement.setInt(9, request.messageTimestampType().id);
        insertBatchPreparedStatement.setLong(10, now);
        insertBatchPreparedStatement.setLong(11, request.batchMaxTimestamp());
        try (final ResultSet resultSet = insertBatchPreparedStatement.executeQuery()) {
            if (!resultSet.next()) {
                throw new RuntimeException();
            }
            final long batchId = resultSet.getLong("batch_id");
            System.out.println(batchId);  // use
        }

        return CommitBatchResponse.success(firstOffset, now, logInfo.logStartOffset(), request);
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(insertFilePreparedStatement, "insertFilePreparedStatement");
        Utils.closeQuietly(checkProducerEpochPreparedStatement, "checkProducerEpochPreparedStatement");
        Utils.closeQuietly(getLastSequenceInProducerEpochPreparedStatement, "getLastSequenceInProducerEpochPreparedStatement");
        Utils.closeQuietly(getProducerStatePreparedStatement, "getProducerStatePreparedStatement");
        Utils.closeQuietly(insertProducerStatePreparedStatement, "insertProducerStatePreparedStatement");
        Utils.closeQuietly(keepOnly5ProducerStatesPreparedStatement, "keepOnly5ProducerStatesPreparedStatement");
        Utils.closeQuietly(insertBatchPreparedStatement, "insertBatchPreparedStatement");
        Utils.closeQuietly(updateHighWatermarkAndAddByteSizePreparedStatement, "updateHighWatermarkAndByteSizePreparedStatement");
    }
}
