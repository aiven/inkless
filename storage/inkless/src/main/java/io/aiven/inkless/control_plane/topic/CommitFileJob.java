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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.FileReason;
import io.aiven.inkless.control_plane.FileState;
import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.postgres.JobUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitFileJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitFileJob.class);

    private final Time time;
    private final Connection connection;
    private final GetLogInfoJob getLogInfoJob;
    private final PreparedStatement insertFilePreparedStatement;
    private final PreparedStatement insertBatchPreparedStatement;
    private final PreparedStatement updateHighWatermarkAndByteSizePreparedStatement;

    CommitFileJob(final Time time,
                  final Connection connection,
                  final GetLogInfoJob getLogInfoJob) throws SQLException {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.connection = Objects.requireNonNull(connection, "connection cannot be null");
        this.getLogInfoJob = Objects.requireNonNull(getLogInfoJob, "getLogInfoJob cannot be null");

        this.insertFilePreparedStatement = connection.prepareStatement(
            "INSERT OR ABORT INTO files (object_key, format, reason, state, uploader_broker_id, committed_at, size) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "RETURNING file_id"
        );
        this.insertBatchPreparedStatement = connection.prepareStatement(
            "INSERT INTO batches (magic, topic_id, partition, base_offset, last_offset, file_id, byte_offset, byte_size, timestamp_type, log_append_timestamp, batch_max_timestamp) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "RETURNING batch_id");
        this.updateHighWatermarkAndByteSizePreparedStatement = connection.prepareStatement(
            "UPDATE logs " +
            "SET high_watermark = ?, byte_size = ? " +
            "WHERE topic_id = ? AND partition = ?");
    }

    public List<CommitBatchResponse> call(final String objectKey,
                                          final ObjectFormat format,
                                          final int uploaderBrokerId,
                                          final long fileSize,
                                          final List<CommitBatchRequest> requests,
                                          final Consumer<Long> durationCallback) {
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
        final List<CommitBatchResponse> responses;

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
            }
            responses = requests.stream().map(request -> commitFileForValidRequest(now, fileId, request)).toList();
            connection.commit();
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }

        return responses;
    }


    private CommitBatchResponse commitFileForValidRequest(
        final long now,
        final int fileId,
        final CommitBatchRequest request
    ) {
        final GetLogInfoResponse logInfo = getLogInfoJob.call(
            List.of(new GetLogInfoRequest(request.topicIdPartition().topicId(), request.topicIdPartition().partition())), d -> {}).get(0);
        if (logInfo.errors() == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            return CommitBatchResponse.unknownTopicOrPartition();
        }

        final long firstOffset = logInfo.highWatermark();

        try {
            // TODO support idempotence
            // Update the producer state
            if (request.hasProducerId()) {
    //            final InMemoryControlPlane.LatestProducerState latestProducerState = producers
    //                .computeIfAbsent(topicIdPartition, k -> new TreeMap<>())
    //                .computeIfAbsent(request.producerId(), k -> InMemoryControlPlane.LatestProducerState.empty(request.producerEpoch()));
    //
    //            if (latestProducerState.epoch > request.producerEpoch()) {
    //                LOGGER.warn("Producer request with epoch {} is less than the latest epoch {}. Rejecting request",
    //                    request.producerEpoch(), latestProducerState.epoch);
    //                return CommitBatchResponse.invalidProducerEpoch();
    //            }
    //
    //            if (latestProducerState.lastEntries.isEmpty()) {
    //                if (request.baseSequence() != 0) {
    //                    LOGGER.warn("Producer request with base sequence {} is not 0. Rejecting request", request.baseSequence());
    //                    return CommitBatchResponse.sequenceOutOfOrder(request);
    //                }
    //            } else {
    //                final Optional<InMemoryControlPlane.ProducerStateItem> first = latestProducerState.lastEntries.stream()
    //                    .filter(e -> e.baseSequence() == request.baseSequence() && e.lastSequence() == request.lastSequence())
    //                    .findFirst();
    //                if (first.isPresent()) {
    //                    LOGGER.warn("Producer request with base sequence {} and last sequence {} is a duplicate. Rejecting request",
    //                        request.baseSequence(), request.lastSequence());
    //                    final InMemoryControlPlane.ProducerStateItem batchMetadata = first.get();
    //                    return CommitBatchResponse.ofDuplicate(batchMetadata.assignedOffset(), batchMetadata.batchMaxTimestamp(), logInfo.logStartOffset);
    //                }
    //
    //                final int lastSeq = latestProducerState.lastEntries.getLast().lastSequence();
    //                if (request.baseSequence() - 1 != lastSeq || (lastSeq == Integer.MAX_VALUE && request.baseSequence() != 0)) {
    //                    LOGGER.warn("Producer request with base sequence {} is not the next sequence after the last sequence {}. Rejecting request",
    //                        request.baseSequence(), lastSeq);
    //                    return CommitBatchResponse.sequenceOutOfOrder(request);
    //                }
    //            }
    //
    //            final InMemoryControlPlane.LatestProducerState current;
    //            if (latestProducerState.epoch < request.producerEpoch()) {
    //                current = InMemoryControlPlane.LatestProducerState.empty(request.producerEpoch());
    //            } else {
    //                current = latestProducerState;
    //            }
    //            current.addElement(request.baseSequence(), request.lastSequence(), firstOffset, request.batchMaxTimestamp());
    //
    //            producers.get(topicIdPartition).put(request.producerId(), current);
            }

            final long lastOffset = firstOffset + request.offsetDelta();
            updateHighWatermarkAndByteSizePreparedStatement.clearParameters();
            updateHighWatermarkAndByteSizePreparedStatement.setLong(1, lastOffset + 1);  // high watermark
            updateHighWatermarkAndByteSizePreparedStatement.setInt(2, request.size());  // byte size
            updateHighWatermarkAndByteSizePreparedStatement.setString(3, request.topicIdPartition().topicId().toString());
            updateHighWatermarkAndByteSizePreparedStatement.setInt(4, request.topicIdPartition().partition());
            updateHighWatermarkAndByteSizePreparedStatement.execute();

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
            insertBatchPreparedStatement.execute();

            return CommitBatchResponse.success(firstOffset, now, logInfo.logStartOffset(), request);
        } catch (final SQLException e) {
            throw new RuntimeException();
        }
    }
}
