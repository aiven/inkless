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
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.ObjectDeleter;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

/**
 * The job of committing the already uploaded file to the control plane.
 *
 * <p>If the file was uploaded successfully, commit to the control plane happens. Otherwise, it doesn't.
 */
class FileCommitJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCommitJob.class);

    private final int brokerId;
    private final ClosedFile file;
    private final Future<ObjectKey> uploadFuture;
    private final Time time;
    private final ControlPlane controlPlane;
    private final ObjectDeleter objectDeleter;
    private final Consumer<Long> durationCallback;

    FileCommitJob(final int brokerId,
                  final ClosedFile file,
                  final Future<ObjectKey> uploadFuture,
                  final Time time,
                  final ControlPlane controlPlane,
                  final ObjectDeleter objectDeleter,
                  final Consumer<Long> durationCallback) {
        this.brokerId = brokerId;
        this.file = file;
        this.uploadFuture = uploadFuture;
        this.controlPlane = controlPlane;
        this.time = time;
        this.objectDeleter = objectDeleter;
        this.durationCallback = durationCallback;
    }

    @Override
    public void run() {
        final UploadResult uploadResult = waitForUpload();
        TimeUtils.measureDurationMs(time, () -> doCommit(uploadResult), durationCallback);
    }

    private UploadResult waitForUpload() {
        try {
            final ObjectKey objectKey = uploadFuture.get();
            return new UploadResult(objectKey, null);
        } catch (final ExecutionException e) {
            return new UploadResult(null, e.getCause());
        } catch (final InterruptedException e) {
            // This is not expected as we try to shut down the executor gracefully.
            LOGGER.error("Interrupted", e);
            throw new RuntimeException(e);
        }
    }

    private void doCommit(final UploadResult result) {
        if (result.objectKey != null) {
            LOGGER.debug("Uploaded {} successfully, committing", result.objectKey);
            try {
                finishCommitSuccessfully(result.objectKey);
            } catch (final Exception e) {
                LOGGER.error("Error commiting data, attempting to remove the uploaded file {}", result.objectKey, e);
                try {
                    objectDeleter.delete(result.objectKey);
                } catch (final StorageBackendException e2) {
                    LOGGER.error("Error removing the uploaded file {}", result.objectKey, e2);
                }
                finishCommitWithError();
            }
        } else {
            LOGGER.error("Upload failed: {}", result.uploadError.getMessage());
            finishCommitWithError();
        }
    }

    private void finishCommitSuccessfully(final ObjectKey objectKey) {
        final var commitBatchResponses = controlPlane.commitFile(objectKey.value(), brokerId, file.size(), file.commitBatchRequests());
        LOGGER.debug("Committed successfully");

        // Each request must have a response.
        final Map<Integer, Map<TopicPartition, ProduceResponse.PartitionResponse>> resultsPerRequest = file
            .awaitingFuturesByRequest()
            .entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, ignore -> new HashMap<>()));

        for (int i = 0; i < commitBatchResponses.size(); i++) {
            final var commitBatchRequest = file.commitBatchRequests().get(i);
            final var result = resultsPerRequest.computeIfAbsent(commitBatchRequest.requestId(), ignore -> new HashMap<>());
            final var commitBatchResponse = commitBatchResponses.get(i);

            result.put(
                commitBatchRequest.topicIdPartition().topicPartition(),
                partitionResponse(commitBatchResponse)
            );
        }

        for (final var entry : file.awaitingFuturesByRequest().entrySet()) {
            final var result = resultsPerRequest.get(entry.getKey());
            entry.getValue().complete(result);
        }
    }

    private static ProduceResponse.PartitionResponse partitionResponse(CommitBatchResponse response) {
        // Producer expects logAppendTime to be NO_TIMESTAMP if the message timestamp type is CREATE_TIME.
        final long logAppendTime;
        if (response.request() != null) {
            logAppendTime = response.request().messageTimestampType() == TimestampType.LOG_APPEND_TIME
                ? response.logAppendTime()
                : RecordBatch.NO_TIMESTAMP;
        } else {
            logAppendTime = RecordBatch.NO_TIMESTAMP;
        }
        return new ProduceResponse.PartitionResponse(
            response.errors(),
            response.assignedBaseOffset(),
            logAppendTime,
            response.logStartOffset()
        );
    }

    private void finishCommitWithError() {
        for (final var entry : file.awaitingFuturesByRequest().entrySet()) {
            final var originalRequest = file.originalRequests().get(entry.getKey());
            final var result = originalRequest.entrySet().stream()
                .collect(Collectors.toMap(
                    kv -> kv.getKey().topicPartition(),
                    ignore -> new ProduceResponse.PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")));
            entry.getValue().complete(result);
        }
    }

    private record UploadResult(ObjectKey objectKey, Throwable uploadError) {
    }
}
