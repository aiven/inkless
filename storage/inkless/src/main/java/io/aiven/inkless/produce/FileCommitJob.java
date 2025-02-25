// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.control_plane.CommitBatchRequestContext;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.ControlPlaneException;
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
            boolean allFuturesCompleted = true;
            try {
                allFuturesCompleted = finishCommitSuccessfully(result.objectKey);
            } catch (final Exception e) {
                LOGGER.error("Error commiting data, attempting to remove the uploaded file {}", result.objectKey, e);
                try {
                    objectDeleter.delete(result.objectKey);
                } catch (final StorageBackendException e2) {
                    LOGGER.error("Error removing the uploaded file {}", result.objectKey, e2);
                }
                finishCommitWithError();
            }
            if (!allFuturesCompleted) {
                throw new ControlPlaneException("Not all futures were completed by commit job for file %s".formatted(file));
            }
        } else {
            LOGGER.error("Upload failed: {}", result.uploadError.getMessage());
            finishCommitWithError();
        }
    }

    private boolean finishCommitSuccessfully(final ObjectKey objectKey) {
        final var requestContexts = file.commitBatchRequestContexts();
        final var requests = requestContexts.stream().map(CommitBatchRequestContext::request).toList();
        final var commitBatchResponses = controlPlane.commitFile(objectKey.value(), brokerId, file.size(), requests);
        LOGGER.debug("Committed successfully");

        // All validated futures must be completed with a response built when commiting the file.
        boolean allCompleted = true;
        for (int i = 0; i < commitBatchResponses.size(); i++) {
            final var requestCtx = requestContexts.get(i);
            final var response = commitBatchResponses.get(i);

            final var future = file.validatedFutures().get(requestCtx.requestId()).get(requestCtx.topicPartition());
            final var completed = future.complete(partitionResponse(response));
            if (!completed) {
                // This should not happen, but if it does, it's a bug.
                // Completing all futures before throwing an exception is important to avoid leaving the futures uncompleted.
                LOGGER.error("The future for this request {} topic-partition {} is already completed", requestCtx.requestId(), requestCtx.topicPartition());
                allCompleted = false;
            }
        }

        return allCompleted;
    }

    private static PartitionResponse partitionResponse(CommitBatchResponse response) {
        // Producer expects logAppendTime to be NO_TIMESTAMP if the message timestamp type is CREATE_TIME.
        final long logAppendTime;
        if (response.request() != null) {
            logAppendTime = response.request().messageTimestampType() == TimestampType.LOG_APPEND_TIME
                ? response.logAppendTime()
                : RecordBatch.NO_TIMESTAMP;
        } else {
            logAppendTime = RecordBatch.NO_TIMESTAMP;
        }
        return new PartitionResponse(
            response.errors(),
            response.assignedBaseOffset(),
            logAppendTime,
            response.logStartOffset()
        );
    }

    private void finishCommitWithError() {
        for (final var entry : file.validatedFutures().entrySet()) {
            for (final var inner : entry.getValue().entrySet()) {
                final var errorResponse = new PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data");
                inner.getValue().complete(errorResponse);
            }
        }
    }

    private record UploadResult(ObjectKey objectKey, Throwable uploadError) {
    }
}
