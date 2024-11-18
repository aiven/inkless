// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse;

import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.control_plane.ControlPlane;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The job of committing the already uploaded file to the control plane.
 *
 * <p>If the file was uploaded successfully, commit to the control plane happens. Otherwise, it doesn't.
 */
class FileCommitJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCommitJob.class);

    private final ClosedFile file;
    private final Future<ObjectKey> uploadFuture;
    private final ControlPlane controlPlane;
    private final Consumer<Integer> sizeCallback;

    FileCommitJob(final ClosedFile file,
                  final Future<ObjectKey> uploadFuture,
                  final ControlPlane controlPlane,
                  final Consumer<Integer> sizeCallback) {
        this.file = file;
        this.uploadFuture = uploadFuture;
        this.controlPlane = controlPlane;
        this.sizeCallback = sizeCallback;
    }

    @Override
    public void run() {
        final WaitForFutureResult waitForFutureResult = waitForFuture();

        if (waitForFutureResult.objectKey != null) {
            LOGGER.debug("Uploaded {} successfully, committing", waitForFutureResult.objectKey);
            finishCommitSuccessfully(waitForFutureResult.objectKey);
        } else {
            LOGGER.error("Upload failed", waitForFutureResult.uploadError);
            finishCommitWithError();
        }

        sizeCallback.accept(file.size());
    }

    private WaitForFutureResult waitForFuture() {
        try {
            final ObjectKey objectKey = uploadFuture.get();
            return new WaitForFutureResult(objectKey, null);
        } catch (final ExecutionException e) {
            return new WaitForFutureResult(null, e.getCause());
        } catch (final InterruptedException e) {
            // This is not expected as we try to shut down the executor gracefully.
            LOGGER.error("Interrupted", e);
            throw new RuntimeException(e);
        }
    }

    private record WaitForFutureResult(ObjectKey objectKey,
                                       Throwable uploadError) {
    }

    private void finishCommitSuccessfully(final ObjectKey objectKey) {
        final var commitBatchResponses = controlPlane.commitFile(objectKey, file.commitBatchRequests());
        LOGGER.debug("Committed successfully");

        // Each request must have a response.
        final Map<Integer, Map<TopicPartition, ProduceResponse.PartitionResponse>> resultsPerRequest = file
            .awaitingFuturesByRequest()
            .entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, ignore -> new HashMap<>()));

        for (int i = 0; i < commitBatchResponses.size(); i++) {
            final int requestId = file.requestIds().get(i);
            final var result = resultsPerRequest.computeIfAbsent(requestId, ignore -> new HashMap<>());

            final var commitBatchRequest = file.commitBatchRequests().get(i);
            final var commitBatchResponse = commitBatchResponses.get(i);
            // TODO correct append time and start offset
            result.put(commitBatchRequest.topicPartition(), new ProduceResponse.PartitionResponse(
                commitBatchResponse.errors(), commitBatchResponse.assignedOffset(), -1, -1
            ));
        }

        for (final var entry : file.awaitingFuturesByRequest().entrySet()) {
            final var result = resultsPerRequest.get(entry.getKey());
            entry.getValue().complete(result);
        }
    }

    private void finishCommitWithError() {
        for (final var entry : file.awaitingFuturesByRequest().entrySet()) {
            final var originalRequest = file.originalRequests().get(entry.getKey());
            final var result = originalRequest.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    ignore -> new ProduceResponse.PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")));
            entry.getValue().complete(result);
        }
    }
}
