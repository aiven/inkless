package io.aiven.inkless.writer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.AbstractIterator;

import io.aiven.inkless.common.CommitFileRequest;
import io.aiven.inkless.common.CommitFileResponse;
import io.aiven.inkless.control_plane.ControlPlane;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class InklessWriter {
    private static final Logger logger = LoggerFactory.getLogger(InklessWriter.class);

    private final ControlPlane controlPlane;
    private final S3Client s3Client;

    private final AtomicLong requestCounter = new AtomicLong(0);

    private final Object lock = new Object();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private boolean tickScheduled = false;
    private BatchBuffer batchBuffer = null;
    private Map<Long, CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>>> responseFutures =
        new HashMap<>();

    public InklessWriter(final ControlPlane controlPlane, final S3Client s3Client) {
        this.controlPlane = controlPlane;
        this.s3Client = s3Client;
    }

    public CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> write(
        final Map<TopicPartition, MemoryRecords> entriesPerPartition
    ) {
        synchronized (lock) {
            if (entriesPerPartition.isEmpty()) {
                return CompletableFuture.completedFuture(new HashMap<>());
            }


            if (!tickScheduled) {
                logger.error("Scheduling next tick");
                batchBuffer = new BatchBuffer();
                scheduler.schedule(this::tick, 100, TimeUnit.MILLISECONDS);
                tickScheduled = true;
            }

            final long requestTracker = requestCounter.getAndIncrement();
            for (final Map.Entry<TopicPartition, MemoryRecords> e : entriesPerPartition.entrySet()) {
                final MemoryRecords records = e.getValue();
                final AbstractIterator<MutableRecordBatch> batchIterator = records.batchIterator();
                while (batchIterator.hasNext()) {
                    batchBuffer.addBatch(e.getKey(), batchIterator.next(), requestTracker);
                }
            }

            final CompletableFuture<Map<TopicPartition, ProduceResponse.PartitionResponse>> future = new CompletableFuture<>();
            responseFutures.put(requestTracker, future);
            return future;
        }
    }

    private void tick() {
        synchronized (lock) {
            logger.error("Tick");

            if (batchBuffer == null) {
                return;
            }

            final BatchBuffer.CloseResult closeResult = batchBuffer.close();
            final String filePath = writeDataFile(closeResult.data);
            // Commit file to the control plane.
            final CommitFileResponse response = controlPlane.commitFile(
                new CommitFileRequest(filePath, closeResult.batches));

            // Match assigned offset to waiting futures.
            // TODO double check correctness (e.g. when request comes, save it's expected topic-partitions).
            if (response.assignedOffsets.size() != closeResult.batches.size()) {
                throw new RuntimeException();
            }
            for (long requestTracker : responseFutures.keySet()) {
                final Map<TopicPartition, ProduceResponse.PartitionResponse> result = new HashMap<>();

                // TODO optimize this
                for (int i = 0; i < response.assignedOffsets.size(); i++) {
                    final CommitFileRequest.Batch batch = closeResult.batches.get(i);
                    if (batch.requestTracker != requestTracker) {
                        continue;
                    }
                    final Long assignedOffset = response.assignedOffsets.get(i);
                    final ProduceResponse.PartitionResponse partitionResponse = new ProduceResponse.PartitionResponse(
                        Errors.NONE, assignedOffset, 0, 0);
                    result.put(batch.topicPartition, partitionResponse);
                }

                responseFutures.get(requestTracker).complete(result);
            }

            tickScheduled = false;
            responseFutures = new HashMap<>();
            batchBuffer = null;
        }
    }

    private String writeDataFile(final byte[] data) {
        final String path = Uuid.randomUuid().toString();
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder()
            .key(path)
            .bucket("my-bucket")
            .build();
        final RequestBody body = RequestBody.fromBytes(data);
        s3Client.putObject(putObjectRequest, body);
        return path;
    }
}
