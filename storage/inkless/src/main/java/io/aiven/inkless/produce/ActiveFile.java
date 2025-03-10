// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import io.aiven.inkless.control_plane.CommitBatchRequest;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogValidator;
import org.apache.kafka.storage.internals.log.RecordValidationException;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import io.aiven.inkless.TimeUtils;

import static io.aiven.inkless.produce.UnifiedLog.analyzeAndValidateRecords;
import static io.aiven.inkless.produce.UnifiedLog.trimInvalidBytes;
import static org.apache.kafka.storage.internals.log.UnifiedLog.newValidatorMetricsRecorder;

/**
 * An active file.
 *
 * <p>This class is not thread-safe and is supposed to be protected with a lock at the call site.
 */
class ActiveFile {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActiveFile.class);

    // Different from UnifiedLog, log start offset is set on the ControlPlane side
    // Using an undefined value to fulfill the validations requirements
    public static final int UNKNOWN_LOG_START_OFFSET = -1;
    // for inkless the origin is always client
    public static final AppendOrigin APPEND_ORIGIN = AppendOrigin.CLIENT;
    // Using 0 as for inkless the leader epoch is not used
    public static final int LEADER_EPOCH = LeaderAndIsr.INITIAL_LEADER_EPOCH;

    private final Time time;
    private final Instant start;

    private int requestId = -1;

    private final BatchBuffer buffer;

    private final Map<Integer, Map<TopicIdPartition, MemoryRecords>> originalRequests = new HashMap<>();
    private final Map<Integer, CompletableFuture<Map<TopicPartition, PartitionResponse>>> awaitingFuturesByRequest = new HashMap<>();
    private final Map<Integer, Map<TopicPartition, PartitionResponse>> invalidBatchesByRequest = new HashMap<>();

    private final BrokerTopicStats brokerTopicStats;
    private final LogValidator.MetricsRecorder validatorMetricsRecorder;

    ActiveFile(final Time time,
               final BrokerTopicStats brokerTopicStats) {
        this.buffer = new BatchBuffer();
        this.time = time;
        this.start = TimeUtils.durationMeasurementNow(time);
        this.brokerTopicStats = brokerTopicStats;
        this.validatorMetricsRecorder = newValidatorMetricsRecorder(brokerTopicStats.allTopicsStats());
    }

    // For testing
    ActiveFile(final Time time, final Instant start) {
        this.buffer = new BatchBuffer();
        this.time = time;
        this.start = start;
        this.brokerTopicStats = new BrokerTopicStats();
        this.validatorMetricsRecorder = newValidatorMetricsRecorder(brokerTopicStats.allTopicsStats());
    }

    CompletableFuture<Map<TopicPartition, PartitionResponse>> add(
        final Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
        final Map<String, LogConfig> topicConfigs,
        final RequestLocal requestLocal
    ) {
        Objects.requireNonNull(entriesPerPartition, "entriesPerPartition cannot be null");
        Objects.requireNonNull(topicConfigs, "topicConfigs cannot be null");
        Objects.requireNonNull(requestLocal, "requestLocal cannot be null");

        requestId += 1;
        originalRequests.put(requestId, entriesPerPartition);
        final var invalidBatches = invalidBatchesByRequest.computeIfAbsent(requestId, id -> new HashMap<>());

        for (final var entry : entriesPerPartition.entrySet()) {
            final TopicIdPartition topicIdPartition = entry.getKey();

            final LogConfig config = topicConfigs.get(topicIdPartition.topic());
            if (config == null) {
                throw new IllegalArgumentException("Config not provided for topic " + topicIdPartition.topic());
            }

            // Similar to ReplicaManager#appendToLocalLog
            try {
                brokerTopicStats.topicStats(topicIdPartition.topic()).totalProduceRequestRate().mark();
                brokerTopicStats.allTopicsStats().totalProduceRequestRate().mark();

                final MemoryRecords records = entry.getValue();

                final LogAppendInfo appendInfo = analyzeAndValidateRecords(
                    topicIdPartition.topicPartition(),
                    config,
                    records,
                    UNKNOWN_LOG_START_OFFSET,
                    APPEND_ORIGIN,
                    false,
                    true, // ensures that offsets across batches on the same partition grow monotonically
                    LEADER_EPOCH,
                    brokerTopicStats);

                if (appendInfo.validBytes() <= 0) {
                    // Reply with empty response for empty batches
                    invalidBatches.put(topicIdPartition.topicPartition(), new PartitionResponse(Errors.NONE));
                } else {
                    MemoryRecords validRecords = trimInvalidBytes(topicIdPartition.topicPartition(), records, appendInfo);

                    // Use the requested log start offset as this value will be updated on the Control Plane anyway
                    final PrimitiveRef.LongRef offset = PrimitiveRef.ofLong(appendInfo.firstOffset());
                    final Compression targetCompression = BrokerCompressionType.targetCompression(config.compression, appendInfo.sourceCompression());

                    final LogValidator validator = new LogValidator(
                        validRecords,
                        topicIdPartition.topicPartition(),
                        time,
                        appendInfo.sourceCompression(),
                        targetCompression,
                        config.compact,
                        RecordBatch.CURRENT_MAGIC_VALUE,
                        config.messageTimestampType,
                        config.messageTimestampBeforeMaxMs,
                        config.messageTimestampAfterMaxMs,
                        LEADER_EPOCH,
                        APPEND_ORIGIN
                    );
                    LogValidator.ValidationResult validateAndOffsetAssignResult = validator.validateMessagesAndAssignOffsets(
                        offset,
                        validatorMetricsRecorder,
                        requestLocal.bufferSupplier()
                    );

                    validRecords = validateAndOffsetAssignResult.validatedRecords;
                    // No need to update the appendInfo as it is not passed back

                    // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
                    // format conversion)
                    if (validateAndOffsetAssignResult.messageSizeMaybeChanged) {
                        validRecords.batches().forEach(batch -> {
                            if (batch.sizeInBytes() > config.maxMessageSize()) {
                                // we record the original message set size instead of the trimmed size
                                // to be consistent with pre-compression bytesRejectedRate recording
                                brokerTopicStats.topicStats(topicIdPartition.topicPartition().topic()).bytesRejectedRate().mark(records.sizeInBytes());
                                brokerTopicStats.allTopicsStats().bytesRejectedRate().mark(records.sizeInBytes());
                                throw new RecordTooLargeException("Message batch size is " + batch.sizeInBytes() + " bytes in append to" +
                                    "partition " + topicIdPartition.topicPartition() + " which exceeds the maximum configured size of " + config.maxMessageSize() + ".");
                            }
                        });
                    }

                    // Ignore batch size validation against segment size as it does not apply to inkless

                    // add batches to the buffer
                    validRecords.batches().forEach(batch -> buffer.addBatch(topicIdPartition, batch, requestId));

                    // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
                    brokerTopicStats.topicStats(topicIdPartition.topic()).bytesInRate().mark(records.sizeInBytes());
                    brokerTopicStats.allTopicsStats().bytesInRate().mark(records.sizeInBytes());
                    brokerTopicStats.topicStats(topicIdPartition.topic()).messagesInRate().mark(appendInfo.numMessages());
                    brokerTopicStats.allTopicsStats().messagesInRate().mark(appendInfo.numMessages());
                }
            } catch (RecordBatchTooLargeException | CorruptRecordException | KafkaStorageException e) {
                // NOTE: Failed produce requests metric is not incremented for known exceptions
                // it is supposed to indicate un-expected failures of a broker in handling a produce request
                invalidBatches.put(topicIdPartition.topicPartition(), new PartitionResponse(Errors.forException(e)));
            } catch (RecordValidationException rve) {
                processFailedRecords(topicIdPartition.topicPartition(), rve.invalidException());
                invalidBatches.put(topicIdPartition.topicPartition(),
                    new PartitionResponse(
                        Errors.forException(rve.invalidException()),
                        ProduceResponse.INVALID_OFFSET,
                        RecordBatch.NO_TIMESTAMP,
                        ProduceResponse.INVALID_OFFSET,
                        rve.recordErrors(),
                        rve.getMessage()
                    ));
            } catch (Throwable t) {
                processFailedRecords(topicIdPartition.topicPartition(), t);
                invalidBatches.put(topicIdPartition.topicPartition(), new PartitionResponse(Errors.forException(t)));
            }
        }

        final CompletableFuture<Map<TopicPartition, PartitionResponse>> result = new CompletableFuture<>();
        awaitingFuturesByRequest.put(requestId, result);
        final var appendRequest = new ClosedFile.Entry(entriesPerPartition, List.of(), invalidBatches, result);

        return result;
    }

    private void processFailedRecords(TopicPartition topicPartition, Throwable t) {
        brokerTopicStats.topicStats(topicPartition.topic()).failedProduceRequestRate().mark();
        brokerTopicStats.allTopicsStats().failedProduceRequestRate().mark();
        if (t instanceof InvalidProducerEpochException) {
            LOGGER.info("Error processing append operation on partition {}", topicPartition, t);
        }
        LOGGER.error("Error processing append operation on partition {}", topicPartition, t);
    }

    boolean isEmpty() {
        return requestId < 0;
    }

    int size() {
        return buffer.totalSize();
    }

    ClosedFile close() {
        final BatchBuffer.CloseResult closeResult = buffer.close();
        final List<CommitBatchRequest> commitBatchRequests = closeResult.commitBatchRequests();
        return new ClosedFile(
            start,
            originalRequests.entrySet()
                .stream()
                .map((entry) ->
                    new ClosedFile.Entry(
                        entry.getValue(),
                        commitBatchRequests.stream()
                            .filter(request -> request.requestId() == entry.getKey())
                            .toList(),
                        invalidBatchesByRequest.get(entry.getKey()),
                        awaitingFuturesByRequest.get(entry.getKey())
                    )
                )
                .toList(),
            closeResult.data()
        );
    }
}
