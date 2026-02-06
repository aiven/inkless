/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.inkless.produce;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.RecordValidationStats;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.LeaderHwChange;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogValidator;
import org.apache.kafka.storage.log.metrics.BrokerTopicMetrics;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.kafka.storage.internals.log.LocalLog.UNKNOWN_OFFSET;

// TODO: This method is being migrated to Java and this is a placeholder for when it becomes available
//  on UnifiedLog.java from apache/kafka#19030
class UnifiedLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedLog.class);

    // for diskless the origin is always client
    public static final AppendOrigin APPEND_ORIGIN = AppendOrigin.CLIENT;
    // Using 0 as for diskless the leader epoch is not used
    public static final int LEADER_EPOCH = LeaderAndIsr.INITIAL_LEADER_EPOCH;

    /**
     * Validate the following:
     * <ol>
     * <li> each message matches its CRC
     * <li> each message size is valid (if ignoreRecordSize is false)
     * <li> that the sequence numbers of the incoming record batches are consistent with the existing state and with each other
     * <li> that the offsets are monotonically increasing (if requireOffsetsMonotonic is true)
     * </ol>
     * <p>
     * Also compute the following quantities:
     * <ol>
     * <li> First offset in the message set
     * <li> Last offset in the message set
     * <li> Number of messages
     * <li> Number of valid bytes
     * <li> Whether the offsets are monotonically increasing
     * <li> Whether any compression codec is used (if many are used, then the last one is given)
     * </ol>
     */
    static LogAppendInfo analyzeAndValidateRecords(TopicPartition topicPartition,
                                                   LogConfig config,
                                                   MemoryRecords records,
                                                   long logStartOffset,
                                                   AppendOrigin origin,
                                                   boolean ignoreRecordSize,
                                                   boolean requireOffsetsMonotonic,
                                                   int leaderEpoch,
                                                   BrokerTopicStats brokerTopicStats) {
        int validBytesCount = 0;
        long firstOffset = UNKNOWN_OFFSET;
        long lastOffset = -1L;
        int lastLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
        CompressionType sourceCompression = CompressionType.NONE;
        boolean monotonic = true;
        long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        boolean readFirstMessage = false;
        long lastOffsetOfFirstBatch = -1L;
        boolean skipRemainingBatches = false;

        for (MutableRecordBatch batch : records.batches()) {
            // we only validate V2 and higher to avoid potential compatibility issues with older clients
            if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && batch.baseOffset() != 0) {
                throw new InvalidRecordException("The baseOffset of the record batch in the append to " + topicPartition + " should " +
                    "be 0, but it is " + batch.baseOffset());
            }

            /* During replication of uncommitted data it is possible for the remote replica to send record batches after it lost
             * leadership. This can happen if sending FETCH responses is slow. There is a race between sending the FETCH
             * response and the replica truncating and appending to the log. The replicating replica resolves this issue by only
             * persisting up to the current leader epoch used in the fetch request. See KAFKA-18723 for more details.
             */
            skipRemainingBatches = skipRemainingBatches || hasHigherPartitionLeaderEpoch(batch, origin, leaderEpoch);
            if (skipRemainingBatches) {
                LOGGER.info("Skipping batch {} from an origin of {} because its partition leader epoch {} is higher than the replica's current leader epoch {}",
                    batch, origin, batch.partitionLeaderEpoch(), leaderEpoch);
            } else {
                // update the first offset if on the first message. For magic versions older than 2, we use the last offset
                // to avoid the need to decompress the data (the last offset can be obtained directly from the wrapper message).
                // For magic version 2, we can get the first offset directly from the batch header.
                // When appending to the leader, we will update LogAppendInfo.baseOffset with the correct value. In the follower
                // case, validation will be more lenient.
                // Also indicate whether we have the accurate first offset or not
                if (!readFirstMessage) {
                    if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                        firstOffset = batch.baseOffset();
                    }
                    lastOffsetOfFirstBatch = batch.lastOffset();
                    readFirstMessage = true;
                }

                // check that offsets are monotonically increasing
                if (lastOffset >= batch.lastOffset()) {
                    monotonic = false;
                }

                // update the last offset seen
                lastOffset = batch.lastOffset();
                lastLeaderEpoch = batch.partitionLeaderEpoch();

                // Check if the message sizes are valid.
                int batchSize = batch.sizeInBytes();
                if (!ignoreRecordSize && batchSize > config.maxMessageSize()) {
                    brokerTopicStats.topicStats(topicPartition.topic()).bytesRejectedRate().mark(records.sizeInBytes());
                    brokerTopicStats.allTopicsStats().bytesRejectedRate().mark(records.sizeInBytes());
                    throw new RecordTooLargeException("The record batch size in the append to " + topicPartition + " is " + batchSize + " bytes " +
                        "which exceeds the maximum configured value of " + config.maxMessageSize() + ").");
                }

                // check the validity of the message by checking CRC
                if (!batch.isValid()) {
                    brokerTopicStats.allTopicsStats().invalidMessageCrcRecordsPerSec().mark();
                    throw new CorruptRecordException("Record is corrupt (stored crc = " + batch.checksum() + ") in topic partition " + topicPartition + ".");
                }

                if (batch.maxTimestamp() > maxTimestamp) {
                    maxTimestamp = batch.maxTimestamp();
                }

                validBytesCount += batchSize;

                CompressionType batchCompression = CompressionType.forId(batch.compressionType().id);
                // sourceCompression is only used on the leader path, which only contains one batch if version is v2 or messages are compressed
                if (batchCompression != CompressionType.NONE) {
                    sourceCompression = batchCompression;
                }
            }

            if (requireOffsetsMonotonic && !monotonic) {
                throw new OffsetOutOfRangeException("Out of order offsets found in append to " + topicPartition + ": " +
                    StreamSupport.stream(records.records().spliterator(), false)
                        .map(Record::offset)
                        .map(String::valueOf)
                        .collect(Collectors.joining(",")));
            }
        }
        OptionalInt lastLeaderEpochOpt = (lastLeaderEpoch != RecordBatch.NO_PARTITION_LEADER_EPOCH)
            ? OptionalInt.of(lastLeaderEpoch)
            : OptionalInt.empty();

        return new LogAppendInfo(firstOffset, lastOffset, lastLeaderEpochOpt, maxTimestamp, RecordBatch.NO_TIMESTAMP,
            logStartOffset, RecordValidationStats.EMPTY, sourceCompression,
            validBytesCount, lastOffsetOfFirstBatch, Collections.emptyList(), LeaderHwChange.NONE);
    }

    /**
     * Return true if the record batch has a higher leader epoch than the specified leader epoch
     *
     * @param batch the batch to validate
     * @param origin the reason for appending the record batch
     * @param leaderEpoch the epoch to compare
     * @return true if the append reason is replication and the batch's partition leader epoch is
     *         greater than the specified leaderEpoch, otherwise false
     */
    private static boolean hasHigherPartitionLeaderEpoch(RecordBatch batch, AppendOrigin origin, int leaderEpoch) {
        return origin == AppendOrigin.REPLICATION
            && batch.partitionLeaderEpoch() != RecordBatch.NO_PARTITION_LEADER_EPOCH
            && batch.partitionLeaderEpoch() > leaderEpoch;
    }

    /**
     * Trim any invalid bytes from the end of this message set (if there are any)
     *
     * @param records The records to trim
     * @param info The general information of the message set
     * @return A trimmed message set. This may be the same as what was passed in, or it may not.
     */
    static MemoryRecords trimInvalidBytes(TopicPartition topicPartition, MemoryRecords records, LogAppendInfo info) {
        int validBytes = info.validBytes();
        if (validBytes < 0) {
            throw new CorruptRecordException("Cannot append record batch with illegal length " + validBytes + " to " +
                "log for " + topicPartition + ". A possible cause is a corrupted produce request.");
        }
        if (validBytes == records.sizeInBytes()) {
            return records;
        } else {
            // trim invalid bytes
            ByteBuffer validByteBuffer = records.buffer().duplicate();
            validByteBuffer.limit(validBytes);
            return MemoryRecords.readableRecords(validByteBuffer);
        }
    }


    // Similar to UnifiedLog.append(...)
    static LogAppendInfo validateAndAppendBatch(
        final Time time,
        final int requestId,
        final TopicIdPartition topicIdPartition,
        final LogConfig config,
        final MemoryRecords records,
        final BatchBuffer buffer,
        final Map<TopicPartition, ProduceResponse.PartitionResponse> invalidBatches,
        final RequestLocal requestLocal,
        final BrokerTopicStats brokerTopicStats,
        final LogValidator.MetricsRecorder validatorMetricsRecorder
    ) {
        final LogAppendInfo appendInfo = analyzeAndValidateRecords(
            topicIdPartition.topicPartition(),
            config,
            records,
            UNKNOWN_OFFSET, // set on control-plane, use unknown value to fulfill validation requirements
            APPEND_ORIGIN,
            false,
            true, // ensures that offsets across batches on the same partition grow monotonically
            LEADER_EPOCH,
            brokerTopicStats);

        if (appendInfo.validBytes() <= 0) {
            // Reply with empty response for empty batches
            invalidBatches.put(topicIdPartition.topicPartition(), new ProduceResponse.PartitionResponse(Errors.NONE));
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
            appendInfo.setMaxTimestamp(validateAndOffsetAssignResult.maxTimestampMs);
            appendInfo.setLastOffset(offset.value - 1);
            appendInfo.setRecordValidationStats(validateAndOffsetAssignResult.recordValidationStats);
            if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME) {
                appendInfo.setLogAppendTime(validateAndOffsetAssignResult.logAppendTimeMs);
            }

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

            // Ignore batch size validation against segment size as it does not apply to diskless
            // if (validRecords.sizeInBytes() > config.segmentSize) {
            //     throw new RecordBatchTooLargeException("Message batch size is " + validRecords.sizeInBytes() + " bytes in append " +
            //         "to partition " + topicIdPartition.topicPartition() + ", which exceeds the maximum configured segment size of " + config.segmentSize + ".");
            // }

            // after this point it comes the log-file specific, leader epoch, and duplication checks that are not relevant when appending to diskless
            // as batches go to the buffer instead of the log file
            // and idempotency is checked on the control plane

            // add batches to the buffer
            validRecords.batches().forEach(batch -> buffer.addBatch(topicIdPartition, batch, requestId));
        }
        return appendInfo;
    }

    public static LogValidator.MetricsRecorder newValidatorMetricsRecorder(BrokerTopicMetrics allTopicsStats) {
        return new LogValidator.MetricsRecorder() {
            public void recordInvalidMagic() {
                allTopicsStats.invalidMagicNumberRecordsPerSec().mark();
            }

            public void recordInvalidOffset() {
                allTopicsStats.invalidOffsetOrSequenceRecordsPerSec().mark();
            }

            public void recordInvalidSequence() {
                allTopicsStats.invalidOffsetOrSequenceRecordsPerSec().mark();
            }

            public void recordInvalidChecksums() {
                allTopicsStats.invalidMessageCrcRecordsPerSec().mark();
            }

            public void recordNoKeyCompactedTopic() {
                allTopicsStats.noKeyCompactedTopicRecordsPerSec().mark();
            }
        };
    }
}
