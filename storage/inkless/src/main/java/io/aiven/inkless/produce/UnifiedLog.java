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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.PrimitiveRef;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.LeaderAndIsr;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.server.record.BrokerCompressionType;
import org.apache.kafka.storage.internals.log.AppendOrigin;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogValidator;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static org.apache.kafka.storage.internals.log.UnifiedLog.UNKNOWN_OFFSET;
import static org.apache.kafka.storage.internals.log.UnifiedLog.analyzeAndValidateRecords;
import static org.apache.kafka.storage.internals.log.UnifiedLog.trimInvalidBytes;

// Stub to keep code that may eventually be moved to upstream UnifiedLog
class UnifiedLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(UnifiedLog.class);

    // for inkless the origin is always client
    public static final AppendOrigin APPEND_ORIGIN = AppendOrigin.CLIENT;
    // Using 0 as for inkless the leader epoch is not used
    public static final int LEADER_EPOCH = LeaderAndIsr.INITIAL_LEADER_EPOCH;

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
            LOGGER,
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

            // Ignore batch size validation against segment size as it does not apply to inkless
            // if (validRecords.sizeInBytes() > config.segmentSize) {
            //     throw new RecordBatchTooLargeException("Message batch size is " + validRecords.sizeInBytes() + " bytes in append " +
            //         "to partition " + topicIdPartition.topicPartition() + ", which exceeds the maximum configured segment size of " + config.segmentSize + ".");
            // }

            // after this point it comes the log-file specific, leader epoch, and duplication checks that are not relevant when appending to inkless
            // as batches go to the buffer instead of the log file
            // and idempotency is checked on the control plane

            // add batches to the buffer
            validRecords.batches().forEach(batch -> buffer.addBatch(topicIdPartition, batch, requestId));
        }
        return appendInfo;
    }

}
