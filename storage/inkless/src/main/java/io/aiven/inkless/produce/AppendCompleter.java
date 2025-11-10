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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.aiven.inkless.cache.BatchCoordinateCache;
import io.aiven.inkless.cache.CacheBatchCoordinate;
import io.aiven.inkless.control_plane.CommitBatchResponse;

/**
 * This job is responsible for completing the Append requests generating client responses.
 */
class AppendCompleter {
    private static final Logger LOGGER = LoggerFactory.getLogger(AppendCompleter.class);

    private final ClosedFile file;
    private final BatchCoordinateCache batchCoordinateCache;

    public AppendCompleter(ClosedFile file, BatchCoordinateCache batchCoordinateCache) {
        this.file = file;
        this.batchCoordinateCache = Objects.requireNonNull(batchCoordinateCache, "batchCoordinateCache cannot be null");
    }

    public void finishCommitSuccessfully(List<CommitBatchResponse> commitBatchResponses) {
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

            final CacheBatchCoordinate cacheBatchCoordinate = commitBatchResponse.cacheBatchCoordinate();
            if (cacheBatchCoordinate != null) {
                try {
                    batchCoordinateCache.put(commitBatchRequest.topicIdPartition(), cacheBatchCoordinate);
                } catch (final Exception e) {
                    LOGGER.error("Failed to put batch coordinate into cache", e);
                }
            }

            result.put(
                    commitBatchRequest.topicIdPartition().topicPartition(),
                    partitionResponse(commitBatchResponse)
            );
        }

        for (Map.Entry<Integer, Map<TopicPartition, ProduceResponse.PartitionResponse>> invalidResponses : file.invalidResponseByRequest().entrySet()) {
            resultsPerRequest.computeIfAbsent(invalidResponses.getKey(), ignore -> new HashMap<>()).putAll(invalidResponses.getValue());
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

    public void finishCommitWithError() {
        for (final var entry : file.awaitingFuturesByRequest().entrySet()) {
            final var originalRequest = file.originalRequests().get(entry.getKey());
            final var result = originalRequest.entrySet().stream()
                    .collect(Collectors.toMap(
                            kv -> kv.getKey().topicPartition(),
                            ignore -> new ProduceResponse.PartitionResponse(Errors.KAFKA_STORAGE_ERROR, "Error commiting data")));
            entry.getValue().complete(result);
        }
    }
}
