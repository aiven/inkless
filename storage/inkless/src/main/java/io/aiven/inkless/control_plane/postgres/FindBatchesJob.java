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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.udt.FindBatchesResponseV1;
import org.jooq.generated.udt.records.BatchInfoV1Record;
import org.jooq.generated.udt.records.FindBatchesRequestV1Record;
import org.jooq.generated.udt.records.FindBatchesResponseV1Record;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

import static org.jooq.generated.Tables.FIND_BATCHES_V2;

class FindBatchesJob implements Callable<List<FindBatchResponse>> {
    private final Time time;
    private final DSLContext jooqCtx;
    private final List<FindBatchRequest> requests;
    private final int fetchMaxBytes;
    private final int maxBatchesPerPartition;
    private final Consumer<Long> durationCallback;

    FindBatchesJob(final Time time,
                   final DSLContext jooqCtx,
                   final List<FindBatchRequest> requests,
                   final int fetchMaxBytes,
                   final int maxBatchesPerPartition,
                   final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.fetchMaxBytes = fetchMaxBytes;
        this.maxBatchesPerPartition = maxBatchesPerPartition;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<FindBatchResponse> call() {
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private List<FindBatchResponse> runOnce() {
        if (requests.isEmpty()) {
            return List.of();
        }

        final FindBatchesRequestV1Record[] dbRequests = requests.stream()
            .map(req -> new FindBatchesRequestV1Record(
                req.topicIdPartition().topicId(),
                req.topicIdPartition().partition(),
                req.offset(),
                req.maxPartitionFetchBytes()
            ))
            .toArray(FindBatchesRequestV1Record[]::new);

        return jooqCtx.transactionResult((final Configuration conf) -> {
            try {
                final List<FindBatchesResponseV1Record> dbResponses = conf.dsl().select(
                    FindBatchesResponseV1.TOPIC_ID,
                    FindBatchesResponseV1.PARTITION,
                    FindBatchesResponseV1.LOG_START_OFFSET,
                    FindBatchesResponseV1.HIGH_WATERMARK,
                    FindBatchesResponseV1.BATCHES,
                    FindBatchesResponseV1.ERROR
                ).from(FIND_BATCHES_V2.call(dbRequests, fetchMaxBytes, maxBatchesPerPartition))
                    .fetchInto(FindBatchesResponseV1Record.class);

                if (dbResponses.size() != requests.size()) {
                    throw new ControlPlaneException(
                        "Number of responses from database (" + dbResponses.size() + ")"
                            + " does not match number of requests (" + requests.size() + ")"
                    );
                }

                final List<FindBatchResponse> responses = new ArrayList<>();
                for (int i = 0; i < dbResponses.size(); i++) {
                    final FindBatchesResponseV1Record record = dbResponses.get(i);
                    final FindBatchRequest request = requests.get(i);

                    validateResponse(record, request);

                    var error = record.getError();
                    if (error == null) {
                        responses.add(FindBatchResponse.success(getBatchInfos(record, request), record.getLogStartOffset(), record.getHighWatermark()));
                    } else {
                        final var errorResponse = switch (error) {
                            case offset_out_of_range ->
                                FindBatchResponse.offsetOutOfRange(record.getLogStartOffset(), record.getHighWatermark());
                            case unknown_topic_or_partition -> FindBatchResponse.unknownTopicOrPartition();
                        };
                        responses.add(errorResponse);
                    }
                }
                return responses;

            } catch (RuntimeException e) {
                throw new ControlPlaneException("Error finding batches", e);
            }
        });
    }

    private static List<BatchInfo> getBatchInfos(FindBatchesResponseV1Record record, FindBatchRequest request) {
        final List<BatchInfo> batches = new ArrayList<>();
        if (record.getBatches() != null) {
            for (final BatchInfoV1Record batchInfo : record.getBatches()) {
                var batchMetadata = batchInfo.getBatchMetadata();
                batches.add(new BatchInfo(
                    batchInfo.getBatchId(), batchInfo.getObjectKey(),
                    new BatchMetadata(
                        batchMetadata.getMagic().byteValue(),
                        request.topicIdPartition(),
                        batchMetadata.getByteOffset(),
                        batchMetadata.getByteSize(),
                        batchMetadata.getBaseOffset(),
                        batchMetadata.getLastOffset(),
                        batchMetadata.getLogAppendTimestamp(),
                        batchMetadata.getBatchMaxTimestamp(),
                        batchMetadata.getTimestampType()
                    )
                ));
            }
        }
        return batches;
    }

    private void validateResponse(FindBatchesResponseV1Record record, FindBatchRequest request) {
        // Sanity check to match returned and requested partitions (they should go in order).
        final Uuid requestTopicId = request.topicIdPartition().topicId();
        final int requestPartition = request.topicIdPartition().partition();
        final Uuid resultTopicId = record.getTopicId();
        final int resultPartition = record.get(FindBatchesResponseV1.PARTITION);
        if (!resultTopicId.equals(requestTopicId) || resultPartition != requestPartition) {
            throw new ControlPlaneException(String.format(
                "Returned topic ID or partition doesn't match: expected %s-%d, got %s-%d",
                requestTopicId, requestPartition,
                resultTopicId, resultPartition
            ));
        }
    }
}
