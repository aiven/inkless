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
import org.jooq.Table;
import org.jooq.generated.udt.CommitBatchResponseV1;
import org.jooq.generated.udt.records.CommitBatchRequestV1Record;
import org.jooq.generated.udt.records.CommitBatchResponseV1Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlaneException;

import static org.jooq.generated.Tables.COMMIT_FILE_V1;
import static org.jooq.generated.Tables.COMMIT_FILE_V2;

class CommitFileJob implements Callable<List<CommitBatchResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitFileJob.class);

    private final Time time;
    private final DSLContext jooqCtx;
    private final String objectKey;
    private final ObjectFormat format;
    private final int uploaderBrokerId;
    private final long fileSize;
    private final List<CommitBatchRequest> requests;
    private final boolean coalesce;
    private final Consumer<Long> durationCallback;

    /**
     * Production entry point: always commits via {@code commit_file_v2}, which coalesces contiguous
     * same-partition batch runs into a single {@code batches} row.
     * {@code commit_file_v1} is retained only for rollback safety and for the coalescing benchmark
     * to measure against; new code must not depend on it and it should be dropped once no supported
     * release calls it.
     */
    CommitFileJob(final Time time,
                  final DSLContext jooqCtx,
                  final String objectKey,
                  final ObjectFormat format,
                  final int uploaderBrokerId,
                  final long fileSize,
                  final List<CommitBatchRequest> requests,
                  final Consumer<Long> durationCallback) {
        this(time, jooqCtx, objectKey, format, uploaderBrokerId, fileSize, requests, true, durationCallback);
    }

    /**
     * Visible for testing/benchmarking: lets callers select the legacy non-coalescing
     * {@code commit_file_v1} ({@code coalesce == false}) to compare against the production
     * {@code commit_file_v2} ({@code coalesce == true}). Production code uses the public constructor above,
     * which always coalesces.
     */
    CommitFileJob(final Time time,
                  final DSLContext jooqCtx,
                  final String objectKey,
                  final ObjectFormat format,
                  final int uploaderBrokerId,
                  final long fileSize,
                  final List<CommitBatchRequest> requests,
                  final boolean coalesce,
                  final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.objectKey = objectKey;
        this.format = format;
        this.uploaderBrokerId = uploaderBrokerId;
        this.fileSize = fileSize;
        this.requests = requests;
        this.coalesce = coalesce;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<CommitBatchResponse> call() {
        if (requests.isEmpty()) {
            return List.of();
        }
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private List<CommitBatchResponse> runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            try {
                final Instant now = TimeUtils.now(time);

                final CommitBatchRequestV1Record[] jooqRequests = requests.stream().map(r ->
                    new CommitBatchRequestV1Record(
                        (short) r.magic(),
                        r.topicIdPartition().topicId(),
                        r.topicIdPartition().partition(),
                        (long) r.byteOffset(),
                        (long) r.size(),
                        r.baseOffset(),
                        r.lastOffset(),
                        r.messageTimestampType(),
                        r.batchMaxTimestamp(),
                        r.producerId(),
                        r.producerEpoch(),
                        r.baseSequence(),
                        r.lastSequence()
                    )
                ).toArray(CommitBatchRequestV1Record[]::new);

                // commit_file_v2 collapses contiguous same-partition runs into fewer batches rows;
                // it returns the same commit_batch_response_v1 set (one response per request, in order),
                // so the projection and result processing below are identical for both versions.
                final Table<?> commitFile = coalesce
                    ? COMMIT_FILE_V2.call(objectKey, (short) format.id, uploaderBrokerId, fileSize, now, jooqRequests)
                    : COMMIT_FILE_V1.call(objectKey, (short) format.id, uploaderBrokerId, fileSize, now, jooqRequests);

                final List<CommitBatchResponseV1Record> functionResult = conf.dsl().select(
                    CommitBatchResponseV1.TOPIC_ID,
                    CommitBatchResponseV1.PARTITION,
                    CommitBatchResponseV1.LOG_START_OFFSET,
                    CommitBatchResponseV1.ASSIGNED_BASE_OFFSET,
                    CommitBatchResponseV1.BATCH_TIMESTAMP,
                    CommitBatchResponseV1.ERROR
                ).from(commitFile).fetchInto(CommitBatchResponseV1Record.class);
                return processFunctionResult(now, functionResult);
            } catch (RuntimeException e) {
                throw new ControlPlaneException("Error committing file", e);
            }
        });
    }

    private List<CommitBatchResponse> processFunctionResult(final Instant now,
                                                            final List<CommitBatchResponseV1Record> functionResult) {
        final List<CommitBatchResponse> responses = new ArrayList<>();
        final Iterator<CommitBatchRequest> iterator = requests.iterator();
        for (final var record : functionResult) {
            if (!iterator.hasNext()) {
                throw new RuntimeException("More records returned than expected");
            }
            final CommitBatchRequest request = iterator.next();

            // Sanity check to match returned and requested partitions (they should go in order). Maybe excessive?
            final Uuid requestTopicId = request.topicIdPartition().topicId();
            final int requestPartition = request.topicIdPartition().partition();
            final Uuid resultTopicId = record.getTopicId();
            final int resultPartition = record.get(CommitBatchResponseV1.PARTITION);
            if (!resultTopicId.equals(requestTopicId) || resultPartition != requestPartition) {
                throw new RuntimeException(String.format(
                    "Returned topic ID or resultPartition doesn't match: expected %s-%d, got %s-%d",
                    requestTopicId, requestPartition,
                    resultTopicId, resultPartition
                ));
            }

            final var response = switch (record.getError()) {
                case none:
                    final long assignedOffset = record.getAssignedBaseOffset();
                    final long logStartOffset = record.getLogStartOffset();
                    yield CommitBatchResponse.success(assignedOffset, now.toEpochMilli(), logStartOffset, objectKey, request);
                case nonexistent_log:
                    yield CommitBatchResponse.unknownTopicOrPartition();
                case invalid_producer_epoch:
                    LOGGER.error("Invalid producer epoch for request: {}", request);
                    yield CommitBatchResponse.invalidProducerEpoch();
                case sequence_out_of_order:
                    LOGGER.error("Sequence out of order for request: {}", request);
                    yield CommitBatchResponse.sequenceOutOfOrder(request);
                case duplicate_batch:
                    LOGGER.debug("Duplicate batch for request: {}", request);
                    yield CommitBatchResponse.ofDuplicate(
                        record.getAssignedBaseOffset(),
                        record.getBatchTimestamp(),
                        record.getLogStartOffset()
                    );
            };

            responses.add(response);
        }

        if (iterator.hasNext()) {
            throw new RuntimeException("Fewer records returned than expected");
        }

        return responses;
    }
}
