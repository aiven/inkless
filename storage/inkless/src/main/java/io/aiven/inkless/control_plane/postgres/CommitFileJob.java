// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
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
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;

import static org.jooq.generated.Tables.COMMIT_FILE_V1;

class CommitFileJob implements Callable<List<CommitBatchResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommitFileJob.class);

    private final Time time;
    private final DSLContext jooqCtx;
    private final String objectKey;
    private final int uploaderBrokerId;
    private final long fileSize;
    private final List<CommitBatchRequest> requests;
    private final Consumer<Long> durationCallback;

    CommitFileJob(final Time time,
                  final DSLContext jooqCtx,
                  final String objectKey,
                  final int uploaderBrokerId,
                  final long fileSize,
                  final List<CommitBatchRequest> requests,
                  final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.objectKey = objectKey;
        this.uploaderBrokerId = uploaderBrokerId;
        this.fileSize = fileSize;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<CommitBatchResponse> call() {
        if (requests.isEmpty()) {
            return List.of();
        }

        try {
            return TimeUtils.measureDurationMs(time, this::runOnce, durationCallback);
        } catch (final Exception e) {
            // TODO retry with backoff
            throw new RuntimeException(e);
        }
    }

    private List<CommitBatchResponse> runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final Instant now = TimeUtils.now(time);

            final CommitBatchRequestV1Record[] jooqRequests = requests.stream().map(r ->
                new CommitBatchRequestV1Record(
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

            final List<CommitBatchResponseV1Record> functionResult = conf.dsl().select(
                CommitBatchResponseV1.TOPIC_ID,
                CommitBatchResponseV1.PARTITION,
                CommitBatchResponseV1.LOG_START_OFFSET,
                CommitBatchResponseV1.ASSIGNED_BASE_OFFSET,
                CommitBatchResponseV1.BATCH_TIMESTAMP,
                CommitBatchResponseV1.ERROR
            ).from(COMMIT_FILE_V1.call(
                objectKey,
                uploaderBrokerId,
                fileSize,
                now,
                jooqRequests
            )).fetchInto(CommitBatchResponseV1Record.class);
            return processFunctionResult(now, functionResult);
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
                    yield CommitBatchResponse.success(assignedOffset, now.toEpochMilli(), logStartOffset, request);
                case nonexistent_log:
                    yield CommitBatchResponse.unknownTopicOrPartition();
                case invalid_producer_epoch:
                    yield CommitBatchResponse.invalidProducerEpoch();
                case sequence_out_of_order:
                    yield CommitBatchResponse.sequenceOutOfOrder(request);
                case duplicate_batch:
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
