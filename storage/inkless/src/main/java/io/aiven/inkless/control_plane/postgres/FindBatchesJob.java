// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;

import static org.jooq.generated.Tables.BATCHES;
import static org.jooq.generated.Tables.FILES;

class FindBatchesJob implements Callable<List<FindBatchResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindBatchesJob.class);

    private final Time time;
    private final DSLContext jooqCtx;
    private final List<FindBatchRequest> requests;
    private final boolean minOneMessage;
    private final int fetchMaxBytes;
    private final Consumer<Long> durationCallback;
    private final Consumer<Long> getLogsDurationCallback;

    FindBatchesJob(final Time time,
                   final DSLContext jooqCtx,
                   final List<FindBatchRequest> requests,
                   final boolean minOneMessage,
                   final int fetchMaxBytes,
                   final Consumer<Long> durationCallback,
                   final Consumer<Long> getLogsDurationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.minOneMessage = minOneMessage;
        this.fetchMaxBytes = fetchMaxBytes;
        this.durationCallback = durationCallback;
        this.getLogsDurationCallback = getLogsDurationCallback;
    }

    @Override
    public List<FindBatchResponse> call() {
        try {
            return runOnce();
        } catch (final Exception e) {
            // TODO add retry with backoff (or not, let the consumers do this?)
            throw new RuntimeException(e);
        }
    }

    private List<FindBatchResponse> runOnce()  {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final DSLContext context = conf.dsl();
            final Map<TopicIdPartition, LogEntity> logInfos = getLogInfos(context);
            final List<FindBatchResponse> result = new ArrayList<>();
            for (final FindBatchRequest request : requests) {
                result.add(
                    findBatchPerPartition(context, request, logInfos.get(request.topicIdPartition()))
                );
            }
            return result;
        });
    }

    private FindBatchResponse findBatchPerPartition(final DSLContext context,
                                                    final FindBatchRequest request,
                                                    final LogEntity logEntity) throws Exception {
        if (logEntity == null) {
            return FindBatchResponse.unknownTopicOrPartition();
        }

        if (request.offset() < 0) {
            LOGGER.debug("Invalid offset {} for {}", request.offset(), request.topicIdPartition());
            return FindBatchResponse.offsetOutOfRange(logEntity.logStartOffset(), logEntity.highWatermark());
        }

        if (request.offset() > logEntity.highWatermark()) {
            return FindBatchResponse.offsetOutOfRange(logEntity.logStartOffset(), logEntity.highWatermark());
        }

        return TimeUtils.measureDurationMs(time, () -> getBatchResponse(context, request, logEntity), durationCallback);
    }

    private FindBatchResponse getBatchResponse(final DSLContext ctx, final FindBatchRequest request, final LogEntity logEntity) {
        final var select = ctx.select(
                BATCHES.BASE_OFFSET,
                BATCHES.LAST_OFFSET,
                FILES.OBJECT_KEY,
                BATCHES.BYTE_OFFSET,
                BATCHES.BYTE_SIZE,
                BATCHES.REQUEST_BASE_OFFSET,
                BATCHES.REQUEST_LAST_OFFSET,
                BATCHES.TIMESTAMP_TYPE,
                BATCHES.LOG_APPEND_TIMESTAMP,
                BATCHES.BATCH_MAX_TIMESTAMP
            ).from(BATCHES)
            .innerJoin(FILES).on(BATCHES.FILE_ID.eq(FILES.FILE_ID))
            .where(BATCHES.TOPIC_ID.eq(request.topicIdPartition().topicId()))
            .and(BATCHES.PARTITION.eq(request.topicIdPartition().partition()))
            .and(BATCHES.LAST_OFFSET.ge(request.offset()))  // offset to find
            .and(BATCHES.LAST_OFFSET.lt(logEntity.highWatermark()))
            .orderBy(BATCHES.BASE_OFFSET);

        final List<BatchInfo> batches = new ArrayList<>();
        long totalSize = 0;
        try (final var cursor = select.fetchSize(1000).fetchLazy()) {
            for (final var record : cursor) {
                final BatchInfo batch = new BatchInfo(
                    record.get(FILES.OBJECT_KEY),
                    record.get(BATCHES.BYTE_OFFSET),
                    record.get(BATCHES.BYTE_SIZE),
                    record.get(BATCHES.BASE_OFFSET),
                    record.get(BATCHES.REQUEST_BASE_OFFSET),
                    record.get(BATCHES.REQUEST_LAST_OFFSET),
                    record.get(BATCHES.LOG_APPEND_TIMESTAMP),
                    record.get(BATCHES.BATCH_MAX_TIMESTAMP),
                    record.get(BATCHES.TIMESTAMP_TYPE)
                );
                batches.add(batch);
                totalSize += batch.size();
                if (totalSize > fetchMaxBytes) {
                    break;
                }

            }
        }
        return FindBatchResponse.success(batches, logEntity.logStartOffset(), logEntity.highWatermark());
    }

    private Map<TopicIdPartition, LogEntity> getLogInfos(final DSLContext context) throws Exception {
        if (requests.isEmpty()) {
            return Map.of();
        }

        final List<TopicIdPartition> tidps = requests.stream()
            .map(FindBatchRequest::topicIdPartition)
            .toList();
        return LogSelectQuery.execute(time, context, tidps, false, getLogsDurationCallback).stream()
            .collect(Collectors.toMap(LogEntity::topicIdPartition, Function.identity()));
    }
}
