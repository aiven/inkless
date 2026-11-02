/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.enums.PruneBatchesBelowHighestTieredOffsetErrorV1;
import org.jooq.generated.udt.PruneBatchesBelowHighestTieredOffsetResponseV2;
import org.jooq.generated.udt.records.PruneBatchesBelowHighestTieredOffsetRequestV1Record;
import org.jooq.generated.udt.records.PruneBatchesBelowHighestTieredOffsetResponseV2Record;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.PruneDisklessLogsError;
import io.aiven.inkless.control_plane.PruneDisklessLogsRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsResponse;

import static org.jooq.generated.Tables.PRUNE_BATCHES_BELOW_HIGHEST_TIERED_OFFSET_V2;

public class PruneDisklessLogsJob implements Callable<List<PruneDisklessLogsResponse>> {

    private final Time time;
    private final DSLContext jooqCtx;
    private final List<PruneDisklessLogsRequest> requests;
    private final int maxBatchesPerRequest;
    private final Consumer<Long> durationCallback;

    public PruneDisklessLogsJob(Time time, DSLContext jooqCtx, List<PruneDisklessLogsRequest> requests,
                                int maxBatchesPerRequest, Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.maxBatchesPerRequest = maxBatchesPerRequest;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<PruneDisklessLogsResponse> call() throws Exception {
        if (requests.isEmpty()) {
            return List.of();
        }
        final List<PruneDisklessLogsResponse> responses = new ArrayList<>(requests.size());
        for (final PruneDisklessLogsRequest request : requests) {
            responses.add(JobUtils.run(() -> runOnce(request), time, durationCallback));
        }
        return responses;
    }

    private PruneDisklessLogsResponse runOnce(final PruneDisklessLogsRequest request) {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final Instant now = TimeUtils.now(time);
            final PruneBatchesBelowHighestTieredOffsetRequestV1Record[] jooqRequests = {
                new PruneBatchesBelowHighestTieredOffsetRequestV1Record(
                    request.topicIdPartition().topicId(),
                    request.topicIdPartition().partition(),
                    request.highestRemoteOffset())
            };

            final List<PruneBatchesBelowHighestTieredOffsetResponseV2Record> functionResult = conf.dsl().select(
                    PruneBatchesBelowHighestTieredOffsetResponseV2.TOPIC_ID,
                    PruneBatchesBelowHighestTieredOffsetResponseV2.PARTITION,
                    PruneBatchesBelowHighestTieredOffsetResponseV2.LOG_START_OFFSET,
                    PruneBatchesBelowHighestTieredOffsetResponseV2.ERROR,
                    PruneBatchesBelowHighestTieredOffsetResponseV2.BATCHES_DELETED,
                    PruneBatchesBelowHighestTieredOffsetResponseV2.BYTES_DELETED
                ).from(PRUNE_BATCHES_BELOW_HIGHEST_TIERED_OFFSET_V2.call(
                    now, jooqRequests, maxBatchesPerRequest
                )).fetchInto(PruneBatchesBelowHighestTieredOffsetResponseV2Record.class);
            return FunctionResultProcessor.processWithMappingOrder(
                List.of(request),
                functionResult,
                r -> new TopicIdPartition(r.topicIdPartition().topicId(), r.topicIdPartition().partition(), null),
                r -> new TopicIdPartition(r.getTopicId(), r.getPartition(), null),
                this::responseMapper
            ).get(0);
        });
    }

    private PruneDisklessLogsResponse responseMapper(final PruneDisklessLogsRequest request,
                                                     final PruneBatchesBelowHighestTieredOffsetResponseV2Record record) {
        final PruneBatchesBelowHighestTieredOffsetErrorV1 pgError = record.getError();
        if (pgError == null) {
            throw new IllegalStateException("prune_batches_below_highest_tiered_offset_v2 returned null error");
        }
        final PruneDisklessLogsError error = switch (pgError) {
            case none -> PruneDisklessLogsError.NONE;
            case unknown_topic_or_partition -> PruneDisklessLogsError.UNKNOWN_TOPIC_OR_PARTITION;
        };
        return new PruneDisklessLogsResponse(
            new TopicIdPartition(record.getTopicId(), record.getPartition(), request.topicIdPartition().topic()),
            record.getLogStartOffset() == null ? -1L : record.getLogStartOffset(),
            record.getBatchesDeleted() == null ? -1 : record.getBatchesDeleted(),
            record.getBytesDeleted() == null ? -1L : record.getBytesDeleted(),
            error);
    }
}
