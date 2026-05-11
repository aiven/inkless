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
import org.jooq.generated.udt.PruneBatchesBelowHighestTieredOffsetResponseV1;
import org.jooq.generated.udt.records.PruneBatchesBelowHighestTieredOffsetRequestV1Record;
import org.jooq.generated.udt.records.PruneBatchesBelowHighestTieredOffsetResponseV1Record;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.PruneDisklessLogsError;
import io.aiven.inkless.control_plane.PruneDisklessLogsRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsResponse;

import static org.jooq.generated.Tables.PRUNE_BATCHES_BELOW_HIGHEST_TIERED_OFFSET_V1;

public class PruneDisklessLogsJob implements Callable<List<PruneDisklessLogsResponse>> {

    private final Time time;
    private final DSLContext jooqCtx;
    private final List<PruneDisklessLogsRequest> requests;
    private final Consumer<Long> durationCallback;

    public PruneDisklessLogsJob(Time time, DSLContext jooqCtx, List<PruneDisklessLogsRequest> requests,
                                Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<PruneDisklessLogsResponse> call() throws Exception {
        if (requests.isEmpty()) {
            return List.of();
        }
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private List<PruneDisklessLogsResponse> runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final Instant now = TimeUtils.now(time);
            final PruneBatchesBelowHighestTieredOffsetRequestV1Record[] jooqRequests = requests.stream().map(r ->
                    new PruneBatchesBelowHighestTieredOffsetRequestV1Record(
                        r.topicIdPartition().topicId(),
                        r.topicIdPartition().partition(),
                        r.highestRemoteOffset()))
                .toArray(PruneBatchesBelowHighestTieredOffsetRequestV1Record[]::new);

            final List<PruneBatchesBelowHighestTieredOffsetResponseV1Record> functionResult = conf.dsl().select(
                    PruneBatchesBelowHighestTieredOffsetResponseV1.TOPIC_ID,
                    PruneBatchesBelowHighestTieredOffsetResponseV1.PARTITION,
                    PruneBatchesBelowHighestTieredOffsetResponseV1.LOG_START_OFFSET,
                    PruneBatchesBelowHighestTieredOffsetResponseV1.ERROR
                ).from(PRUNE_BATCHES_BELOW_HIGHEST_TIERED_OFFSET_V1.call(now, jooqRequests))
                .fetchInto(PruneBatchesBelowHighestTieredOffsetResponseV1Record.class);
            return FunctionResultProcessor.processWithMappingOrder(
                requests,
                functionResult,
                // We don't care about the topic name for the key.
                r -> new TopicIdPartition(r.topicIdPartition().topicId(), r.topicIdPartition().partition(), null),
                r -> new TopicIdPartition(r.getTopicId(), r.getPartition(), null),
                this::responseMapper
            );
        });
    }

    private PruneDisklessLogsResponse responseMapper(final PruneDisklessLogsRequest request,
                                                     final PruneBatchesBelowHighestTieredOffsetResponseV1Record record) {
        final PruneBatchesBelowHighestTieredOffsetErrorV1 pgError = record.getError();
        if (pgError == null) {
            throw new IllegalStateException("prune_batches_below_highest_tiered_offset_v1 returned null error");
        }
        final PruneDisklessLogsError error = switch (pgError) {
            case none -> PruneDisklessLogsError.NONE;
            case unknown_topic_or_partition -> PruneDisklessLogsError.UNKNOWN_TOPIC_OR_PARTITION;
        };
        return new PruneDisklessLogsResponse(
            new TopicIdPartition(record.getTopicId(), record.getPartition(), null),
            record.getLogStartOffset(),
            error);
    }
}
