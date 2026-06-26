/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
import org.jooq.generated.udt.AdvanceCrossTierLogStartResponseV1;
import org.jooq.generated.udt.records.AdvanceCrossTierLogStartRequestV1Record;
import org.jooq.generated.udt.records.AdvanceCrossTierLogStartResponseV1Record;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.AdvanceCrossTierLogStartOffsetRequest;
import io.aiven.inkless.control_plane.AdvanceCrossTierLogStartOffsetResponse;
import io.aiven.inkless.control_plane.ControlPlaneException;

import static org.jooq.generated.Tables.ADVANCE_CROSS_TIER_LOG_START_V1;

public class AdvanceCrossTierLogStartOffsetJob implements Callable<List<AdvanceCrossTierLogStartOffsetResponse>> {
    private final Time time;
    private final DSLContext jooqCtx;
    private final List<AdvanceCrossTierLogStartOffsetRequest> requests;
    private final Consumer<Long> durationCallback;

    public AdvanceCrossTierLogStartOffsetJob(final Time time,
                                             final DSLContext jooqCtx,
                                             final List<AdvanceCrossTierLogStartOffsetRequest> requests,
                                             final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<AdvanceCrossTierLogStartOffsetResponse> call() throws Exception {
        if (requests.isEmpty()) {
            return List.of();
        }
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private List<AdvanceCrossTierLogStartOffsetResponse> runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final AdvanceCrossTierLogStartRequestV1Record[] jooqRequests = requests.stream().map(r ->
                new AdvanceCrossTierLogStartRequestV1Record(
                    r.topicId(),
                    r.partition(),
                    r.remoteLogStartOffset()
                )).toArray(AdvanceCrossTierLogStartRequestV1Record[]::new);

            try {
                final List<AdvanceCrossTierLogStartResponseV1Record> functionResult = conf.dsl().select(
                    AdvanceCrossTierLogStartResponseV1.TOPIC_ID,
                    AdvanceCrossTierLogStartResponseV1.PARTITION,
                    AdvanceCrossTierLogStartResponseV1.ERROR,
                    AdvanceCrossTierLogStartResponseV1.REMOTE_LOG_START_OFFSET
                ).from(ADVANCE_CROSS_TIER_LOG_START_V1.call(
                    jooqRequests
                )).fetchInto(AdvanceCrossTierLogStartResponseV1Record.class);
                return FunctionResultProcessor.processWithMappingOrder(
                    requests,
                    functionResult,
                    // We don't care about the topic name for the key.
                    r -> new TopicIdPartition(r.topicId(), r.partition(), null),
                    r -> new TopicIdPartition(r.getTopicId(), r.getPartition(), null),
                    this::responseMapper
                );
            } catch (RuntimeException e) {
                throw new ControlPlaneException("Error advancing cross-tier log start offset", e);
            }
        });
    }

    private AdvanceCrossTierLogStartOffsetResponse responseMapper(final AdvanceCrossTierLogStartOffsetRequest request,
                                                                 final AdvanceCrossTierLogStartResponseV1Record record) {
        if (record.getError() == null) {
            final Long stored = record.getRemoteLogStartOffset();
            return AdvanceCrossTierLogStartOffsetResponse.success(
                stored == null ? AdvanceCrossTierLogStartOffsetResponse.NO_OFFSET : stored);
        } else {
            return switch (record.getError()) {
                case unknown_topic_or_partition ->
                    AdvanceCrossTierLogStartOffsetResponse.unknownTopicOrPartition();
                default ->
                    throw new RuntimeException(String.format("Unknown error '%s' returned for %s-%d",
                        record.getError(), record.getTopicId(), record.getPartition()));
            };
        }
    }
}
