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
import org.jooq.generated.udt.EnforceRetentionResponseV1;
import org.jooq.generated.udt.records.EnforceRetentionRequestV1Record;
import org.jooq.generated.udt.records.EnforceRetentionResponseV1Record;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.EnforceRetentionRequest;
import io.aiven.inkless.control_plane.EnforceRetentionResponse;

import static org.jooq.generated.Tables.ENFORCE_RETENTION_V2;

public class EnforceRetentionJob implements Callable<List<EnforceRetentionResponse>> {
    private final Time time;
    private final DSLContext jooqCtx;
    private final List<EnforceRetentionRequest> requests;
    private final Consumer<Long> durationCallback;
    private final int maxBatchesPerRequest;

    public EnforceRetentionJob(final Time time,
                               final DSLContext jooqCtx,
                               final List<EnforceRetentionRequest> requests,
                               final int maxBatchesPerRequest,
                               final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.maxBatchesPerRequest = maxBatchesPerRequest;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<EnforceRetentionResponse> call() throws Exception {
        if (requests.isEmpty()) {
            return List.of();
        }
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private List<EnforceRetentionResponse> runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final Instant now = TimeUtils.now(time);

            final EnforceRetentionRequestV1Record[] jooqRequests = requests.stream().map(r ->
                new EnforceRetentionRequestV1Record(
                    r.topicId(),
                    r.partition(),
                    r.retentionBytes(),
                    r.retentionMs()
                )).toArray(EnforceRetentionRequestV1Record[]::new);

            try {
                final List<EnforceRetentionResponseV1Record> functionResult = conf.dsl().select(
                    EnforceRetentionResponseV1.TOPIC_ID,
                    EnforceRetentionResponseV1.PARTITION,
                    EnforceRetentionResponseV1.ERROR,
                    EnforceRetentionResponseV1.BATCHES_DELETED,
                    EnforceRetentionResponseV1.BYTES_DELETED,
                    EnforceRetentionResponseV1.LOG_START_OFFSET
                ).from(ENFORCE_RETENTION_V2.call(
                    now, jooqRequests, maxBatchesPerRequest
                )).fetchInto(EnforceRetentionResponseV1Record.class);
                return FunctionResultProcessor.processWithMappingOrder(
                    requests,
                    functionResult,
                    // We don't care about the topic name for the key.
                    r -> new TopicIdPartition(r.topicId(), r.partition(), null),
                    r -> new TopicIdPartition(r.getTopicId(), r.getPartition(), null),
                    this::responseMapper
                );
            } catch (RuntimeException e) {
                throw new ControlPlaneException("Error enforcing retention", e);
            }
        });
    }

    private EnforceRetentionResponse responseMapper(final EnforceRetentionRequest request,
                                                    final EnforceRetentionResponseV1Record record) {
        if (record.getError() == null) {
            return EnforceRetentionResponse.success(record.getBatchesDeleted(), record.getBytesDeleted(), record.getLogStartOffset());
        } else {
            return switch (record.getError()) {
                case unknown_topic_or_partition ->
                    EnforceRetentionResponse.unknownTopicOrPartition();
                default ->
                    throw new RuntimeException(String.format("Unknown error '%s' returned for %s-%d",
                        record.getError(), record.getTopicId(), record.getPartition()));
            };
        }
    }
}
