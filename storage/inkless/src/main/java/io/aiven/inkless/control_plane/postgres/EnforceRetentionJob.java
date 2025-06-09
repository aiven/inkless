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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.EnforceRetentionRequest;
import io.aiven.inkless.control_plane.EnforceRetentionResponse;

import static org.jooq.generated.Tables.ENFORCE_RETENTION_V1;

public class EnforceRetentionJob implements Callable<List<EnforceRetentionResponse>> {
    private final Time time;
    private final DSLContext jooqCtx;
    private final List<EnforceRetentionRequest> requests;
    private final Consumer<Long> durationCallback;

    public EnforceRetentionJob(final Time time,
                               final DSLContext jooqCtx,
                               final List<EnforceRetentionRequest> requests,
                               final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
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
                ).from(ENFORCE_RETENTION_V1.call(
                    now, jooqRequests
                )).fetchInto(EnforceRetentionResponseV1Record.class);
                return processFunctionResult(functionResult);
            } catch (RuntimeException e) {
                throw new ControlPlaneException("Error enforcing retention", e);
            }
        });
    }

    private List<EnforceRetentionResponse> processFunctionResult(
        final List<EnforceRetentionResponseV1Record> functionResult
    ) {
        if (functionResult.size() != requests.size()) {
            throw new RuntimeException(String.format("Expected %d items, returned %d", requests.size(), functionResult.size()));
        }

        // The PG function reorders requests in safe locking order. We're reverting that here.

        // Technically there may be multiple requests for a partition, handle this situation.
        final Map<TopicIdPartition, List<EnforceRetentionResponseV1Record>> resultMap = new HashMap<>();
        for (var record : functionResult) {
            resultMap.computeIfAbsent(
                // We don't care about the topic name.
                new TopicIdPartition(record.getTopicId(), record.getPartition(), null),
                k -> new ArrayList<>()
            ).add(record);
        }

        final List<EnforceRetentionResponse> responses = new ArrayList<>();
        for (final var request : requests) {
            final TopicIdPartition key = new TopicIdPartition(request.topicId(), request.partition(), null);
            final var records = resultMap.get(key);
            if (records == null || records.isEmpty()) {
                throw new RuntimeException("No result found for request: " + request);
            }
            final var record = records.remove(0);
            final EnforceRetentionResponse response;
            if (record.getError() == null) {
                response = EnforceRetentionResponse.success(record.getBatchesDeleted(), record.getBytesDeleted(), record.getLogStartOffset());
            } else {
                response = switch (record.getError()) {
                    case unknown_topic_or_partition ->
                        EnforceRetentionResponse.unknownTopicOrPartition();
                };
            }
            responses.add(response);
        }
        return responses;
    }
}
