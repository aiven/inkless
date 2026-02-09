/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
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
import org.jooq.generated.enums.InitDisklessLogResponseErrorV1;
import org.jooq.generated.udt.InitDisklessLogResponseV1;
import org.jooq.generated.udt.records.InitDisklessLogRequestV1Record;
import org.jooq.generated.udt.records.InitDisklessLogResponseV1Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.control_plane.InitDisklessLogResponse;

import static org.jooq.generated.Tables.INIT_DISKLESS_LOG_V1;

public class InitDisklessLogJob implements Callable<List<InitDisklessLogResponse>> {
    private final Time time;
    private final DSLContext jooqCtx;
    private final List<InitDisklessLogRequest> requests;
    private final Consumer<Long> durationCallback;

    public InitDisklessLogJob(final Time time,
                              final DSLContext jooqCtx,
                              final List<InitDisklessLogRequest> requests,
                              final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<InitDisklessLogResponse> call() {
        if (requests.isEmpty()) {
            return List.of();
        }
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private List<InitDisklessLogResponse> runOnce() {
        final List<InitDisklessLogRequest> validRequests = new ArrayList<>(requests.size());
        final List<Integer> validIndexes = new ArrayList<>(requests.size());
        final InitDisklessLogResponse[] responses = new InitDisklessLogResponse[requests.size()];

        for (int i = 0; i < requests.size(); i++) {
            final InitDisklessLogRequest request = requests.get(i);
            if (request.disklessStartOffset() < 0) {
                responses[i] = InitDisklessLogResponse.invalidRequest();
                continue;
            }
            validRequests.add(request);
            validIndexes.add(i);
        }

        if (validRequests.isEmpty()) {
            return Arrays.asList(responses);
        }

        final List<InitDisklessLogResponse> validResponses = jooqCtx.transactionResult((final Configuration conf) -> {
            try {
                final InitDisklessLogRequestV1Record[] jooqRequests = new InitDisklessLogRequestV1Record[validRequests.size()];
                for (int i = 0; i < validRequests.size(); i++) {
                    final InitDisklessLogRequest request = validRequests.get(i);
                    jooqRequests[i] = new InitDisklessLogRequestV1Record(
                        request.topicId(),
                        request.partition(),
                        request.disklessStartOffset()
                    );
                }

                final List<InitDisklessLogResponseV1Record> functionResult = conf.dsl().select(
                    InitDisklessLogResponseV1.TOPIC_ID,
                    InitDisklessLogResponseV1.PARTITION,
                    InitDisklessLogResponseV1.ERROR,
                    InitDisklessLogResponseV1.DISKLESS_START_OFFSET
                ).from(INIT_DISKLESS_LOG_V1.call(jooqRequests))
                    .fetchInto(InitDisklessLogResponseV1Record.class);

                return FunctionResultProcessor.processWithMappingOrder(
                    validRequests,
                    functionResult,
                    r -> new TopicIdPartition(r.topicId(), r.partition(), null),
                    r -> new TopicIdPartition(r.getTopicId(), r.getPartition(), null),
                    this::responseMapper
                );
            } catch (RuntimeException e) {
                throw new ControlPlaneException("Error initializing diskless log", e);
            }
        });

        for (int i = 0; i < validResponses.size(); i++) {
            responses[validIndexes.get(i)] = validResponses.get(i);
        }

        return Arrays.asList(responses);
    }

    private InitDisklessLogResponse responseMapper(final InitDisklessLogRequest request,
                                                   final InitDisklessLogResponseV1Record record) {
        final InitDisklessLogResponseErrorV1 error = record.getError();
        if (error == null || error == InitDisklessLogResponseErrorV1.none) {
            return InitDisklessLogResponse.success(record.getDisklessStartOffset());
        }
        return switch (error) {
            case invalid_request -> InitDisklessLogResponse.invalidRequest();
            case conflicting_start_offset -> InitDisklessLogResponse.conflictingStartOffset(
                record.getDisklessStartOffset() == null
                    ? InitDisklessLogResponse.INVALID_OFFSET
                    : record.getDisklessStartOffset()
            );
            case none -> InitDisklessLogResponse.success(record.getDisklessStartOffset());
            default -> InitDisklessLogResponse.invalidRequest();
        };
    }
}
