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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.udt.InitDisklessLogResponseV1;
import org.jooq.generated.udt.records.InitDisklessLogProducerStateV1Record;
import org.jooq.generated.udt.records.InitDisklessLogRequestV1Record;
import org.jooq.generated.udt.records.InitDisklessLogResponseV1Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.InitDisklessLogProducerState;
import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.control_plane.InitDisklessLogResponse;

import static org.jooq.generated.Tables.INIT_DISKLESS_LOG_V1;

class InitDisklessLogJob implements Callable<List<InitDisklessLogResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(InitDisklessLogJob.class);

    private final Time time;
    private final DSLContext jooqCtx;
    private final List<InitDisklessLogRequest> requests;
    private final Consumer<Long> durationCallback;

    InitDisklessLogJob(final Time time,
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
        return jooqCtx.transactionResult((final Configuration conf) -> {
            try {
                final InitDisklessLogRequestV1Record[] jooqRequests = requests.stream().map(r ->
                    new InitDisklessLogRequestV1Record(
                        r.topicId(),
                        r.partition(),
                        r.topicName(),
                        r.logStartOffset(),
                        r.disklessStartOffset()
                    )
                ).toArray(InitDisklessLogRequestV1Record[]::new);

                final List<InitDisklessLogProducerStateV1Record> producerStatesList = new ArrayList<>();
                for (final InitDisklessLogRequest request : requests) {
                    if (request.producerStates() != null) {
                        for (final InitDisklessLogProducerState ps : request.producerStates()) {
                            producerStatesList.add(new InitDisklessLogProducerStateV1Record(
                                request.topicId(),
                                request.partition(),
                                ps.producerId(),
                                ps.producerEpoch(),
                                ps.baseSequence(),
                                ps.lastSequence(),
                                ps.assignedOffset(),
                                ps.batchMaxTimestamp()
                            ));
                        }
                    }
                }
                final InitDisklessLogProducerStateV1Record[] jooqProducerStates =
                    producerStatesList.toArray(InitDisklessLogProducerStateV1Record[]::new);

                final List<InitDisklessLogResponseV1Record> functionResult = conf.dsl().select(
                    InitDisklessLogResponseV1.TOPIC_ID,
                    InitDisklessLogResponseV1.PARTITION,
                    InitDisklessLogResponseV1.ERROR
                ).from(INIT_DISKLESS_LOG_V1.call(
                    jooqRequests,
                    jooqProducerStates
                )).fetchInto(InitDisklessLogResponseV1Record.class);

                return processFunctionResult(functionResult);
            } catch (RuntimeException e) {
                throw new ControlPlaneException("Error initializing diskless log", e);
            }
        });
    }

    private List<InitDisklessLogResponse> processFunctionResult(
        final List<InitDisklessLogResponseV1Record> functionResult
    ) {
        final List<InitDisklessLogResponse> responses = new ArrayList<>();
        final Iterator<InitDisklessLogRequest> iterator = requests.iterator();
        for (final var record : functionResult) {
            if (!iterator.hasNext()) {
                throw new RuntimeException("More records returned than expected");
            }
            final InitDisklessLogRequest request = iterator.next();

            final Uuid requestTopicId = request.topicId();
            final int requestPartition = request.partition();
            final Uuid resultTopicId = record.getTopicId();
            final int resultPartition = record.getPartition();
            if (!resultTopicId.equals(requestTopicId) || resultPartition != requestPartition) {
                throw new RuntimeException(String.format(
                    "Returned topic ID or partition doesn't match: expected %s-%d, got %s-%d",
                    requestTopicId, requestPartition,
                    resultTopicId, resultPartition
                ));
            }

            final var response = switch (record.getError()) {
                case none -> {
                    LOGGER.info("Initialized diskless log for topic {} partition {} with offset {}",
                        requestTopicId, requestPartition, request.disklessStartOffset());
                    yield InitDisklessLogResponse.success();
                }
                case already_initialized -> {
                    LOGGER.warn("initDisklessLog for topic {} partition {} rejected: already initialized",
                        requestTopicId, requestPartition);
                    yield InitDisklessLogResponse.alreadyInitialized();
                }
            };
            responses.add(response);
        }

        if (iterator.hasNext()) {
            throw new RuntimeException("Fewer records returned than expected");
        }

        return responses;
    }
}
