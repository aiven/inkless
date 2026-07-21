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
import org.jooq.generated.udt.RepairDisklessLogResponseV1;
import org.jooq.generated.udt.records.RepairDisklessLogRequestV1Record;
import org.jooq.generated.udt.records.RepairDisklessLogResponseV1Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.RepairDisklessLogRequest;
import io.aiven.inkless.control_plane.RepairDisklessLogResponse;

import static org.jooq.generated.Tables.REPAIR_DISKLESS_LOG_V1;

class RepairDisklessLogJob implements Callable<List<RepairDisklessLogResponse>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RepairDisklessLogJob.class);

    private final Time time;
    private final DSLContext jooqCtx;
    private final List<RepairDisklessLogRequest> requests;
    private final Consumer<Long> durationCallback;

    RepairDisklessLogJob(final Time time,
                         final DSLContext jooqCtx,
                         final List<RepairDisklessLogRequest> requests,
                         final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<RepairDisklessLogResponse> call() {
        if (requests.isEmpty()) {
            return List.of();
        }
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private List<RepairDisklessLogResponse> runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            try {
                final RepairDisklessLogRequestV1Record[] jooqRequests = requests.stream().map(r ->
                    new RepairDisklessLogRequestV1Record(
                        r.topicId(),
                        r.partition(),
                        r.topicName(),
                        r.disklessStartOffset()
                    )
                ).toArray(RepairDisklessLogRequestV1Record[]::new);

                final List<RepairDisklessLogResponseV1Record> functionResult = conf.dsl().select(
                    RepairDisklessLogResponseV1.TOPIC_ID,
                    RepairDisklessLogResponseV1.PARTITION,
                    RepairDisklessLogResponseV1.FOUND
                ).from(REPAIR_DISKLESS_LOG_V1.call(
                    jooqRequests
                )).fetchInto(RepairDisklessLogResponseV1Record.class);

                return FunctionResultProcessor.processWithMappingOrder(
                    requests,
                    functionResult,
                    r -> new TopicIdPartition(r.topicId(), r.partition(), null),
                    r -> new TopicIdPartition(r.getTopicId(), r.getPartition(), null),
                    this::responseMapper
                );
            } catch (RuntimeException e) {
                throw new ControlPlaneException("Error repairing diskless log", e);
            }
        });
    }

    private RepairDisklessLogResponse responseMapper(final RepairDisklessLogRequest request,
                                                     final RepairDisklessLogResponseV1Record record) {
        final boolean found = Boolean.TRUE.equals(record.getFound());
        if (found) {
            LOGGER.info("Repaired diskless log for topic {} partition {}: diskless_start_offset={}",
                request.topicId(), request.partition(), request.disklessStartOffset());
        } else {
            LOGGER.warn("Repair skipped for topic {} partition {}: no control-plane log entry found",
                request.topicId(), request.partition());
        }
        return new RepairDisklessLogResponse(found);
    }
}
