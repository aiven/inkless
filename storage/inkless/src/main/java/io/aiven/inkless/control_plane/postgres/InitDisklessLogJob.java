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

import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.udt.InitDisklessLogResponseV1;
import org.jooq.generated.udt.records.InitDisklessLogProducerStateV1Record;
import org.jooq.generated.udt.records.InitDisklessLogRequestV1Record;
import org.jooq.generated.udt.records.InitDisklessLogResponseV1Record;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.control_plane.InvalidDisklessStartOffsetException;
import io.aiven.inkless.control_plane.ProducerStateSnapshot;

import static org.jooq.generated.Tables.INIT_DISKLESS_LOG_V1;

public class InitDisklessLogJob implements Callable<Void> {

    private final Time time;
    private final DSLContext jooqCtx;
    private final Set<InitDisklessLogRequest> requests;
    private final Consumer<Long> durationCallback;

    InitDisklessLogJob(final Time time,
                       final DSLContext jooqCtx,
                       final Set<InitDisklessLogRequest> requests,
                       final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public Void call() {
        if (requests.isEmpty()) {
            return null;
        }
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private Void runOnce() {
        final InitDisklessLogRequestV1Record[] jooqRequests = requests.stream()
            .map(this::toJooqRequest)
            .toArray(InitDisklessLogRequestV1Record[]::new);

        final List<InitDisklessLogResponseV1Record> responses = jooqCtx.transactionResult(
            (final Configuration conf) -> conf.dsl().select(
                InitDisklessLogResponseV1.TOPIC_ID,
                InitDisklessLogResponseV1.PARTITION,
                InitDisklessLogResponseV1.ERROR
            ).from(INIT_DISKLESS_LOG_V1.call(jooqRequests))
            .fetchInto(InitDisklessLogResponseV1Record.class)
        );

        for (final var response : responses) {
            switch (response.getError()) {
                case none -> { }
                case invalid_diskless_start_offset -> throw new InvalidDisklessStartOffsetException(
                    response.getTopicId(),
                    response.getPartition()
                );
            }
        }
        return null;
    }

    private InitDisklessLogRequestV1Record toJooqRequest(final InitDisklessLogRequest request) {
        final InitDisklessLogProducerStateV1Record[] producerState;
        if (request.producerStateEntries() == null || request.producerStateEntries().isEmpty()) {
            producerState = new InitDisklessLogProducerStateV1Record[0];
        } else {
            producerState = request.producerStateEntries().stream()
                .map(this::toJooqProducerState)
                .toArray(InitDisklessLogProducerStateV1Record[]::new);
        }

        return new InitDisklessLogRequestV1Record(
            request.topicId(),
            request.partition(),
            request.topicName(),
            request.logStartOffset(),
            request.disklessStartOffset(),
            request.leaderEpoch(),
            producerState
        );
    }

    private InitDisklessLogProducerStateV1Record toJooqProducerState(final ProducerStateSnapshot entry) {
        return new InitDisklessLogProducerStateV1Record(
            entry.producerId(),
            entry.producerEpoch(),
            entry.baseSequence(),
            entry.lastSequence(),
            entry.assignedOffset(),
            entry.batchMaxTimestamp()
        );
    }
}
