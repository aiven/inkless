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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record3;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.GetLogInfoResult;

import static org.jooq.generated.Tables.LOGS;

public class GetLogInfoJob implements Callable<GetLogInfoResult> {
    private final Time time;
    private final DSLContext jooqCtx;
    private final Uuid topicId;
    private final int partition;
    private final Consumer<Long> durationCallback;

    public GetLogInfoJob(final Time time,
                         final DSLContext jooqCtx,
                         final Uuid topicId,
                         final int partition,
                         final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.topicId = topicId;
        this.partition = partition;
        this.durationCallback = durationCallback;
    }

    @Override
    public GetLogInfoResult call() throws Exception {
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private GetLogInfoResult runOnce() throws Exception {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final DSLContext context = conf.dsl();

            final var select = context.select(
                    LOGS.LOG_START_OFFSET,
                    LOGS.HIGH_WATERMARK,
                    LOGS.BYTE_SIZE
                ).from(LOGS)
                .where(LOGS.TOPIC_ID.eq(topicId))
                .and(LOGS.PARTITION.eq(partition));
            if (select.execute() == 0) {
                throw new ControlPlaneException(String.format("Topic ID %s partition %d not found", topicId, partition));
            }
            final Record3<Long, Long, Long> record = select.getResult().get(0);
            return new GetLogInfoResult(
                record.getValue(LOGS.LOG_START_OFFSET),
                record.getValue(LOGS.HIGH_WATERMARK),
                record.getValue(LOGS.BYTE_SIZE)
            );
        });
    }
}
