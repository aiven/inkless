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

import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static org.jooq.generated.tables.Logs.LOGS;

/**
 * Reads the raw cross-tier (remote) log start offset for a single partition, null-aware.
 *
 * <p>Returns empty when the partition is unknown or {@code remote_log_start_offset} is still NULL
 * (not yet reported by the classic leader). Deliberately does not fall back to {@code log_start_offset}.
 */
public class GetCrossTierLogStartJob implements Callable<OptionalLong> {
    private final Time time;
    private final DSLContext jooqCtx;
    private final TopicIdPartition topicIdPartition;
    private final Consumer<Long> durationCallback;

    public GetCrossTierLogStartJob(final Time time,
                                   final DSLContext jooqCtx,
                                   final TopicIdPartition topicIdPartition,
                                   final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.topicIdPartition = topicIdPartition;
        this.durationCallback = durationCallback;
    }

    @Override
    public OptionalLong call() {
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private OptionalLong runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final Long value = conf.dsl()
                .select(LOGS.REMOTE_LOG_START_OFFSET)
                .from(LOGS)
                .where(LOGS.TOPIC_ID.eq(topicIdPartition.topicId()))
                .and(LOGS.PARTITION.eq(topicIdPartition.partition()))
                .fetchOne(LOGS.REMOTE_LOG_START_OFFSET);
            return value == null ? OptionalLong.empty() : OptionalLong.of(value);
        });
    }
}
