/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
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

import org.jooq.DSLContext;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.common.UuidUtil;

class UpdateConsolidatedTieredEndOffsetJob implements Callable<Void> {
    private final Time time;
    private final DSLContext jooqCtx;
    private final Uuid topicId;
    private final int partition;
    private final long endOffset;
    private final Consumer<Long> durationCallback;

    UpdateConsolidatedTieredEndOffsetJob(
        final Time time,
        final DSLContext jooqCtx,
        final Uuid topicId,
        final int partition,
        final long endOffset,
        final Consumer<Long> durationCallback
    ) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.topicId = topicId;
        this.partition = partition;
        this.endOffset = endOffset;
        this.durationCallback = durationCallback;
    }

    @Override
    public Void call() {
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private Void runOnce() {
        final UUID topicIdJava = UuidUtil.toJava(topicId);
        jooqCtx.execute(
            "SELECT update_consolidated_tiered_end_offset_v1(?::uuid, ?::int, ?::bigint)",
            topicIdJava, partition, endOffset
        );
        return null;
    }
}
