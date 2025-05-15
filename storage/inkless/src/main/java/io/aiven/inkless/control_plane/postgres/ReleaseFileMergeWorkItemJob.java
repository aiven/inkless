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

import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.Routines;
import org.jooq.generated.udt.records.ReleaseFileMergeWorkItemResponseV1Record;

import java.util.concurrent.Callable;
import java.util.function.Consumer;


public class ReleaseFileMergeWorkItemJob implements Callable<ReleaseFileMergeWorkItemResponseV1Record> {
    private final Time time;
    private final Long workItemId;
    private final DSLContext jooqCtx;
    private final Consumer<Long> durationCallback;

    public ReleaseFileMergeWorkItemJob(final Time time,
                                       final Long workItemId,
                                       final DSLContext jooqCtx,
                                       final Consumer<Long> durationCallback) {
        this.time = time;
        this.workItemId = workItemId;
        this.jooqCtx = jooqCtx;
        this.durationCallback = durationCallback;
    }

    @Override
    public ReleaseFileMergeWorkItemResponseV1Record call() {
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private ReleaseFileMergeWorkItemResponseV1Record runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> Routines.releaseFileMergeWorkItemV1(conf, workItemId));
    }
}
