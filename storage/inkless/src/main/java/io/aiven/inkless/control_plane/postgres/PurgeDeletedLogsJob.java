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
import org.jooq.generated.udt.PurgeDeletedLogsResponseV1;
import org.jooq.generated.udt.records.PurgeDeletedLogsResponseV1Record;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.PurgeDeletedLogsResponse;

import static org.jooq.generated.Tables.PURGE_DELETED_LOGS_V1;

class PurgeDeletedLogsJob implements Callable<PurgeDeletedLogsResponse> {
    private final Time time;
    private final DSLContext jooqCtx;
    private final int maxBatches;
    private final Consumer<Long> durationCallback;

    PurgeDeletedLogsJob(final Time time,
                        final DSLContext jooqCtx,
                        final int maxBatches,
                        final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.maxBatches = maxBatches;
        this.durationCallback = durationCallback;
    }

    @Override
    public PurgeDeletedLogsResponse call() {
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private PurgeDeletedLogsResponse runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final Instant now = TimeUtils.now(time);
            try {
                final List<PurgeDeletedLogsResponseV1Record> rows = conf.dsl().select(
                    PurgeDeletedLogsResponseV1.BATCHES_DELETED,
                    PurgeDeletedLogsResponseV1.LOGS_PURGED,
                    PurgeDeletedLogsResponseV1.FILES_MARKED,
                    PurgeDeletedLogsResponseV1.MORE_REMAIN
                ).from(PURGE_DELETED_LOGS_V1.call(now, maxBatches))
                    .fetchInto(PurgeDeletedLogsResponseV1Record.class);
                if (rows.size() != 1) {
                    throw new RuntimeException("Expected 1 purge_deleted_logs_v1 response, got " + rows.size());
                }
                final PurgeDeletedLogsResponseV1Record record = rows.get(0);
                return new PurgeDeletedLogsResponse(
                    record.getBatchesDeleted(),
                    record.getLogsPurged(),
                    record.getFilesMarked(),
                    Boolean.TRUE.equals(record.getMoreRemain())
                );
            } catch (final RuntimeException e) {
                throw new ControlPlaneException("Error purging deleted logs", e);
            }
        });
    }
}
