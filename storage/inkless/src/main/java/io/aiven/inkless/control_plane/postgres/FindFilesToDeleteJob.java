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

import org.jooq.DSLContext;
import org.jooq.SelectLimitStep;
import org.jooq.generated.enums.FileStateT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.FileToDelete;

import static org.jooq.generated.Tables.FILES;

public class FindFilesToDeleteJob implements Callable<List<FileToDelete>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindFilesToDeleteJob.class);

    private final Time time;
    private final DSLContext jooqCtx;
    private final Instant markedBefore;
    private final int limit;
    private final Consumer<Long> durationCallback;

    public FindFilesToDeleteJob(final Time time,
                                final DSLContext jooqCtx,
                                final Instant markedBefore,
                                final int limit,
                                final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.markedBefore = markedBefore;
        this.limit = limit;
        this.durationCallback = durationCallback;
    }

    @Override
    public List<FileToDelete> call() {
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private List<FileToDelete> runOnce() {
        // No ORDER BY: correctness does not need one, since every returned row is actionable and any
        // prefix makes progress. The scan is bounded by files_by_marked_for_deletion_deleting_idx, which
        // covers the grace-period predicate and lets Postgres stop once `limit` rows are found. Walking
        // that index also makes the prefix oldest-first in practice -- a property of the plan, not a
        // guarantee of the contract -- so files whose deletion is never confirmed are the ones re-served.
        final SelectLimitStep<?> select = jooqCtx.select(
                FILES.FILE_ID,
                FILES.OBJECT_KEY,
                FILES.MARKED_FOR_DELETION_AT
            ).from(FILES)
            .where(FILES.STATE.eq(FileStateT.deleting))
            .and(FILES.MARKED_FOR_DELETION_AT.lessThan(markedBefore));
        // limit(0) in jOOQ means "no rows", so unbounded must skip the clause entirely.
        final var fetchResult = (limit > 0 ? select.limit(limit) : select).fetchStream();
        return fetchResult.map(r -> new FileToDelete(
                r.get(FILES.OBJECT_KEY),
                r.get(FILES.MARKED_FOR_DELETION_AT)
            ))
            .toList();
    }
}
