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

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.Routines;
import org.jooq.generated.udt.records.ReleaseFileMergeWorkItemResponseV1Record;

import java.util.concurrent.Callable;


public class ReleaseFileMergeWorkItemJob implements Callable<ReleaseFileMergeWorkItemResponseV1Record> {
    private final Long workItemId;
    private final DSLContext jooqCtx;

    public ReleaseFileMergeWorkItemJob(Long workItemId, DSLContext jooqCtx) {
        this.workItemId = workItemId;
        this.jooqCtx = jooqCtx;
    }

    @Override
    public ReleaseFileMergeWorkItemResponseV1Record call() {
        return JobUtils.run(this::runOnce);
    }

    private ReleaseFileMergeWorkItemResponseV1Record runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> Routines.releaseFileMergeWorkItemV1(conf, workItemId));
    }
}
