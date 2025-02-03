// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.Routines;
import org.jooq.generated.udt.records.ReleaseFileMergeWorkItemV1ResponseRecord;

import java.util.concurrent.Callable;

public class ReleaseFileMergeWorkItemJob implements Callable<ReleaseFileMergeWorkItemV1ResponseRecord> {
    private final Long workItemId;
    private final DSLContext jooqCtx;

    public ReleaseFileMergeWorkItemJob(Long workItemId, DSLContext jooqCtx) {
        this.workItemId = workItemId;
        this.jooqCtx = jooqCtx;
    }

    @Override
    public ReleaseFileMergeWorkItemV1ResponseRecord call() {
        try {
            return runOnce();
        } catch (final Exception e) {
            // TODO retry with backoff
            throw new RuntimeException(e);
        }
    }

    private ReleaseFileMergeWorkItemV1ResponseRecord runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> Routines.releaseFileMergeWorkItemV1(conf, workItemId));
    }
}
