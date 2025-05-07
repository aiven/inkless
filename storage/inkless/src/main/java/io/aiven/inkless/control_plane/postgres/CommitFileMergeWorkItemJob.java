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
import org.jooq.generated.udt.records.BatchMetadataV1Record;
import org.jooq.generated.udt.records.CommitFileMergeWorkItemBatchV1Record;
import org.jooq.generated.udt.records.CommitFileMergeWorkItemResponseV1Record;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.MergedFileBatch;

public class CommitFileMergeWorkItemJob implements Callable<CommitFileMergeWorkItemResponseV1Record> {
    private final Time time;
    private final Long workItemId;
    private final String objectKey;
    private final ObjectFormat format;
    private final int uploaderBrokerId;
    private final long fileSize;
    private final List<MergedFileBatch> mergedFileBatches;
    private final DSLContext jooqCtx;

    public CommitFileMergeWorkItemJob(
        final Time time,
        final Long workItemId,
        final String objectKey,
        final ObjectFormat format,
        final int uploaderBrokerId,
        final long fileSize,
        final List<MergedFileBatch> mergedFileBatches,
        final DSLContext jooqCtx
    ) {
        this.time = time;
        this.workItemId = workItemId;
        this.objectKey = objectKey;
        this.format = format;
        this.uploaderBrokerId = uploaderBrokerId;
        this.fileSize = fileSize;
        this.mergedFileBatches = mergedFileBatches;
        this.jooqCtx = jooqCtx;
    }

    @Override
    public CommitFileMergeWorkItemResponseV1Record call() {
        return JobUtils.run(this::runOnce);
    }

    private CommitFileMergeWorkItemResponseV1Record runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final Instant now = TimeUtils.now(time);
            CommitFileMergeWorkItemBatchV1Record[] batches = mergedFileBatches.stream()
                .map(b ->
                    new CommitFileMergeWorkItemBatchV1Record(
                        new BatchMetadataV1Record(
                            (short) b.metadata().magic(),
                            b.metadata().topicIdPartition().topicId(),
                            b.metadata().topicIdPartition().topic(),
                            b.metadata().topicIdPartition().partition(),
                            b.metadata().byteOffset(),
                            b.metadata().byteSize(),
                            b.metadata().baseOffset(),
                            b.metadata().lastOffset(),
                            b.metadata().logAppendTimestamp(),
                            b.metadata().batchMaxTimestamp(),
                            b.metadata().timestampType()
                        ),
                        b.parentBatches().toArray(new Long[0])
                    )
                )
                .toArray(CommitFileMergeWorkItemBatchV1Record[]::new);
            return Routines.commitFileMergeWorkItemV1(conf, now, workItemId, objectKey, (short) format.id, uploaderBrokerId, fileSize, batches);
        });
    }
}
