// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.Routines;
import org.jooq.generated.udt.records.BatchMetadataV1Record;
import org.jooq.generated.udt.records.CommitFileMergeWorkItemV1BatchRecord;
import org.jooq.generated.udt.records.CommitFileMergeWorkItemV1ResponseRecord;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.MergedFileBatch;

public class CommitFileMergeWorkItemJob implements Callable<CommitFileMergeWorkItemV1ResponseRecord> {
    private final Time time;
    private final Long workItemId;
    private final String objectKey;
    private final int uploaderBrokerId;
    private final long fileSize;
    private final List<MergedFileBatch> mergedFileBatches;
    private final DSLContext jooqCtx;

    public CommitFileMergeWorkItemJob(
        final Time time,
        final Long workItemId,
        final String objectKey,
        final int uploaderBrokerId,
        final long fileSize,
        final List<MergedFileBatch> mergedFileBatches,
        final DSLContext jooqCtx
    ) {
        this.time = time;
        this.workItemId = workItemId;
        this.objectKey = objectKey;
        this.uploaderBrokerId = uploaderBrokerId;
        this.fileSize = fileSize;
        this.mergedFileBatches = mergedFileBatches;
        this.jooqCtx = jooqCtx;
    }

    @Override
    public CommitFileMergeWorkItemV1ResponseRecord call() {
        try {
            return runOnce();
        } catch (final Exception e) {
            // TODO retry with backoff
            throw new RuntimeException(e);
        }
    }

    private CommitFileMergeWorkItemV1ResponseRecord runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final Instant now = TimeUtils.now(time);
            CommitFileMergeWorkItemV1BatchRecord[] batches = mergedFileBatches.stream()
                .map(b ->
                    new CommitFileMergeWorkItemV1BatchRecord(
                        new BatchMetadataV1Record(
                            b.metadata().topicIdPartition().topicId(),
                            b.metadata().topicIdPartition().topic(),
                            b.metadata().topicIdPartition().partition(),
                            b.metadata().byteOffset(),
                            b.metadata().byteSize(),
                            b.metadata().baseOffset(),
                            b.metadata().lastOffset(),
                            b.metadata().logAppendTimestamp(),
                            b.metadata().batchMaxTimestamp(),
                            b.metadata().timestampType(),
                            b.metadata().producerId(),
                            b.metadata().producerEpoch(),
                            b.metadata().baseSequence(),
                            b.metadata().lastSequence()
                        ),
                        b.parentBatches().toArray(new Long[0])
                    )
                )
                .toArray(CommitFileMergeWorkItemV1BatchRecord[]::new);
            return Routines.commitFileMergeWorkItemV1(conf, now, workItemId, objectKey, uploaderBrokerId, fileSize, batches);
        });
    }
}
