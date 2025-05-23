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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.udt.FileMergeWorkItemResponseV1;
import org.jooq.generated.udt.records.FileMergeWorkItemResponseV1Record;
import org.jooq.types.YearToSecond;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.FileMergeWorkItem;

import static org.jooq.generated.Tables.GET_FILE_MERGE_WORK_ITEM_V1;

public class GetFileMergeWorkItemJob implements Callable<FileMergeWorkItem> {
    private final Time time;
    private final Duration expirationInterval;
    private final Long maxFileSize;
    private final DSLContext jooqCtx;
    private final Consumer<Long> durationCallback;

    public GetFileMergeWorkItemJob(
        final Time time,
        final Duration expirationInterval,
        final Long maxFileSize,
        final DSLContext jooqCtx,
        final Consumer<Long> durationCallback
    ) {
        this.time = time;
        this.expirationInterval = expirationInterval;
        this.maxFileSize = maxFileSize;
        this.jooqCtx = jooqCtx;
        this.durationCallback = durationCallback;
    }

    @Override
    public FileMergeWorkItem call() {
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private FileMergeWorkItem runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) -> {
            final Instant now = TimeUtils.now(time);

            final List<FileMergeWorkItemResponseV1Record> functionResult = conf.dsl()
                .select(
                    FileMergeWorkItemResponseV1.WORK_ITEM_ID,
                    FileMergeWorkItemResponseV1.CREATED_AT,
                    FileMergeWorkItemResponseV1.FILE_IDS
                )
                .from(
                    GET_FILE_MERGE_WORK_ITEM_V1.call(
                        now,
                        YearToSecond.valueOf(expirationInterval),
                        maxFileSize
                    )
                )
                .fetchInto(FileMergeWorkItemResponseV1Record.class);
            if (functionResult.isEmpty()) {
                return null;
            }

            final FileMergeWorkItemResponseV1Record record = functionResult.get(0);
            return new FileMergeWorkItem(
                record.getWorkItemId(),
                record.getCreatedAt(),
                Arrays.stream(record.getFileIds())
                    .map(r ->
                        new FileMergeWorkItem.File(
                            r.getFileId(),
                            r.getObjectKey(),
                            ObjectFormat.forId(r.getFormat().byteValue()),
                            r.getSize(),
                            Arrays.stream(r.getBatches())
                                .map(b -> {
                                        final var m = b.getMetadata();
                                        return new BatchInfo(
                                            b.getBatchId(), b.getObjectKey(),
                                            new BatchMetadata(
                                                m.getMagic().byteValue(),
                                                new TopicIdPartition(m.getTopicId(), new TopicPartition(m.getTopicName(), m.getPartition())),
                                                m.getByteOffset(),
                                                m.getByteSize(),
                                                m.getBaseOffset(),
                                                m.getLastOffset(),
                                                m.getLogAppendTimestamp(),
                                                m.getBatchMaxTimestamp(),
                                                m.getTimestampType()
                                            )
                                        );
                                    }
                                )
                                .toList()
                        )
                    )
                    .toList()
            );
        });
    }
}
