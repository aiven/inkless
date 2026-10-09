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
package io.aiven.inkless.remote_log_metadata.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.rlmm.generated.tables.records.RlmmPartitionsRecord;
import org.jooq.rlmm.generated.tables.records.RlmmSegmentsRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

import static org.jooq.rlmm.generated.tables.RlmmEpochState.RLMM_EPOCH_STATE;
import static org.jooq.rlmm.generated.tables.RlmmPartitions.RLMM_PARTITIONS;
import static org.jooq.rlmm.generated.tables.RlmmSegmentEpochs.RLMM_SEGMENT_EPOCHS;
import static org.jooq.rlmm.generated.tables.RlmmSegments.RLMM_SEGMENTS;

class RemoteLogMetadataDao {
    private static final Logger log = LoggerFactory.getLogger(RemoteLogMetadataDao.class);

    private final DSLContext dsl;
    private final Time time;
    private final String clusterId;

    RemoteLogMetadataDao(final DSLContext dsl, final Time time, final String clusterId) {
        this.dsl = Objects.requireNonNull(dsl, "dsl must not be null");
        this.time = Objects.requireNonNull(time, "time must not be null");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId must not be null");
    }

    MutationOutcome addSegment(final RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
        Objects.requireNonNull(metadata, "metadata must not be null");
        if (metadata.state() != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
            throw new IllegalArgumentException(
                "addSegment called with a segment in unexpected state: " + metadata.state());
        }
        final TopicIdPartition tp = metadata.topicIdPartition();
        return inMutationTransaction(tp, (tx, partition) -> {
            throwIfPartitionDeleted(partition, tp);
            final RlmmSegmentsRecord existing =
                findSegmentRow(tx, tp, metadata.remoteLogSegmentId().id());
            if (existing != null) {
                if (segmentState(existing) != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
                    log.warn("Dropping add for segment {} in partition {}: already in state {}",
                        metadata.remoteLogSegmentId(), tp, segmentState(existing));
                    return MutationOutcome.INVALID_TRANSITION;
                }
                if (!storedContentEquals(existing, metadata)) {
                    throw new IllegalArgumentException("Re-add of segment " + metadata.remoteLogSegmentId()
                        + " carries different content; refusing to replace the stored metadata");
                }
                return MutationOutcome.NOOP_SAME_STATE;
            }
            tx.insertInto(RLMM_SEGMENTS)
                .columns(RLMM_SEGMENTS.CLUSTER_ID,
                    RLMM_SEGMENTS.TOPIC_ID,
                    RLMM_SEGMENTS.PARTITION_ID,
                    RLMM_SEGMENTS.SEGMENT_ID,
                    RLMM_SEGMENTS.START_OFFSET,
                    RLMM_SEGMENTS.END_OFFSET,
                    RLMM_SEGMENTS.MAX_TIMESTAMP_MS,
                    RLMM_SEGMENTS.SEGMENT_SIZE,
                    RLMM_SEGMENTS.TXN_IDX_EMPTY,
                    RLMM_SEGMENTS.LEADER_EPOCHS,
                    RLMM_SEGMENTS.STATE,
                    RLMM_SEGMENTS.CUSTOM_METADATA,
                    RLMM_SEGMENTS.BROKER_ID,
                    RLMM_SEGMENTS.EVENT_TIMESTAMP_MS,
                    RLMM_SEGMENTS.UPDATED_AT)
                .values(clusterId,
                    tp.topicId(),
                    tp.partition(),
                    metadata.remoteLogSegmentId().id(),
                    metadata.startOffset(),
                    metadata.endOffset(),
                    metadata.maxTimestampMs(),
                    metadata.segmentSizeInBytes(),
                    metadata.isTxnIdxEmpty(),
                    JSONB.valueOf(leaderEpochsToJson(metadata.segmentLeaderEpochs())),
                    stateId(RemoteLogSegmentState.COPY_SEGMENT_STARTED),
                    customBytes(metadata.customMetadata()),
                    metadata.brokerId(),
                    metadata.eventTimestampMs(),
                    Instant.ofEpochMilli(time.milliseconds()))
                .execute();
            // Seed one unreferenced mapping per epoch, like
            // RemoteLogLeaderEpochState.handleSegmentWithCopySegmentStartedState. The watermark
            // does not move here. highestOffsetForEpoch only covers finished segments.
            final NavigableMap<Integer, Long> epochs = new TreeMap<>(metadata.segmentLeaderEpochs());
            for (final Map.Entry<Integer, Long> entry : epochs.entrySet()) {
                tx.insertInto(RLMM_SEGMENT_EPOCHS)
                    .columns(RLMM_SEGMENT_EPOCHS.CLUSTER_ID,
                        RLMM_SEGMENT_EPOCHS.TOPIC_ID,
                        RLMM_SEGMENT_EPOCHS.PARTITION_ID,
                        RLMM_SEGMENT_EPOCHS.SEGMENT_ID,
                        RLMM_SEGMENT_EPOCHS.EPOCH,
                        RLMM_SEGMENT_EPOCHS.EPOCH_START_OFFSET,
                        RLMM_SEGMENT_EPOCHS.EPOCH_END_OFFSET,
                        RLMM_SEGMENT_EPOCHS.REFERENCED)
                    .values(clusterId,
                        tp.topicId(),
                        tp.partition(),
                        metadata.remoteLogSegmentId().id(),
                        entry.getKey(),
                        entry.getValue(),
                        epochEndOffset(epochs, entry.getKey(), metadata.endOffset()),
                        false)
                    .execute();
            }
            return MutationOutcome.APPLIED;
        });
    }

    MutationOutcome applyUpdate(final RemoteLogSegmentMetadataUpdate update) throws RemoteStorageException {
        Objects.requireNonNull(update, "update must not be null");
        switch (update.state()) {
            case COPY_SEGMENT_STARTED:
                throw new IllegalArgumentException(
                    "update with state COPY_SEGMENT_STARTED can not be applied: " + update);
            case COPY_SEGMENT_FINISHED:
                return applyCopyFinished(update);
            case DELETE_SEGMENT_STARTED:
                return applyDeleteStarted(update);
            case DELETE_SEGMENT_FINISHED:
                return applyDeleteFinished(update);
            default:
                throw new IllegalArgumentException("update with unsupported state: " + update.state());
        }
    }

    MutationOutcome putPartitionDelete(final RemotePartitionDeleteMetadata delete) throws RemoteStorageException {
        Objects.requireNonNull(delete, "delete must not be null");
        final TopicIdPartition tp = delete.topicIdPartition();
        return inMutationTransaction(tp, (tx, partition) -> {
            final RemotePartitionDeleteState current = partitionDeleteState(partition);
            final RemotePartitionDeleteState target = delete.state();
            if (current == target) {
                return MutationOutcome.NOOP_SAME_STATE;
            }
            if (!RemotePartitionDeleteState.isValidTransition(current, target)) {
                log.warn("Dropping partition delete for {}: cannot transition from {} to {}",
                    tp, current, target);
                return MutationOutcome.INVALID_TRANSITION;
            }
            tx.update(RLMM_PARTITIONS)
                .set(RLMM_PARTITIONS.DELETE_STATE, (short) target.id())
                .set(RLMM_PARTITIONS.DELETE_EVENT_TIMESTAMP_MS, delete.eventTimestampMs())
                .set(RLMM_PARTITIONS.DELETE_BROKER_ID, delete.brokerId())
                .where(partitionCondition(tp))
                .execute();
            if (target == RemotePartitionDeleteState.DELETE_PARTITION_FINISHED) {
                // Clear all per-partition state, like the topic-based store drops the cache.
                tx.deleteFrom(RLMM_EPOCH_STATE).where(epochStateCondition(tp)).execute();
                tx.deleteFrom(RLMM_SEGMENTS).where(segmentCondition(tp)).execute();
            }
            return MutationOutcome.APPLIED;
        });
    }

    Optional<RemoteLogSegmentMetadata> findSegment(final TopicIdPartition tp, final int epoch, final long offset)
        throws RemoteStorageException {
        Objects.requireNonNull(tp, "tp must not be null");
        try {
            final Field<Short> deleteState = deleteStateField(tp);
            // Floor lookup first, boundary check second: like RemoteLogLeaderEpochState.floorEntry,
            // this resolves the greatest start at or below the offset and only then checks the
            // epoch end, so a stale narrow finisher punches the same read hole it does upstream.
            final Record record = dsl.select(RLMM_SEGMENTS.asterisk(), RLMM_SEGMENT_EPOCHS.EPOCH_END_OFFSET, deleteState)
                .from(RLMM_SEGMENTS)
                .join(RLMM_SEGMENT_EPOCHS)
                .on(RLMM_SEGMENT_EPOCHS.CLUSTER_ID.eq(clusterId))
                .and(RLMM_SEGMENT_EPOCHS.TOPIC_ID.eq(tp.topicId()))
                .and(RLMM_SEGMENT_EPOCHS.PARTITION_ID.eq(tp.partition()))
                .and(RLMM_SEGMENT_EPOCHS.SEGMENT_ID.eq(RLMM_SEGMENTS.SEGMENT_ID))
                .and(RLMM_SEGMENT_EPOCHS.EPOCH.eq(epoch))
                .where(segmentCondition(tp))
                .and(RLMM_SEGMENT_EPOCHS.REFERENCED.eq(true))
                .and(RLMM_SEGMENT_EPOCHS.EPOCH_START_OFFSET.le(offset))
                .orderBy(RLMM_SEGMENT_EPOCHS.EPOCH_START_OFFSET.desc())
                .limit(1)
                .fetchOne();
            if (record == null) {
                throwIfPartitionFinished(tp);
                return Optional.empty();
            }
            throwIfFinished(record.get(deleteState), tp);
            if (offset > record.get(RLMM_SEGMENT_EPOCHS.EPOCH_END_OFFSET)) {
                return Optional.empty();
            }
            return Optional.of(materialize(tp, record));
        } catch (final RemoteResourceNotFoundException e) {
            throw e;
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    Optional<RemoteLogSegmentMetadata> nextSegmentWithTxnIndex(final TopicIdPartition tp, final int epoch, final long offset)
        throws RemoteStorageException {
        Objects.requireNonNull(tp, "tp must not be null");
        // Walk forward exactly like RemoteLogMetadataCache.nextSegmentWithTxnIndex: start
        // from the segment containing the offset, then hop to each segment's end plus one
        // until a segment with a transaction index shows up.
        Optional<RemoteLogSegmentMetadata> current = findSegment(tp, epoch, offset);
        while (current.isPresent() && current.get().isTxnIdxEmpty()) {
            current = findSegment(tp, epoch, current.get().endOffset() + 1);
        }
        return current.filter(metadata -> !metadata.isTxnIdxEmpty());
    }

    Optional<Long> highestOffsetForEpoch(final TopicIdPartition tp, final int epoch) throws RemoteStorageException {
        Objects.requireNonNull(tp, "tp must not be null");
        try {
            final Field<Short> deleteState = deleteStateField(tp);
            final Record record = dsl.select(RLMM_EPOCH_STATE.HIGHEST_OFFSET, deleteState)
                .from(RLMM_EPOCH_STATE)
                .where(epochStateCondition(tp))
                .and(RLMM_EPOCH_STATE.EPOCH.eq(epoch))
                .fetchOne();
            if (record == null) {
                throwIfPartitionFinished(tp);
                return Optional.empty();
            }
            throwIfFinished(record.get(deleteState), tp);
            return Optional.of(record.get(RLMM_EPOCH_STATE.HIGHEST_OFFSET));
        } catch (final RemoteResourceNotFoundException e) {
            throw e;
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    List<RemoteLogSegmentMetadata> listSegments(final TopicIdPartition tp) throws RemoteStorageException {
        Objects.requireNonNull(tp, "tp must not be null");
        try {
            final List<RemoteLogSegmentMetadata> result = new ArrayList<>();
            for (final RlmmSegmentsRecord record : dsl.selectFrom(RLMM_SEGMENTS)
                .where(segmentCondition(tp))
                .and(RLMM_SEGMENTS.STATE.ne(stateId(RemoteLogSegmentState.DELETE_SEGMENT_FINISHED)))
                .orderBy(RLMM_SEGMENTS.START_OFFSET.asc())
                .fetch()) {
                result.add(materialize(tp, record));
            }
            if (result.isEmpty()) {
                throwIfPartitionFinished(tp);
            }
            return result;
        } catch (final RemoteResourceNotFoundException e) {
            throw e;
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    List<RemoteLogSegmentMetadata> listSegments(final TopicIdPartition tp, final int epoch) throws RemoteStorageException {
        Objects.requireNonNull(tp, "tp must not be null");
        try {
            final List<RemoteLogSegmentMetadata> result = new ArrayList<>();
            for (final Record record : dsl.select(RLMM_SEGMENTS.asterisk())
                .from(RLMM_SEGMENTS)
                .join(RLMM_SEGMENT_EPOCHS)
                .on(RLMM_SEGMENT_EPOCHS.CLUSTER_ID.eq(clusterId))
                .and(RLMM_SEGMENT_EPOCHS.TOPIC_ID.eq(tp.topicId()))
                .and(RLMM_SEGMENT_EPOCHS.PARTITION_ID.eq(tp.partition()))
                .and(RLMM_SEGMENT_EPOCHS.SEGMENT_ID.eq(RLMM_SEGMENTS.SEGMENT_ID))
                .and(RLMM_SEGMENT_EPOCHS.EPOCH.eq(epoch))
                .where(segmentCondition(tp))
                .and(RLMM_SEGMENTS.STATE.ne(stateId(RemoteLogSegmentState.DELETE_SEGMENT_FINISHED)))
                .orderBy(RLMM_SEGMENTS.START_OFFSET.asc(), RLMM_SEGMENT_EPOCHS.REFERENCED.desc())
                .fetch()) {
                result.add(materialize(tp, record));
            }
            if (result.isEmpty()) {
                throwIfPartitionFinished(tp);
            }
            return result;
        } catch (final RemoteResourceNotFoundException e) {
            throw e;
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    long remoteLogSize(final TopicIdPartition tp, final int epoch) throws RemoteStorageException {
        Objects.requireNonNull(tp, "tp must not be null");
        try {
            final Long total = dsl.select(DSL.coalesce(DSL.sum(RLMM_SEGMENTS.SEGMENT_SIZE).cast(Long.class), DSL.val(0L)))
                .from(RLMM_SEGMENTS)
                .join(RLMM_SEGMENT_EPOCHS)
                .on(RLMM_SEGMENT_EPOCHS.CLUSTER_ID.eq(clusterId))
                .and(RLMM_SEGMENT_EPOCHS.TOPIC_ID.eq(tp.topicId()))
                .and(RLMM_SEGMENT_EPOCHS.PARTITION_ID.eq(tp.partition()))
                .and(RLMM_SEGMENT_EPOCHS.SEGMENT_ID.eq(RLMM_SEGMENTS.SEGMENT_ID))
                .and(RLMM_SEGMENT_EPOCHS.EPOCH.eq(epoch))
                .where(segmentCondition(tp))
                .and(RLMM_SEGMENTS.STATE.ne(stateId(RemoteLogSegmentState.DELETE_SEGMENT_FINISHED)))
                .fetchOneInto(Long.class);
            final long size = total == null ? 0L : total;
            if (size == 0L) {
                throwIfPartitionFinished(tp);
            }
            return size;
        } catch (final RemoteResourceNotFoundException e) {
            throw e;
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    private MutationOutcome applyCopyFinished(final RemoteLogSegmentMetadataUpdate update) throws RemoteStorageException {
        final TopicIdPartition tp = update.topicIdPartition();
        return inMutationTransaction(tp, (tx, partition) -> {
            throwIfPartitionDeleted(partition, tp);
            final RlmmSegmentsRecord stored = findSegmentRow(tx, tp, update.remoteLogSegmentId().id());
            if (stored == null) {
                throw new RemoteResourceNotFoundException(
                    "No remote log segment metadata found for: " + update.remoteLogSegmentId());
            }
            final RemoteLogSegmentState current = segmentState(stored);
            if (current != RemoteLogSegmentState.COPY_SEGMENT_STARTED
                && current != RemoteLogSegmentState.COPY_SEGMENT_FINISHED) {
                log.warn("Dropping update for segment {} in partition {}: cannot transition from {} to {}",
                    update.remoteLogSegmentId(), tp, current, update.state());
                return MutationOutcome.INVALID_TRANSITION;
            }
            // Refresh the event fields and custom metadata from the update, as
            // createWithUpdates does, and resolve overlaps from the stored segment.
            tx.update(RLMM_SEGMENTS)
                .set(RLMM_SEGMENTS.STATE, stateId(RemoteLogSegmentState.COPY_SEGMENT_FINISHED))
                .set(RLMM_SEGMENTS.BROKER_ID, update.brokerId())
                .set(RLMM_SEGMENTS.EVENT_TIMESTAMP_MS, update.eventTimestampMs())
                .set(RLMM_SEGMENTS.CUSTOM_METADATA, customBytes(update.customMetadata()))
                .set(RLMM_SEGMENTS.UPDATED_AT, Instant.ofEpochMilli(time.milliseconds()))
                .where(segmentPkCondition(tp, stored.getSegmentId()))
                .execute();
            final NavigableMap<Integer, Long> epochs = leaderEpochsFromJson(stored.getLeaderEpochs().data());
            for (final Map.Entry<Integer, Long> entry : epochs.entrySet()) {
                final int epoch = entry.getKey();
                final long epochStart = entry.getValue();
                final long epochEnd = epochEndOffset(epochs, epoch, stored.getEndOffset());
                final Long watermark = currentWatermark(tx, tp, epoch);
                if (watermark == null || watermark <= epochEnd) {
                    // Pop every referenced slot at this start or higher, like the
                    // while loop in handleSegmentWithCopySegmentFinishedState.
                    tx.update(RLMM_SEGMENT_EPOCHS)
                        .set(RLMM_SEGMENT_EPOCHS.REFERENCED, false)
                        .where(segmentEpochsCondition(tp))
                        .and(RLMM_SEGMENT_EPOCHS.EPOCH.eq(epoch))
                        .and(RLMM_SEGMENT_EPOCHS.REFERENCED.eq(true))
                        .and(RLMM_SEGMENT_EPOCHS.EPOCH_START_OFFSET.ge(epochStart))
                        .execute();
                }
                // A stale finisher silently replaces the slot without covering the
                // watermark, like offsetToId.put. This only matches in that case: the
                // preceding pop already cleared the slot otherwise.
                tx.deleteFrom(RLMM_SEGMENT_EPOCHS)
                    .where(segmentEpochsCondition(tp))
                    .and(RLMM_SEGMENT_EPOCHS.EPOCH.eq(epoch))
                    .and(RLMM_SEGMENT_EPOCHS.REFERENCED.eq(true))
                    .and(RLMM_SEGMENT_EPOCHS.EPOCH_START_OFFSET.eq(epochStart))
                    .and(RLMM_SEGMENT_EPOCHS.SEGMENT_ID.ne(stored.getSegmentId()))
                    .execute();
                // Reference the slot, recreating the row when a stale finisher
                // shadowed it. This takes over the slot, as offsetToId.put does.
                tx.insertInto(RLMM_SEGMENT_EPOCHS)
                    .columns(RLMM_SEGMENT_EPOCHS.CLUSTER_ID,
                        RLMM_SEGMENT_EPOCHS.TOPIC_ID,
                        RLMM_SEGMENT_EPOCHS.PARTITION_ID,
                        RLMM_SEGMENT_EPOCHS.SEGMENT_ID,
                        RLMM_SEGMENT_EPOCHS.EPOCH,
                        RLMM_SEGMENT_EPOCHS.EPOCH_START_OFFSET,
                        RLMM_SEGMENT_EPOCHS.EPOCH_END_OFFSET,
                        RLMM_SEGMENT_EPOCHS.REFERENCED)
                    .values(clusterId,
                        tp.topicId(),
                        tp.partition(),
                        stored.getSegmentId(),
                        epoch,
                        epochStart,
                        epochEnd,
                        true)
                    .onConflict(RLMM_SEGMENT_EPOCHS.CLUSTER_ID,
                        RLMM_SEGMENT_EPOCHS.TOPIC_ID,
                        RLMM_SEGMENT_EPOCHS.PARTITION_ID,
                        RLMM_SEGMENT_EPOCHS.SEGMENT_ID,
                        RLMM_SEGMENT_EPOCHS.EPOCH)
                    .doUpdate()
                    .set(RLMM_SEGMENT_EPOCHS.EPOCH_START_OFFSET, epochStart)
                    .set(RLMM_SEGMENT_EPOCHS.EPOCH_END_OFFSET, epochEnd)
                    .set(RLMM_SEGMENT_EPOCHS.REFERENCED, true)
                    .execute();
                tx.insertInto(RLMM_EPOCH_STATE)
                    .columns(RLMM_EPOCH_STATE.CLUSTER_ID,
                        RLMM_EPOCH_STATE.TOPIC_ID,
                        RLMM_EPOCH_STATE.PARTITION_ID,
                        RLMM_EPOCH_STATE.EPOCH,
                        RLMM_EPOCH_STATE.HIGHEST_OFFSET)
                    .values(clusterId, tp.topicId(), tp.partition(), epoch, epochEnd)
                    .onConflict(RLMM_EPOCH_STATE.CLUSTER_ID,
                        RLMM_EPOCH_STATE.TOPIC_ID,
                        RLMM_EPOCH_STATE.PARTITION_ID,
                        RLMM_EPOCH_STATE.EPOCH)
                    .doUpdate()
                    .set(RLMM_EPOCH_STATE.HIGHEST_OFFSET,
                        DSL.greatest(RLMM_EPOCH_STATE.HIGHEST_OFFSET, DSL.val(epochEnd)))
                    .execute();
            }
            return MutationOutcome.APPLIED;
        });
    }

    private MutationOutcome applyDeleteStarted(final RemoteLogSegmentMetadataUpdate update) throws RemoteStorageException {
        final TopicIdPartition tp = update.topicIdPartition();
        return inMutationTransaction(tp, (tx, partition) -> {
            throwIfPartitionDeleted(partition, tp);
            final RlmmSegmentsRecord stored = findSegmentRow(tx, tp, update.remoteLogSegmentId().id());
            if (stored == null) {
                throw new RemoteResourceNotFoundException(
                    "No remote log segment metadata found for: " + update.remoteLogSegmentId());
            }
            final RemoteLogSegmentState current = segmentState(stored);
            if (current != RemoteLogSegmentState.COPY_SEGMENT_STARTED
                && current != RemoteLogSegmentState.COPY_SEGMENT_FINISHED
                && current != RemoteLogSegmentState.DELETE_SEGMENT_STARTED) {
                log.warn("Dropping update for segment {} in partition {}: cannot transition from {} to {}",
                    update.remoteLogSegmentId(), tp, current, update.state());
                return MutationOutcome.INVALID_TRANSITION;
            }
            tx.update(RLMM_SEGMENTS)
                .set(RLMM_SEGMENTS.STATE, stateId(RemoteLogSegmentState.DELETE_SEGMENT_STARTED))
                .set(RLMM_SEGMENTS.BROKER_ID, update.brokerId())
                .set(RLMM_SEGMENTS.EVENT_TIMESTAMP_MS, update.eventTimestampMs())
                .set(RLMM_SEGMENTS.CUSTOM_METADATA, customBytes(update.customMetadata()))
                .set(RLMM_SEGMENTS.UPDATED_AT, Instant.ofEpochMilli(time.milliseconds()))
                .where(segmentPkCondition(tp, stored.getSegmentId()))
                .execute();
            // Unreference every slot of this segment. The rows remain listable. Rows a
            // stale finisher shadowed are re-created as unreferenced, matching the
            // unconditional unreferencedSegmentIds.add upstream.
            tx.update(RLMM_SEGMENT_EPOCHS)
                .set(RLMM_SEGMENT_EPOCHS.REFERENCED, false)
                .where(segmentEpochsCondition(tp))
                .and(RLMM_SEGMENT_EPOCHS.SEGMENT_ID.eq(stored.getSegmentId()))
                .execute();
            final NavigableMap<Integer, Long> epochs = leaderEpochsFromJson(stored.getLeaderEpochs().data());
            for (final Map.Entry<Integer, Long> entry : epochs.entrySet()) {
                tx.insertInto(RLMM_SEGMENT_EPOCHS)
                    .columns(RLMM_SEGMENT_EPOCHS.CLUSTER_ID,
                        RLMM_SEGMENT_EPOCHS.TOPIC_ID,
                        RLMM_SEGMENT_EPOCHS.PARTITION_ID,
                        RLMM_SEGMENT_EPOCHS.SEGMENT_ID,
                        RLMM_SEGMENT_EPOCHS.EPOCH,
                        RLMM_SEGMENT_EPOCHS.EPOCH_START_OFFSET,
                        RLMM_SEGMENT_EPOCHS.EPOCH_END_OFFSET,
                        RLMM_SEGMENT_EPOCHS.REFERENCED)
                    .values(clusterId,
                        tp.topicId(),
                        tp.partition(),
                        stored.getSegmentId(),
                        entry.getKey(),
                        entry.getValue(),
                        epochEndOffset(epochs, entry.getKey(), stored.getEndOffset()),
                        false)
                    .onConflict(RLMM_SEGMENT_EPOCHS.CLUSTER_ID,
                        RLMM_SEGMENT_EPOCHS.TOPIC_ID,
                        RLMM_SEGMENT_EPOCHS.PARTITION_ID,
                        RLMM_SEGMENT_EPOCHS.SEGMENT_ID,
                        RLMM_SEGMENT_EPOCHS.EPOCH)
                    .doNothing()
                    .execute();
            }
            return MutationOutcome.APPLIED;
        });
    }

    private MutationOutcome applyDeleteFinished(final RemoteLogSegmentMetadataUpdate update) throws RemoteStorageException {
        final TopicIdPartition tp = update.topicIdPartition();
        return inMutationTransaction(tp, (tx, partition) -> {
            throwIfPartitionDeleted(partition, tp);
            final RlmmSegmentsRecord stored = findSegmentRow(tx, tp, update.remoteLogSegmentId().id());
            if (stored == null) {
                throw new RemoteResourceNotFoundException(
                    "No remote log segment metadata found for: " + update.remoteLogSegmentId());
            }
            final RemoteLogSegmentState current = segmentState(stored);
            if (current == RemoteLogSegmentState.DELETE_SEGMENT_FINISHED) {
                // The cache forgets finished segments and throws here; the tombstone
                // remembers them, so a retried terminal update completes as a no-op.
                return MutationOutcome.NOOP_SAME_STATE;
            }
            if (current != RemoteLogSegmentState.DELETE_SEGMENT_STARTED) {
                log.warn("Dropping update for segment {} in partition {}: cannot transition from {} to {}",
                    update.remoteLogSegmentId(), tp, current, update.state());
                return MutationOutcome.INVALID_TRANSITION;
            }
            tx.deleteFrom(RLMM_SEGMENT_EPOCHS)
                .where(segmentEpochsCondition(tp))
                .and(RLMM_SEGMENT_EPOCHS.SEGMENT_ID.eq(stored.getSegmentId()))
                .execute();
            tx.update(RLMM_SEGMENTS)
                .set(RLMM_SEGMENTS.STATE, stateId(RemoteLogSegmentState.DELETE_SEGMENT_FINISHED))
                .set(RLMM_SEGMENTS.BROKER_ID, update.brokerId())
                .set(RLMM_SEGMENTS.EVENT_TIMESTAMP_MS, update.eventTimestampMs())
                .set(RLMM_SEGMENTS.CUSTOM_METADATA, customBytes(update.customMetadata()))
                .set(RLMM_SEGMENTS.UPDATED_AT, Instant.ofEpochMilli(time.milliseconds()))
                .where(segmentPkCondition(tp, stored.getSegmentId()))
                .execute();
            return MutationOutcome.APPLIED;
        });
    }

    private interface MutationBody<T> {
        T apply(DSLContext tx, RlmmPartitionsRecord partition) throws RemoteStorageException;
    }

    private <T> T inMutationTransaction(final TopicIdPartition tp, final MutationBody<T> body)
        throws RemoteStorageException {
        try {
            return dsl.transactionResult(configuration -> {
                final DSLContext tx = DSL.using(configuration);
                final RlmmPartitionsRecord partition = upsertPartition(tx, tp);
                return body.apply(tx, partition);
            });
        } catch (final Exception e) {
            throw unwrapMutationError(e);
        }
    }

    private RlmmPartitionsRecord upsertPartition(final DSLContext tx, final TopicIdPartition tp) {
        // Insert the partition row, or bump its revision. Either way the row stays
        // write-locked until commit, which serializes concurrent mutations.
        return tx.insertInto(RLMM_PARTITIONS)
            .columns(RLMM_PARTITIONS.CLUSTER_ID, RLMM_PARTITIONS.TOPIC_ID, RLMM_PARTITIONS.PARTITION_ID)
            .values(clusterId, tp.topicId(), tp.partition())
            .onConflict(RLMM_PARTITIONS.CLUSTER_ID, RLMM_PARTITIONS.TOPIC_ID, RLMM_PARTITIONS.PARTITION_ID)
            .doUpdate()
            .set(RLMM_PARTITIONS.REVISION, RLMM_PARTITIONS.REVISION.add(1))
            .returning()
            .fetchOne();
    }

    private RlmmSegmentsRecord findSegmentRow(final DSLContext tx, final TopicIdPartition tp,
                                              final org.apache.kafka.common.Uuid segmentId) {
        return tx.selectFrom(RLMM_SEGMENTS).where(segmentPkCondition(tp, segmentId)).fetchOne();
    }

    private Long currentWatermark(final DSLContext tx, final TopicIdPartition tp, final int epoch) {
        final Record record = tx.select(RLMM_EPOCH_STATE.HIGHEST_OFFSET)
            .from(RLMM_EPOCH_STATE)
            .where(epochStateCondition(tp))
            .and(RLMM_EPOCH_STATE.EPOCH.eq(epoch))
            .fetchOne();
        return record == null ? null : record.get(RLMM_EPOCH_STATE.HIGHEST_OFFSET);
    }

    private void throwIfPartitionDeleted(final RlmmPartitionsRecord partition, final TopicIdPartition tp)
        throws RemoteResourceNotFoundException {
        if (partitionDeleteState(partition) == RemotePartitionDeleteState.DELETE_PARTITION_FINISHED) {
            throw new RemoteResourceNotFoundException("No resource found for partition: " + tp);
        }
    }

    private void throwIfPartitionFinished(final TopicIdPartition tp) throws RemoteResourceNotFoundException {
        final RlmmPartitionsRecord partition = dsl.selectFrom(RLMM_PARTITIONS)
            .where(partitionCondition(tp))
            .fetchOne();
        if (partition != null
            && partitionDeleteState(partition) == RemotePartitionDeleteState.DELETE_PARTITION_FINISHED) {
            throw new RemoteResourceNotFoundException("No resource found for partition: " + tp);
        }
    }

    private void throwIfFinished(final Short deleteState, final TopicIdPartition tp)
        throws RemoteResourceNotFoundException {
        if (deleteState != null && deleteState == (short) RemotePartitionDeleteState.DELETE_PARTITION_FINISHED.id()) {
            throw new RemoteResourceNotFoundException("No resource found for partition: " + tp);
        }
    }

    private Field<Short> deleteStateField(final TopicIdPartition tp) {
        return DSL.field(dsl.select(RLMM_PARTITIONS.DELETE_STATE)
            .from(RLMM_PARTITIONS)
            .where(partitionCondition(tp)));
    }

    private Condition partitionCondition(final TopicIdPartition tp) {
        return RLMM_PARTITIONS.CLUSTER_ID.eq(clusterId)
            .and(RLMM_PARTITIONS.TOPIC_ID.eq(tp.topicId()))
            .and(RLMM_PARTITIONS.PARTITION_ID.eq(tp.partition()));
    }

    private Condition segmentCondition(final TopicIdPartition tp) {
        return RLMM_SEGMENTS.CLUSTER_ID.eq(clusterId)
            .and(RLMM_SEGMENTS.TOPIC_ID.eq(tp.topicId()))
            .and(RLMM_SEGMENTS.PARTITION_ID.eq(tp.partition()));
    }

    private Condition segmentPkCondition(final TopicIdPartition tp, final org.apache.kafka.common.Uuid segmentId) {
        return segmentCondition(tp).and(RLMM_SEGMENTS.SEGMENT_ID.eq(segmentId));
    }

    private Condition segmentEpochsCondition(final TopicIdPartition tp) {
        return RLMM_SEGMENT_EPOCHS.CLUSTER_ID.eq(clusterId)
            .and(RLMM_SEGMENT_EPOCHS.TOPIC_ID.eq(tp.topicId()))
            .and(RLMM_SEGMENT_EPOCHS.PARTITION_ID.eq(tp.partition()));
    }

    private Condition epochStateCondition(final TopicIdPartition tp) {
        return RLMM_EPOCH_STATE.CLUSTER_ID.eq(clusterId)
            .and(RLMM_EPOCH_STATE.TOPIC_ID.eq(tp.topicId()))
            .and(RLMM_EPOCH_STATE.PARTITION_ID.eq(tp.partition()));
    }

    private RemoteLogSegmentMetadata materialize(final TopicIdPartition tp, final Record record) {
        final RemoteLogSegmentId id =
            new RemoteLogSegmentId(tp, record.get(RLMM_SEGMENTS.SEGMENT_ID));
        final byte[] custom = record.get(RLMM_SEGMENTS.CUSTOM_METADATA);
        return new RemoteLogSegmentMetadata(id,
            record.get(RLMM_SEGMENTS.START_OFFSET),
            record.get(RLMM_SEGMENTS.END_OFFSET),
            record.get(RLMM_SEGMENTS.MAX_TIMESTAMP_MS),
            record.get(RLMM_SEGMENTS.BROKER_ID),
            record.get(RLMM_SEGMENTS.EVENT_TIMESTAMP_MS),
            record.get(RLMM_SEGMENTS.SEGMENT_SIZE),
            custom == null ? Optional.empty() : Optional.of(new CustomMetadata(custom)),
            segmentState(record.get(RLMM_SEGMENTS.STATE)),
            leaderEpochsFromJson(record.get(RLMM_SEGMENTS.LEADER_EPOCHS).data()),
            record.get(RLMM_SEGMENTS.TXN_IDX_EMPTY));
    }

    private static short stateId(final RemoteLogSegmentState state) {
        return (short) state.id();
    }

    private static RemoteLogSegmentState segmentState(final RlmmSegmentsRecord record) {
        return segmentState(record.getState());
    }

    private static RemoteLogSegmentState segmentState(final Short stateId) {
        final RemoteLogSegmentState state = RemoteLogSegmentState.forId(stateId.byteValue());
        if (state == null) {
            throw new IllegalStateException("Unknown remote log segment state id: " + stateId);
        }
        return state;
    }

    private static RemotePartitionDeleteState partitionDeleteState(final RlmmPartitionsRecord partition) {
        final Short stateId = partition.getDeleteState();
        if (stateId == null) {
            return null;
        }
        final RemotePartitionDeleteState state = RemotePartitionDeleteState.forId(stateId.byteValue());
        if (state == null) {
            throw new IllegalStateException("Unknown remote partition delete state id: " + stateId);
        }
        return state;
    }

    private static byte[] customBytes(final Optional<CustomMetadata> customMetadata) {
        return customMetadata.map(CustomMetadata::value).orElse(null);
    }

    private static boolean storedContentEquals(final RlmmSegmentsRecord stored,
                                               final RemoteLogSegmentMetadata metadata) {
        return stored.getStartOffset() == metadata.startOffset()
            && stored.getEndOffset() == metadata.endOffset()
            && stored.getMaxTimestampMs() == metadata.maxTimestampMs()
            && stored.getSegmentSize() == metadata.segmentSizeInBytes()
            && stored.getTxnIdxEmpty() == metadata.isTxnIdxEmpty()
            && stored.getBrokerId() == metadata.brokerId()
            && stored.getEventTimestampMs() == metadata.eventTimestampMs()
            && Arrays.equals(stored.getCustomMetadata(), customBytes(metadata.customMetadata()))
            && leaderEpochsFromJson(stored.getLeaderEpochs().data()).equals(new TreeMap<>(metadata.segmentLeaderEpochs()));
    }

    private static long epochEndOffset(final NavigableMap<Integer, Long> epochs, final int epoch,
                                       final long segmentEndOffset) {
        final Map.Entry<Integer, Long> next = epochs.higherEntry(epoch);
        return next != null ? next.getValue() - 1 : segmentEndOffset;
    }

    static String leaderEpochsToJson(final Map<Integer, Long> epochs) {
        final StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (final Map.Entry<Integer, Long> entry : new TreeMap<>(epochs).entrySet()) {
            if (!first) {
                json.append(',');
            }
            first = false;
            json.append('"').append(entry.getKey().intValue()).append('"')
                .append(':').append(entry.getValue().longValue());
        }
        return json.append('}').toString();
    }

    static NavigableMap<Integer, Long> leaderEpochsFromJson(final String json) {
        // Parses {"0":0,"1":20} style objects. Only the writer produces this
        // format, and this parser rejects anything else.
        final TreeMap<Integer, Long> epochs = new TreeMap<>();
        int pos = 0;
        pos = skipWhitespace(json, pos);
        if (pos >= json.length() || json.charAt(pos) != '{') {
            throw new IllegalStateException("Invalid leader epochs JSON: " + json);
        }
        pos = skipWhitespace(json, pos + 1);
        if (pos < json.length() && json.charAt(pos) == '}') {
            throw new IllegalStateException("Empty leader epochs JSON: " + json);
        }
        while (true) {
            pos = skipWhitespace(json, pos);
            if (pos >= json.length() || json.charAt(pos) != '"') {
                throw new IllegalStateException("Invalid leader epochs JSON: " + json);
            }
            final int keyEnd = json.indexOf('"', pos + 1);
            if (keyEnd < 0) {
                throw new IllegalStateException("Invalid leader epochs JSON: " + json);
            }
            final int epoch;
            try {
                epoch = Integer.parseInt(json.substring(pos + 1, keyEnd));
            } catch (final NumberFormatException e) {
                throw new IllegalStateException("Invalid leader epochs JSON: " + json, e);
            }
            pos = skipWhitespace(json, keyEnd + 1);
            if (pos >= json.length() || json.charAt(pos) != ':') {
                throw new IllegalStateException("Invalid leader epochs JSON: " + json);
            }
            pos = skipWhitespace(json, pos + 1);
            int valueEnd = pos;
            if (valueEnd < json.length() && json.charAt(valueEnd) == '-') {
                valueEnd++;
            }
            while (valueEnd < json.length() && Character.isDigit(json.charAt(valueEnd))) {
                valueEnd++;
            }
            if (valueEnd == pos) {
                throw new IllegalStateException("Invalid leader epochs JSON: " + json);
            }
            final long startOffset;
            try {
                startOffset = Long.parseLong(json.substring(pos, valueEnd));
            } catch (final NumberFormatException e) {
                throw new IllegalStateException("Invalid leader epochs JSON: " + json, e);
            }
            epochs.put(epoch, startOffset);
            pos = skipWhitespace(json, valueEnd);
            if (pos >= json.length()) {
                throw new IllegalStateException("Invalid leader epochs JSON: " + json);
            }
            final char next = json.charAt(pos);
            if (next == '}') {
                break;
            }
            if (next != ',') {
                throw new IllegalStateException("Invalid leader epochs JSON: " + json);
            }
            pos = pos + 1;
        }
        if (skipWhitespace(json, pos + 1) != json.length()) {
            throw new IllegalStateException("Invalid leader epochs JSON: " + json);
        }
        return epochs;
    }

    private static int skipWhitespace(final String json, final int pos) {
        int current = pos;
        while (current < json.length() && Character.isWhitespace(json.charAt(current))) {
            current++;
        }
        return current;
    }

    private RemoteStorageException unwrapMutationError(final Exception error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof RemoteStorageException rse) {
                return rse;
            }
            if (current instanceof IllegalArgumentException iae) {
                throw iae;
            }
            if (current instanceof IllegalStateException ise) {
                throw ise;
            }
            current = current.getCause();
        }
        return new RemoteStorageException(error);
    }
}
