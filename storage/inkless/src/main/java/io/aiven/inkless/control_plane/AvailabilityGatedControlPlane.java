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
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;

import io.aiven.inkless.common.ObjectFormat;

/**
 * Fails every control-plane call immediately while the control plane is reported unavailable,
 * instead of waiting for the underlying connection to time out.
 *
 * <p>The thrown {@link ControlPlaneException} is the same exception callers already handle for a
 * control-plane failure, so this changes when a call fails, not how.
 *
 * <p>{@link #configure(Map)} and {@link #close()} are never gated: shutdown must work while the
 * control plane is offline.
 */
public class AvailabilityGatedControlPlane implements ControlPlane {
    private final ControlPlane delegate;
    private final ControlPlaneAvailability availability;

    public AvailabilityGatedControlPlane(final ControlPlane delegate, final ControlPlaneAvailability availability) {
        this.delegate = delegate;
        this.availability = availability;
    }

    private void ensureAvailable() {
        final ControlPlaneAvailability.State state = availability.state();
        if (state != ControlPlaneAvailability.State.AVAILABLE) {
            availability.recordGatedCall();
            throw new ControlPlaneException(
                "Control plane reported as " + state.configValue() + " by the management plane");
        }
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        delegate.configure(configs);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public List<CommitBatchResponse> commitFile(final String objectKey,
                                                final ObjectFormat format,
                                                final int uploaderBrokerId,
                                                final long fileSize,
                                                final List<CommitBatchRequest> batches) {
        ensureAvailable();
        return delegate.commitFile(objectKey, format, uploaderBrokerId, fileSize, batches);
    }

    @Override
    public List<FindBatchResponse> findBatches(final List<FindBatchRequest> findBatchRequests,
                                               final int fetchMaxBytes,
                                               final int maxBatchesPerPartition) {
        ensureAvailable();
        return delegate.findBatches(findBatchRequests, fetchMaxBytes, maxBatchesPerPartition);
    }

    @Override
    public void createTopicAndPartitions(final Set<CreateTopicAndPartitionsRequest> requests) {
        ensureAvailable();
        delegate.createTopicAndPartitions(requests);
    }

    @Override
    public List<InitDisklessLogResponse> initDisklessLog(final List<InitDisklessLogRequest> requests) {
        ensureAvailable();
        return delegate.initDisklessLog(requests);
    }

    @Override
    public List<RepairDisklessLogResponse> repairDisklessLog(final List<RepairDisklessLogRequest> requests) {
        ensureAvailable();
        return delegate.repairDisklessLog(requests);
    }

    @Override
    public List<DeleteRecordsResponse> deleteRecords(final List<DeleteRecordsRequest> requests) {
        ensureAvailable();
        return delegate.deleteRecords(requests);
    }

    @Override
    public void deleteTopics(final Set<Uuid> topicIds) {
        ensureAvailable();
        delegate.deleteTopics(topicIds);
    }

    @Override
    public List<EnforceRetentionResponse> enforceRetention(final List<EnforceRetentionRequest> requests,
                                                           final int maxBatchesPerRequest) {
        ensureAvailable();
        return delegate.enforceRetention(requests, maxBatchesPerRequest);
    }

    @Override
    public List<AdvanceCrossTierLogStartOffsetResponse> advanceCrossTierLogStartOffset(
        final List<AdvanceCrossTierLogStartOffsetRequest> requests) {
        ensureAvailable();
        return delegate.advanceCrossTierLogStartOffset(requests);
    }

    @Override
    public OptionalLong getCrossTierLogStart(final TopicIdPartition topicIdPartition) {
        ensureAvailable();
        return delegate.getCrossTierLogStart(topicIdPartition);
    }

    @Override
    public List<FileToDelete> getFilesToDelete(final Instant markedBefore, final int limit) {
        ensureAvailable();
        return delegate.getFilesToDelete(markedBefore, limit);
    }

    @Override
    public void deleteFiles(final DeleteFilesRequest request) {
        ensureAvailable();
        delegate.deleteFiles(request);
    }

    @Override
    public List<ListOffsetsResponse> listOffsets(final List<ListOffsetsRequest> requests) {
        ensureAvailable();
        return delegate.listOffsets(requests);
    }

    @Override
    public boolean isSafeToDeleteFile(final String objectKeyPath) {
        ensureAvailable();
        return delegate.isSafeToDeleteFile(objectKeyPath);
    }

    @Override
    public List<GetLogInfoResponse> getLogInfo(final List<GetLogInfoRequest> requests) {
        ensureAvailable();
        return delegate.getLogInfo(requests);
    }

    @Override
    public List<GetProducerStateResponse> getProducerState(final List<GetProducerStateRequest> requests) {
        ensureAvailable();
        return delegate.getProducerState(requests);
    }

    @Override
    public List<PruneDisklessLogsResponse> pruneDisklessLogs(
        final List<PruneDisklessLogsRequest> pruneDisklessLogsRequests) {
        ensureAvailable();
        return delegate.pruneDisklessLogs(pruneDisklessLogsRequests);
    }
}
