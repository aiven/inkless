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
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;

import io.aiven.inkless.common.ObjectFormat;

/**
 * Fails every control-plane call immediately while the control plane is reported unavailable,
 * instead of waiting for the underlying connection to time out.
 *
 * <p>The thrown {@link ControlPlaneException} is the same exception callers already handle for a
 * control-plane failure, so this changes when a call fails, not how.
 *
 * <p>The real delegate (and whatever it opens on construction, e.g. database connection pools) is
 * created lazily, on the first call made while {@link ControlPlaneAvailability.State#AVAILABLE},
 * and torn down as soon as the control plane is reported unavailable again. This way, nothing is
 * ever connected to while the management plane reports the control plane offline: not at startup,
 * and not left over from a previous available period.
 *
 * <p>Calls in flight are not serialized against teardown: {@code delegateLock} guards only
 * creating and discarding the delegate, never a call through it. A call that started just before
 * the control plane was reported unavailable therefore runs against a delegate that is being
 * closed underneath it, and fails with whatever that delegate raises. That is the point. Waiting
 * for in-flight calls to drain would block the reconfiguration thread that reports the new state
 * for as long as the control plane takes to time out, which is unbounded in exactly the situation
 * the new state describes.
 *
 * <p>{@link #configure(Map)} is a no-op: the supplied {@code delegateFactory} is expected to
 * already produce a fully configured delegate. {@link #close()} is never gated and always
 * succeeds, closing the delegate only if one has been created.
 */
public class AvailabilityGatedControlPlane implements ControlPlane {
    private final Supplier<ControlPlane> delegateFactory;
    private final ControlPlaneAvailability availability;
    private final Object delegateLock = new Object();
    private volatile ControlPlane delegate;
    private boolean closed = false;

    public AvailabilityGatedControlPlane(final Supplier<ControlPlane> delegateFactory,
                                          final ControlPlaneAvailability availability) {
        this.delegateFactory = delegateFactory;
        this.availability = availability;
        this.availability.onChange(this::onAvailabilityChanged);
    }

    private void onAvailabilityChanged(final ControlPlaneAvailability.State newState) {
        if (newState == ControlPlaneAvailability.State.AVAILABLE) {
            // Created lazily by the next call; nothing to do here.
            return;
        }
        Utils.closeQuietly(takeDelegate(), "inkless control plane");
    }

    /**
     * Returns the current delegate, creating it if needed, and throws if the control plane isn't
     * available. Holds no lock on return, so the returned delegate may be closed concurrently.
     */
    private ControlPlane delegate() {
        final ControlPlaneAvailability.State state = availability.state();
        if (state != ControlPlaneAvailability.State.AVAILABLE) {
            availability.recordGatedCall();
            throw new ControlPlaneException(
                "Control plane reported as " + state.configValue() + " by the management plane");
        }
        final ControlPlane current = delegate;
        return current != null ? current : createDelegate();
    }

    private ControlPlane createDelegate() {
        synchronized (delegateLock) {
            if (closed) {
                throw new ControlPlaneException("Control plane is closed");
            }
            final ControlPlane current = delegate;
            if (current != null) {
                return current;
            }
            // Re-check under the lock: the state may have changed while another thread was
            // building a delegate here, and whoever reported it discards what it finds.
            if (!availability.isAvailable()) {
                availability.recordGatedCall();
                throw new ControlPlaneException("Control plane became unavailable while connecting");
            }
            final ControlPlane created = delegateFactory.get();
            delegate = created;
            return created;
        }
    }

    /**
     * Detaches the current delegate so no further call picks it up, and returns it for the caller
     * to close. Returns {@code null} if no delegate is attached.
     */
    private ControlPlane takeDelegate() {
        synchronized (delegateLock) {
            final ControlPlane current = delegate;
            delegate = null;
            return current;
        }
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // The delegateFactory already produces a fully configured delegate; nothing to do here.
    }

    @Override
    public void close() throws IOException {
        final ControlPlane current;
        synchronized (delegateLock) {
            closed = true;
            current = delegate;
            delegate = null;
        }
        if (current != null) {
            current.close();
        }
    }

    @Override
    public List<CommitBatchResponse> commitFile(final String objectKey,
                                                final ObjectFormat format,
                                                final int uploaderBrokerId,
                                                final long fileSize,
                                                final List<CommitBatchRequest> batches) {
        return delegate().commitFile(objectKey, format, uploaderBrokerId, fileSize, batches);
    }

    @Override
    public List<FindBatchResponse> findBatches(final List<FindBatchRequest> findBatchRequests,
                                               final int fetchMaxBytes,
                                               final int maxBatchesPerPartition) {
        return delegate().findBatches(findBatchRequests, fetchMaxBytes, maxBatchesPerPartition);
    }

    @Override
    public void createTopicAndPartitions(final Set<CreateTopicAndPartitionsRequest> requests) {
        delegate().createTopicAndPartitions(requests);
    }

    @Override
    public List<InitDisklessLogResponse> initDisklessLog(final List<InitDisklessLogRequest> requests) {
        return delegate().initDisklessLog(requests);
    }

    @Override
    public List<RepairDisklessLogResponse> repairDisklessLog(final List<RepairDisklessLogRequest> requests) {
        return delegate().repairDisklessLog(requests);
    }

    @Override
    public List<DeleteRecordsResponse> deleteRecords(final List<DeleteRecordsRequest> requests) {
        return delegate().deleteRecords(requests);
    }

    @Override
    public void deleteTopics(final Set<Uuid> topicIds) {
        delegate().deleteTopics(topicIds);
    }

    @Override
    public List<EnforceRetentionResponse> enforceRetention(final List<EnforceRetentionRequest> requests,
                                                           final int maxBatchesPerRequest) {
        return delegate().enforceRetention(requests, maxBatchesPerRequest);
    }

    @Override
    public List<AdvanceCrossTierLogStartOffsetResponse> advanceCrossTierLogStartOffset(
        final List<AdvanceCrossTierLogStartOffsetRequest> requests) {
        return delegate().advanceCrossTierLogStartOffset(requests);
    }

    @Override
    public OptionalLong getCrossTierLogStart(final TopicIdPartition topicIdPartition) {
        return delegate().getCrossTierLogStart(topicIdPartition);
    }

    @Override
    public List<FileToDelete> getFilesToDelete(final Instant markedBefore, final int limit) {
        return delegate().getFilesToDelete(markedBefore, limit);
    }

    @Override
    public void deleteFiles(final DeleteFilesRequest request) {
        delegate().deleteFiles(request);
    }

    @Override
    public List<ListOffsetsResponse> listOffsets(final List<ListOffsetsRequest> requests) {
        return delegate().listOffsets(requests);
    }

    @Override
    public boolean isSafeToDeleteFile(final String objectKeyPath) {
        return delegate().isSafeToDeleteFile(objectKeyPath);
    }

    @Override
    public List<GetLogInfoResponse> getLogInfo(final List<GetLogInfoRequest> requests) {
        return delegate().getLogInfo(requests);
    }

    @Override
    public List<GetProducerStateResponse> getProducerState(final List<GetProducerStateRequest> requests) {
        return delegate().getProducerState(requests);
    }

    @Override
    public List<PruneDisklessLogsResponse> pruneDisklessLogs(
        final List<PruneDisklessLogsRequest> pruneDisklessLogsRequests) {
        return delegate().pruneDisklessLogs(pruneDisklessLogsRequests);
    }
}
