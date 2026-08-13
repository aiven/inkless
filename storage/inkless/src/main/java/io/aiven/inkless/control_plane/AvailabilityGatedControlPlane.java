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
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.config.InklessConfig;

/**
 * Fails every control-plane call immediately while no control plane is configured, instead of
 * waiting for a connection to time out.
 *
 * <p>The real delegate is created lazily, on the first call, from the configuration current at
 * that moment. {@link #invalidate()} closes it, so a configuration change takes effect on the
 * next call. Nothing is ever connected to while the connection string is empty: not at startup,
 * and not left over from a previous configuration.
 *
 * <p>{@link #invalidate()} and {@link #takeOutOfService(ControlPlaneAvailability.UnavailableReason)}
 * both close the current delegate, but they leave the availability in different states.
 * {@code invalidate()} marks it {@link ControlPlaneAvailability.State#UNKNOWN}, for a
 * reconfiguration that might still point at a working control plane: the next call is what
 * actually finds out. {@code takeOutOfService} marks it directly
 * {@link ControlPlaneAvailability.State#UNAVAILABLE}, for a reconfiguration that deliberately
 * empties the connection string. Marking it unknown in that case would let calls between the
 * takedown and the next delegate-creation attempt see a stale {@code AVAILABLE}-like state and
 * proceed to buffer and upload a produce request that can never commit.
 *
 * <p>An empty connection string surfaces as a {@link ConfigException} from the delegate factory,
 * raised while the delegate parses its configuration and before it opens a socket. That is the
 * signal for "no control plane is configured." Any other failure leaves the availability
 * {@link ControlPlaneAvailability.State#UNKNOWN}, because an unreachable control plane is a
 * different problem from an unconfigured one and must not be reported as one.
 *
 * <p>Calls in flight are not serialized against teardown: {@code delegateLock} guards only
 * creating and discarding the delegate, never a call through it. A call that started just before
 * {@link #invalidate()} therefore runs against a delegate that is being closed underneath it, and
 * fails with whatever that delegate raises. That is the point. Waiting for in-flight calls to
 * drain would block the reconfiguration thread for as long as the control plane takes to time out,
 * which is unbounded in exactly the situation that prompts the change.
 *
 * <p>Once the availability is {@link ControlPlaneAvailability.State#UNAVAILABLE}, a call fast-fails
 * without asking the delegate factory to parse the configuration again: the answer would be the
 * same {@link ConfigException} every time, until {@link #invalidate()} resets the availability to
 * {@link ControlPlaneAvailability.State#UNKNOWN}. This is what lets most callers skip a pre-check
 * of their own and just try the call, catching {@link ControlPlaneUnavailableException}. Keep a
 * caller-side pre-check only where reaching this gate at all is itself the expensive part, such as
 * before buffering and uploading a produce request to object storage.
 */
public class AvailabilityGatedControlPlane implements ControlPlane {
    private static final String NOT_CONFIGURED_MESSAGE = "No diskless control plane is configured";

    private final Supplier<InklessConfig> configSupplier;
    private final Function<InklessConfig, ControlPlane> delegateFactory;
    private final ControlPlaneAvailability availability = new ControlPlaneAvailability();
    private final Object delegateLock = new Object();
    private volatile ControlPlane delegate;
    private volatile boolean closed = false;

    public AvailabilityGatedControlPlane(final Supplier<InklessConfig> configSupplier,
                                         final Function<InklessConfig, ControlPlane> delegateFactory) {
        this.configSupplier = configSupplier;
        this.delegateFactory = delegateFactory;
    }

    public ControlPlaneAvailability availability() {
        return availability;
    }

    /**
     * Discards the current delegate so the next call rebuilds one from the current configuration.
     * Marks the availability {@link ControlPlaneAvailability.State#UNKNOWN}, not
     * {@code UNAVAILABLE}, because the new configuration might work. Returns without waiting for
     * calls already in flight.
     */
    public void invalidate() {
        availability.markUnknown();
        Utils.closeQuietly(takeDelegate(), "inkless control plane");
    }

    /**
     * Discards the current delegate and marks the availability
     * {@link ControlPlaneAvailability.State#UNAVAILABLE} immediately,
     * instead of leaving it {@code UNKNOWN} until the next call rebuilds the delegate.
     * 
     * Use this when the reconfiguration itself, not just the next call, already knows the control
     * plane is out of service, such as when the connection string is emptied.
     */
    public void takeOutOfService(final ControlPlaneAvailability.UnavailableReason reason) {
        availability.markUnavailable(reason);
        Utils.closeQuietly(takeDelegate(), "inkless control plane");
    }

    /**
     * Returns the current delegate, creating it if needed. Holds no lock on return, so the
     * returned delegate may be closed concurrently.
     */
    private ControlPlane delegate() {
        final ControlPlane current = delegate;
        if (current != null) {
            return current;
        }
        if (!closed && availability.state() == ControlPlaneAvailability.State.UNAVAILABLE) {
            // Already known unavailable: skip rebuilding from config, which would just raise the
            // same ConfigException again. This is what lets callers skip their own pre-check and
            // simply try the call, catching ControlPlaneUnavailableException.
            availability.recordGatedCall();
            throw new ControlPlaneUnavailableException(NOT_CONFIGURED_MESSAGE);
        }
        return createDelegate();
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
            final ControlPlane created;
            try {
                created = delegateFactory.apply(configSupplier.get());
            } catch (final ConfigException e) {
                availability.markUnavailable(ControlPlaneAvailability.UnavailableReason.NOT_CONFIGURED);
                availability.recordGatedCall();
                throw new ControlPlaneUnavailableException(NOT_CONFIGURED_MESSAGE, e);
            }
            delegate = created;
            availability.markAvailable();
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
        Utils.closeQuietly(availability, "inkless control plane availability");
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
    public PurgeDeletedLogsResponse purgeDeletedLogs(final int maxBatches) {
        return delegate().purgeDeletedLogs(maxBatches);
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
