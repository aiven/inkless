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
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Fails every control-plane call immediately while no control plane is ready, instead of waiting
 * for a connection to time out.
 *
 * <p>Every method forwards to whatever delegate {@link ControlPlaneDelegateReconciler} currently
 * has ready, and raises {@link ControlPlaneUnavailableException} when it has none. That is the
 * whole of this class: building the delegate, replacing it after a reconfiguration, closing the one
 * it replaced, and deciding whether the control plane counts as available all belong to the
 * reconciler. Read its documentation for how that works and what the states mean.
 *
 * <p>Most callers can skip a pre-check of their own and just try the call, catching
 * {@link ControlPlaneUnavailableException}. Keep a caller-side pre-check only where reaching this
 * gate at all is itself the expensive part, such as before buffering and uploading a produce
 * request to object storage.
 */
public class AvailabilityGatedControlPlane implements ControlPlane {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvailabilityGatedControlPlane.class);

    /**
     * How long {@link #startAndAwaitFirstBuild()} waits. A backstop, not the expected path: a
     * healthy control plane is built in well under a second, and the cost of giving up early is
     * only that the first few calls fail.
     */
    static final long FIRST_BUILD_TIMEOUT_MS = 60_000;

    private final ControlPlaneDelegateReconciler reconciler;

    public AvailabilityGatedControlPlane(final Supplier<InklessConfig> configSupplier,
                                         final Function<InklessConfig, ControlPlane> delegateFactory,
                                         final Time time) {
        this.reconciler = new ControlPlaneDelegateReconciler(configSupplier, delegateFactory, time);
    }

    public ControlPlaneAvailability availability() {
        return reconciler.availability();
    }

    /**
     * Builds the delegate, waiting for that first attempt to finish before returning.
     *
     * <p>Call this during startup, before the node begins serving requests. Building happens on the
     * reconciler's thread either way; waiting here only keeps requests from arriving while the very
     * first attempt is still running, because some of them, diskless topic creation in particular,
     * have nowhere to put a retriable error.
     *
     * <p>Returns as soon as the attempt settles, whether or not it produced a usable delegate, so an
     * unconfigured or unreachable control plane does not hold up startup. If the attempt outlasts
     * {@link #FIRST_BUILD_TIMEOUT_MS} this gives up waiting and returns, leaving it to finish in the
     * background.
     *
     * @see ControlPlaneDelegateReconciler#awaitFirstBuild(long)
     */
    public void startAndAwaitFirstBuild() {
        reconciler.start();
        try {
            if (!reconciler.awaitFirstBuild(FIRST_BUILD_TIMEOUT_MS)) {
                LOGGER.warn("Diskless control plane is still being built after {} ms; continuing startup "
                    + "without it. Calls fail until it is ready.", FIRST_BUILD_TIMEOUT_MS);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("Interrupted while waiting for the diskless control plane to be built");
        }
    }

    /**
     * Retires the current delegate so a new one is built from the current configuration, and marks
     * the availability {@link ControlPlaneAvailability.State#UNKNOWN}.
     *
     * @see ControlPlaneDelegateReconciler#invalidate()
     */
    public void invalidate() {
        reconciler.invalidate();
    }

    /**
     * Retires the current delegate and marks the availability
     * {@link ControlPlaneAvailability.State#UNAVAILABLE} immediately.
     *
     * @see ControlPlaneDelegateReconciler#takeOutOfService(ControlPlaneAvailability.UnavailableReason)
     */
    public void takeOutOfService(final ControlPlaneAvailability.UnavailableReason reason) {
        reconciler.takeOutOfService(reason);
    }

    private ControlPlane delegate() {
        return reconciler.current();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // The delegateFactory already produces a fully configured delegate; nothing to do here.
    }

    @Override
    public void close() throws IOException {
        reconciler.close();
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
