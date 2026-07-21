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
package io.aiven.inkless.delete;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import io.aiven.inkless.cache.CrossTierLogStartCache;
import io.aiven.inkless.control_plane.AdvanceCrossTierLogStartOffsetRequest;
import io.aiven.inkless.control_plane.AdvanceCrossTierLogStartOffsetResponse;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.MetadataView;

/**
 * Reports the cross-tier (remote) log start offset of consolidating diskless partitions to the
 * control plane.
 *
 * <p>For a born-consolidated topic, only the partition's classic leader observes remote retention
 * advancing the lowest readable offset (via its {@code RemoteLogManager}). Followers fetch from the
 * diskless WAL and never learn this value, so a {@code ListOffsets(EARLIEST)} served by a follower
 * would return a stale offset. This reporter persists the leader's observation in the control plane
 * so that any broker can serve the correct value (see {@code advanceCrossTierLogStartOffset}).
 *
 * <p>Updates are buffered and flushed periodically (via {@link #run()}), coalescing all pending
 * partitions into a single control-plane call. Only strictly-advancing values are reported, since
 * remote retention is monotonic and the control plane stores the value forward-only.
 */
public class CrossTierLogStartReporter implements Runnable, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrossTierLogStartReporter.class);

    // Bounds for the lastReported dedup cache. It is a soft optimization, not a source of truth:
    // the control plane is forward-only and idempotent, so an evicted entry only costs a single
    // redundant (no-op) advance call. Bounding it keeps a long-lived broker that leads many
    // partitions over time (incl. deleted/recreated topics, which get fresh topic ids) from leaking.
    static final long LAST_REPORTED_MAX_SIZE = 10_000;
    static final Duration LAST_REPORTED_EXPIRE_AFTER_ACCESS = Duration.ofHours(1);

    private final MetadataView metadataView;
    private final ControlPlane controlPlane;
    private final CrossTierLogStartCache crossTierLogStartCache;
    private final CrossTierLogStartReporterMetrics metrics;

    // Pending updates not yet flushed to the control plane (per partition, highest reported offset).
    private final ConcurrentHashMap<TopicIdPartition, Long> pending = new ConcurrentHashMap<>();
    // Highest offset already accepted by the control plane, used to drop non-advancing reports.
    // Bounded/expiring (see above) so it can't grow without bound as leadership churns.
    private final Cache<TopicIdPartition, Long> lastReported = Caffeine.newBuilder()
        .maximumSize(LAST_REPORTED_MAX_SIZE)
        .expireAfterAccess(LAST_REPORTED_EXPIRE_AFTER_ACCESS)
        .build();

    public CrossTierLogStartReporter(final MetadataView metadataView,
                                     final ControlPlane controlPlane,
                                     final CrossTierLogStartCache crossTierLogStartCache) {
        this.metadataView = Objects.requireNonNull(metadataView, "metadataView cannot be null");
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane cannot be null");
        this.crossTierLogStartCache = Objects.requireNonNull(crossTierLogStartCache, "crossTierLogStartCache cannot be null");
        this.metrics = new CrossTierLogStartReporterMetrics(pending::size);
    }

    /**
     * Buffers a cross-tier log start offset observed on this broker (the partition's classic leader).
     *
     * <p>No-op unless the topic is a consolidating diskless topic and the value strictly advances the
     * last reported offset. Safe to call from the {@code RemoteLogManager} callback thread.
     */
    public void enqueue(final TopicPartition topicPartition, final long remoteLogStartOffset) {
        // 0 is meaningful and MUST be reported: it is the cross-tier earliest of a freshly-tiered
        // born-consolidated topic whose WAL prune frontier (log_start_offset) has since advanced above 0.
        // The EARLIEST read path returns COALESCE(remote_log_start_offset, log_start_offset) (see V17), so
        // if 0 were never written, EARLIEST would incorrectly report the pruned log_start_offset instead of
        // the true earliest still readable from remote. Do NOT change this to `<= 0`.
        if (remoteLogStartOffset < 0) {
            return;
        }
        final String topic = topicPartition.topic();
        if (!metadataView.isConsolidatingDisklessTopic(topic)) {
            return;
        }
        final Uuid topicId = metadataView.getTopicId(topic);
        if (topicId == null || topicId.equals(Uuid.ZERO_UUID)) {
            return;
        }
        final TopicIdPartition tidp = new TopicIdPartition(topicId, topicPartition.partition(), topic);

        final Long last = lastReported.getIfPresent(tidp);
        if (last != null && remoteLogStartOffset <= last) {
            return;
        }
        pending.merge(tidp, remoteLogStartOffset, Math::max);
    }

    @Override
    public void run() {
        try {
            flush();
        } catch (final Exception e) {
            LOGGER.error("Error reporting cross-tier log start offsets", e);
        }
    }

    private void flush() {
        if (pending.isEmpty()) {
            return;
        }

        // Atomically drain the currently pending updates; concurrent enqueues will be picked up next flush.
        final List<TopicIdPartition> partitions = new ArrayList<>();
        final List<AdvanceCrossTierLogStartOffsetRequest> requests = new ArrayList<>();
        for (final TopicIdPartition tidp : List.copyOf(pending.keySet())) {
            final Long offset = pending.remove(tidp);
            if (offset == null) {
                continue;
            }
            final Long last = lastReported.getIfPresent(tidp);
            if (last != null && offset <= last) {
                continue;
            }
            partitions.add(tidp);
            requests.add(new AdvanceCrossTierLogStartOffsetRequest(tidp.topicId(), tidp.partition(), offset));
        }

        if (requests.isEmpty()) {
            return;
        }

        final List<AdvanceCrossTierLogStartOffsetResponse> responses;
        try {
            responses = controlPlane.advanceCrossTierLogStartOffset(requests);
        } catch (final Exception e) {
            // Re-buffer so the next flush retries; control-plane storage is forward-only, so retries are safe.
            for (int i = 0; i < requests.size(); i++) {
                pending.merge(partitions.get(i), requests.get(i).remoteLogStartOffset(), Math::max);
            }
            metrics.recordReportError();
            throw e;
        }

        for (int i = 0; i < responses.size(); i++) {
            final TopicIdPartition tidp = partitions.get(i);
            final AdvanceCrossTierLogStartOffsetResponse response = responses.get(i);
            switch (response.errors()) {
                case NONE -> {
                    final long stored = response.remoteLogStartOffset();
                    lastReported.asMap().merge(tidp, stored, Math::max);
                    // Write-through: keep the leader's local read path from re-querying the control plane.
                    crossTierLogStartCache.put(tidp, stored);
                    metrics.recordPartitionReported();
                    LOGGER.trace("Reported cross-tier log start offset for {}: stored={}", tidp, stored);
                }
                case UNKNOWN_TOPIC_OR_PARTITION -> {
                    // Topic/partition gone (e.g. deleted); stop tracking it.
                    lastReported.invalidate(tidp);
                    LOGGER.debug("Cross-tier log start offset report for {} returned unknown topic or partition", tidp);
                }
                default -> {
                    metrics.recordReportError();
                    LOGGER.error("Cross-tier log start offset report for {} returned error: {}", tidp, response.errors());
                }
            }
        }
    }

    // Visible for testing.
    Map<TopicIdPartition, Long> pendingView() {
        return Map.copyOf(pending);
    }

    @Override
    public void close() {
        // Scheduling and lifecycle are owned externally; only the JMX metrics are owned here.
        metrics.close();
    }
}
