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
 * GNU Affero General Public License for the details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.metrics;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.storage.internals.log.LogMetricNames;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.MetadataView;

/**
 * Exposes JMX metrics for diskless (Inkless) partitions that mirror the classic
 * {@code kafka.log:type=Log} metrics (LogStartOffset, LogEndOffset, Size). Values are
 * sourced from the Control Plane and tagged with topic and partition so existing
 * tooling and dashboards work unchanged.
 */
public final class InklessLogMetrics implements Runnable, Closeable {

    private static final String METRICS_GROUP_PKG = "kafka.log";
    private static final String METRICS_GROUP_TYPE = "Log";

    private final ControlPlane controlPlane;
    private final MetadataView metadataView;
    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(METRICS_GROUP_PKG, METRICS_GROUP_TYPE);

    /** Cache of log info per partition, updated by {@link #run()}. */
    private final ConcurrentHashMap<TopicIdPartition, LogInfoSnapshot> cache = new ConcurrentHashMap<>();

    /** Partitions that currently have gauges registered, for diffing on refresh. */
    private final Set<TopicIdPartition> registeredPartitions = ConcurrentHashMap.newKeySet();

    public InklessLogMetrics(final SharedState sharedState) {
        this(sharedState.controlPlane(), sharedState.metadata());
    }

    /**
     * Constructor for tests; avoids building a full SharedState.
     */
    InklessLogMetrics(final ControlPlane controlPlane, final MetadataView metadataView) {
        this.controlPlane = controlPlane;
        this.metadataView = metadataView;
        run();
    }

    @Override
    public void run() {
        final Set<TopicIdPartition> currentSet = metadataView.getDisklessTopicPartitions();
        if (currentSet.isEmpty()) {
            removeGaugesForPartitions(registeredPartitions);
            registeredPartitions.clear();
            cache.clear();
            return;
        }

        final List<TopicIdPartition> currentOrdered = new ArrayList<>(currentSet);
        final List<GetLogInfoRequest> requests = new ArrayList<>();
        for (final TopicIdPartition tp : currentOrdered) {
            requests.add(new GetLogInfoRequest(tp.topicId(), tp.partition()));
        }
        final List<GetLogInfoResponse> responses = controlPlane.getLogInfo(requests);

        final Map<TopicIdPartition, LogInfoSnapshot> newCache = new ConcurrentHashMap<>();
        for (int i = 0; i < currentOrdered.size(); i++) {
            final TopicIdPartition tp = currentOrdered.get(i);
            final GetLogInfoResponse r = responses.get(i);
            final long logStartOffset = r.errors() != Errors.NONE
                ? GetLogInfoResponse.INVALID_OFFSET
                : r.logStartOffset();
            final long highWatermark = r.errors() != Errors.NONE
                ? GetLogInfoResponse.INVALID_OFFSET
                : r.highWatermark();
            final long byteSize = r.errors() != Errors.NONE
                ? GetLogInfoResponse.INVALID_BYTE_SIZE
                : r.byteSize();
            newCache.put(tp, new LogInfoSnapshot(logStartOffset, highWatermark, byteSize));
        }
        cache.clear();
        cache.putAll(newCache);

        for (final TopicIdPartition tp : currentSet) {
            if (!registeredPartitions.contains(tp)) {
                registerGaugesForPartition(tp);
                registeredPartitions.add(tp);
            }
        }
        final Set<TopicIdPartition> toRemove = new java.util.HashSet<>(registeredPartitions);
        toRemove.removeAll(currentSet);
        removeGaugesForPartitions(toRemove);
        registeredPartitions.retainAll(currentSet);
    }

    private static Map<String, String> tagsFor(final TopicIdPartition tp) {
        final Map<String, String> tags = new LinkedHashMap<>();
        final String topic = tp.topic();
        tags.put("topic", topic != null ? topic : "unknown");
        tags.put("partition", String.valueOf(tp.partition()));
        return tags;
    }

    private void registerGaugesForPartition(final TopicIdPartition tp) {
        final Map<String, String> tags = tagsFor(tp);
        metricsGroup.newGauge(LogMetricNames.LOG_START_OFFSET, () -> {
            final LogInfoSnapshot s = cache.get(tp);
            return s != null ? s.logStartOffset : GetLogInfoResponse.INVALID_OFFSET;
        }, tags);
        metricsGroup.newGauge(LogMetricNames.LOG_END_OFFSET, () -> {
            final LogInfoSnapshot s = cache.get(tp);
            return s != null ? s.highWatermark : GetLogInfoResponse.INVALID_OFFSET;
        }, tags);
        metricsGroup.newGauge(LogMetricNames.SIZE, () -> {
            final LogInfoSnapshot s = cache.get(tp);
            return s != null ? s.byteSize : GetLogInfoResponse.INVALID_BYTE_SIZE;
        }, tags);
    }

    private void removeGaugesForPartitions(final Set<TopicIdPartition> partitions) {
        for (final TopicIdPartition tp : partitions) {
            final Map<String, String> tags = tagsFor(tp);
            metricsGroup.removeMetric(LogMetricNames.LOG_START_OFFSET, tags);
            metricsGroup.removeMetric(LogMetricNames.LOG_END_OFFSET, tags);
            metricsGroup.removeMetric(LogMetricNames.SIZE, tags);
        }
    }

    @Override
    public void close() throws IOException {
        removeGaugesForPartitions(Set.copyOf(registeredPartitions));
        registeredPartitions.clear();
        cache.clear();
    }

    private record LogInfoSnapshot(long logStartOffset, long highWatermark, long byteSize) {}
}
