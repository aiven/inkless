/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.LogConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.EnforceRetentionRequest;
import io.aiven.inkless.control_plane.EnforceRetentionResponse;
import io.aiven.inkless.control_plane.MetadataView;

public class RetentionEnforcer implements Runnable, Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(RetentionEnforcer.class);

    private final Time time;
    private final MetadataView metadataView;
    private final ControlPlane controlPlane;
    private final RetentionEnforcementScheduler retentionEnforcementScheduler;
    private final int maxBatchesPerRequest;

    private final RetentionEnforcerMetrics metrics = new RetentionEnforcerMetrics();

    public RetentionEnforcer(final SharedState sharedState) {
        this(Objects.requireNonNull(sharedState, "sharedState cannot be null").time(),
            sharedState.metadata(),
            sharedState.controlPlane(),
            new RetentionEnforcementScheduler(
                sharedState.time(),
                sharedState.metadata(),
                sharedState.config().retentionEnforcementInterval(),
                new Random()
            ),
            sharedState.config().maxBatchesPerEnforcementRequest()
        );
    }

    // Visible for testing.
    RetentionEnforcer(final Time time,
                      final MetadataView metadataView,
                      final ControlPlane controlPlane,
                      final RetentionEnforcementScheduler retentionEnforcementScheduler,
                      final int maxBatchesPerRequest) {
        this.time = time;
        this.metadataView = metadataView;
        this.controlPlane = controlPlane;
        this.retentionEnforcementScheduler = retentionEnforcementScheduler;
        this.maxBatchesPerRequest = maxBatchesPerRequest;
    }

    @Override
    public void run() {
        try {
            runUnsafe();
        } catch (final Exception e) {
            LOGGER.error("Error enforcing retention", e);
        }
    }

    private synchronized void runUnsafe() {
        final List<EnforceRetentionRequest> requests = new ArrayList<>();
        final List<TopicIdPartition> readyPartitions = retentionEnforcementScheduler.getReadyPartitions();
        final Map<String, LogConfig> topicConfigs = new HashMap<>();
        for (final TopicIdPartition partition : readyPartitions) {
            final LogConfig topicConfig = topicConfigs.computeIfAbsent(partition.topic(),
                t -> LogConfig.fromProps(metadataView.getDefaultConfig(), metadataView.getTopicConfig(t)));

            // This check must be done here and not at scheduling, because the config may change at any moment.
            if (topicConfig.delete) {
                requests.add(new EnforceRetentionRequest(
                    partition.topicId(),
                    partition.partition(),
                    topicConfig.retentionSize,
                    topicConfig.retentionMs
                ));
            }
        }

        if (requests.isEmpty()) {
            return;
        }

        metrics.recordRetentionEnforcementStarted();

        final Instant start = TimeUtils.durationMeasurementNow(time);
        final List<EnforceRetentionResponse> responses;
        try {
            responses = controlPlane.enforceRetention(requests, maxBatchesPerRequest);
        } catch (final Exception e) {
            metrics.recordRetentionEnforcementFinishedWithError();
            LOGGER.error("Unexpected error when enforcing retention", e);
            throw new RuntimeException(e);
        }
        final Instant ended = TimeUtils.durationMeasurementNow(time);
        final long durationMs = Duration.between(start, ended).toMillis();

        LOGGER.debug("Enforcing retention for {} partitions took {} ms", requests.size(), durationMs);

        long totalBatchesDeleted = 0;
        long totalBytesDeleted = 0;
        for (int i = 0; i < responses.size(); i++) {
            final TopicIdPartition readyPartition = readyPartitions.get(i);
            final var request = requests.get(i);
            final var response = responses.get(i);

            switch (response.errors()) {
                case NONE -> {
                    LOGGER.trace("Enforcing retention for {} with retentionBytes={}, retentionMs={} completed successfully. " +
                            "{} batches and {} bytes deleted, new log start offset: {}",
                        readyPartition.topicPartition(), request.retentionBytes(), request.retentionMs(),
                        response.batchesDeleted(), response.bytesDeleted(), response.logStartOffset());
                    totalBatchesDeleted += response.batchesDeleted();
                    totalBytesDeleted += response.bytesDeleted();
                }

                case UNKNOWN_TOPIC_OR_PARTITION ->
                    // When a topic is deleted, each partition may still be attempted once.
                    // Use debug logging to not pollute the log with this normal behavior.
                    LOGGER.debug("Enforcing retention for {} with retentionBytes={}, retentionMs={} completed with error: {}",
                        readyPartition.topicPartition(), request.retentionBytes(), request.retentionMs(), response.errors());

                default ->
                    LOGGER.error("Enforcing retention for {} with retentionBytes={}, retentionMs={} completed with error: {}",
                        readyPartition.topicPartition(), request.retentionBytes(), request.retentionMs(), response.errors());
            }
        }

        metrics.recordRetentionEnforcementFinishedSuccessfully(durationMs, totalBatchesDeleted, totalBytesDeleted);
    }

    @Override
    public void close() throws IOException {
        metrics.close();
    }
}
