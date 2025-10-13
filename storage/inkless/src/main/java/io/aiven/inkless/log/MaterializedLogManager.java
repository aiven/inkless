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
package io.aiven.inkless.log;

import java.nio.file.Path;
import java.util.Enumeration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.storage_backend.common.StorageBackend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaterializedLogManager {
    private static final Logger LOG = LoggerFactory.getLogger(MaterializedLogManager.class);

    private final KafkaScheduler unifiedLogScheduler = new KafkaScheduler(1, true);
    private final BrokerTopicStats brokerTopicStats = new BrokerTopicStats();

    private final Time time;
    private final ControlPlane controlPlane;
    private final Path materializationDirectory;
    private final ObjectFetchManager objectFetchManager;
    private final ObjectKeyCreator objectKeyCreator;
    private final LogConfig logConfig;
    private final int maxBytesInQueuePerPartition = 1 * 1024 * 1024;  // TODO configurable

    private final HighWatermarkUpdater highWatermarkUpdater;
    private final BatchFinder batchFinder;

    public MaterializedLogManager(final SharedState sharedState) {
        this(
            sharedState.time(),
            sharedState.controlPlane(),
            sharedState.config().materializationDirectory(),
            sharedState.config().storage(),
            sharedState.objectKeyCreator(),
            sharedState.config().materializationLogConfig()
        );
    }

    // Visible for testing
    MaterializedLogManager(final Time time,
                           final ControlPlane controlPlane,
                           final Path materializationDirectory,
                           final StorageBackend storage,
                           final ObjectKeyCreator objectKeyCreator,
                           final LogConfig logConfig) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane cannot be null");
        this.materializationDirectory = Objects.requireNonNull(materializationDirectory, "materializationDirectory cannot be null");
        this.objectFetchManager = new ObjectFetchManager(
            time,
            Objects.requireNonNull(storage, "storage cannot be null"),
            10,  // TODO configure
            2
        );
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");
        this.logConfig = Objects.requireNonNull(logConfig, "logConfig cannot be null");

        final long hwmDelayMs = 100;  // TODO configurable
        final int numThreadsHighWatermarkUpdater = 1;  // TODO configurable
        this.highWatermarkUpdater = new HighWatermarkUpdater(time, controlPlane, hwmDelayMs, numThreadsHighWatermarkUpdater);

        final int numThreadsBatchFinder = 1;  // TODO configurable
        this.batchFinder = new BatchFinder(time, numThreadsBatchFinder, controlPlane);
    }

    public void startReplica(final TopicIdPartition topicIdPartition) {
        LOG.error("QQQQQQQQQQ I'm replica of {}", topicIdPartition);
        final var partition = new MaterializedPartition(
            topicIdPartition,
            time,
            materializationDirectory,
            maxBytesInQueuePerPartition,
            objectKeyCreator,
            objectFetchManager,
            unifiedLogScheduler,
            brokerTopicStats,
            logConfig
        );

        highWatermarkUpdater.addPartition(partition);
        batchFinder.addPartition(partition);
    }

    public void shutdown() {
        // TODO better shutdown
        objectFetchManager.shutdown();
        highWatermarkUpdater.shutdown();
        batchFinder.shutdown();
        try {
            unifiedLogScheduler.shutdown();
        } catch (final InterruptedException e) {
            LOG.error("Interrupted", e);
        }
        brokerTopicStats.close();
    }
}
