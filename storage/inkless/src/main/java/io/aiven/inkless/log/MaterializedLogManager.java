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
    private final ObjectFetchManager objectFetchManager;
    private final ObjectKeyCreator objectKeyCreator;
    private final ExecutorService batchRequestExecutor;
    private final ExecutorService diskWriteExecutor;

    private final HighWatermarkUpdater highWatermarkUpdater;

    private final ReentrantLock partitionEnumLock = new ReentrantLock();
    private Enumeration<TopicIdPartition> partitionEnum = null;

    public MaterializedLogManager(final SharedState sharedState) {
        this(
            sharedState.time(),
            sharedState.controlPlane(),
            sharedState.config().storage(),
            sharedState.objectKeyCreator()
        );
    }

    // Visible for testing
    MaterializedLogManager(final Time time,
                           final ControlPlane controlPlane,
                           final StorageBackend storage,
                           final ObjectKeyCreator objectKeyCreator) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.controlPlane = Objects.requireNonNull(controlPlane, "controlPlane cannot be null");
        this.objectFetchManager = new ObjectFetchManager(
            time,
            Objects.requireNonNull(storage, "storage cannot be null"),
            10,  // TODO configure
            2
        );
        this.objectKeyCreator = Objects.requireNonNull(objectKeyCreator, "objectKeyCreator cannot be null");

        final long hwmDelayMs = 100;  // TODO configurable
        this.highWatermarkUpdater = new HighWatermarkUpdater(time, controlPlane, hwmDelayMs, 1);

        // TODO configurable threads
        this.batchRequestExecutor =
            Executors.newFixedThreadPool(1, ThreadUtils.createThreadFactory("inkless-materialized-partition-batch-request-%d", true,
                (t, e) -> LOG.error("Uncaught exception in thread '{}':", t.getName(), e)));
        this.diskWriteExecutor =
            Executors.newFixedThreadPool(1, ThreadUtils.createThreadFactory("inkless-materialized-partition-disk-write-%d", true,
                (t, e) -> LOG.error("Uncaught exception in thread '{}':", t.getName(), e)));
    }

    public void startReplica(final TopicIdPartition topicIdPartition) {
        LOG.error("QQQQQQQQQQ I'm replica of {}", topicIdPartition);
        final var partition = new MaterializedPartition(
            topicIdPartition,
            time,
            controlPlane,
            batchRequestExecutor,
            diskWriteExecutor,
            objectKeyCreator,
            objectFetchManager,
            unifiedLogScheduler,
            brokerTopicStats
        );

        highWatermarkUpdater.addPartition(partition);
    }

    void shutdown() {
        // TODO better shutdown
        highWatermarkUpdater.shutdown();
        try {
            unifiedLogScheduler.shutdown();
        } catch (final InterruptedException e) {
            LOG.error("Interrupted", e);
        }
        brokerTopicStats.close();
        ThreadUtils.shutdownExecutorServiceQuietly(batchRequestExecutor, 0, TimeUnit.SECONDS);
        ThreadUtils.shutdownExecutorServiceQuietly(diskWriteExecutor, 0, TimeUnit.SECONDS);
    }
}
