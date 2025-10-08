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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.KafkaScheduler;
import org.apache.kafka.storage.internals.log.LogAppendInfo;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogDirFailureChannel;
import org.apache.kafka.storage.internals.log.ProducerStateManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.consume.FetchCompleter;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.storage_backend.common.StorageBackend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaterializedLogManager {
    private static final Logger LOG = LoggerFactory.getLogger(MaterializedLogManager.class);

    private final KafkaScheduler unifiedLogScheduler = new KafkaScheduler(1, true);
    private final BrokerTopicStats brokerTopicStats = new BrokerTopicStats();
    private final ProducerStateManagerConfig producerStateManagerConfig = new ProducerStateManagerConfig(1, false);
    private final LogDirFailureChannel logDirFailureChannel = new LogDirFailureChannel(1);

    private final Time time;
    private final ControlPlane controlPlane;
    private final ObjectFetchManager objectFetchManager;
    private final ObjectKeyCreator objectKeyCreator;
    private final ScheduledExecutorService pool;

    private final HighWatermarkUpdater highWatermarkUpdater;
    private final BatchRequester batchRequester;

//    private final ConcurrentHashMap<TopicIdPartition, MaterializedPartition> partitions = new ConcurrentHashMap<>();

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

        final int numThreads = 1;  // TODO config
        this.pool = Executors.newScheduledThreadPool(
            numThreads,
            ThreadUtils.createThreadFactory("inkless-materialized-log-manager-%d", true,
                (t, e) -> LOG.error("Uncaught exception in thread '{}':", t.getName(), e)));
//        for (int i = 0; i < numThreads; i++) {
//            pool.scheduleWithFixedDelay(this::iteration, 0, 1, TimeUnit.MILLISECONDS);
//        }
//        LOG.error("QQQQQQQQQQ Scheduled {} threads", numThreads);

        final long hwmDelayMs = 100;  // TODO configurable
        this.highWatermarkUpdater = new HighWatermarkUpdater(time, controlPlane, hwmDelayMs, 1);
        this.batchRequester = new BatchRequester(time, controlPlane, objectKeyCreator, objectFetchManager, 1);
    }

//    private void iteration() {
//        try {
//            final TopicIdPartition tidp;
//            partitionEnumLock.lock();
//            try {
//                if (partitionEnum == null || !partitionEnum.hasMoreElements()) {
//                    partitionEnum = partitions.keys();
//                }
//                if (partitionEnum.hasMoreElements()) {
//                    tidp = partitionEnum.nextElement();
//                } else {
//                    LOG.error("QQQQQQQQQQ No partitions yet");
//                    return;
//                }
//            } finally {
//                partitionEnumLock.unlock();
//            }
////            LOG.error("QQQQQQQQQQ Looking at {}", tidp);
//
//            final MaterializedPartition partition = partitions.get(tidp);
//            partition.lock();
//            try {
//                if (partition.currentOffset == -1) {
//                    final List<GetLogInfoResponse> logInfo =
//                        controlPlane.getLogInfo(List.of(new GetLogInfoRequest(tidp.topicId(), tidp.partition())));
//                    partition.currentOffset = logInfo.get(0).highWatermark();
//                }
//
//                final long now = time.milliseconds();
//                // TODO better form of throttling.
//                final boolean enoughTimePassed = now - partition.lastCheckTimestamp > 1000;
//                if (partition.taskQueue.isEmpty() && enoughTimePassed) {
//                    LOG.error("QQQQQQQQQQ No running futures for {}, looking for batches from offset {}", tidp, partition.currentOffset);
//
//                    // TODO configure
//                    final int requestSize = 2 * 1024 * 1024;
//                    final FindBatchResponse batches = controlPlane.findBatches(
//                        List.of(new FindBatchRequest(tidp, partition.currentOffset, requestSize)), requestSize, 0
//                    ).get(0);
//                    partition.lastCheckTimestamp = now;
//
//                    if (batches.errors().code() == Errors.NONE.code()) {
//                        LOG.error("QQQQQQQQQQ Found {} batches in {}, requesting download", batches.batches().size(), tidp);
//                        for (final BatchInfo batch : batches.batches()) {
//                            final ObjectFetchTask task = objectFetchManager.request(objectKeyCreator.from(batch.objectKey()), batch, batch.metadata().range());
//                            partition.taskQueue.add(task);
//                        }
//                    } else {
//                        LOG.error("QQQQQQQQQQ Error reading partition {}: {}", tidp, batches.errors());
//                        throw new RuntimeException();
//                    }
//                } else {
//                    if (partition.taskQueue.peek() != null && partition.taskQueue.peek().future().isDone()) {
//                        LOG.error("QQQQQQQQQQ Future for finished {}", tidp);
//                        final ObjectFetchTask task = partition.taskQueue.poll();
//                        final ByteBuffer byteBuffer;
//                        try {
//                            byteBuffer = task.future().get();
//                        } catch (final InterruptedException | ExecutionException e) {
//                            // TODO handle gracefully
//                            LOG.error("QQQQQQQQQQ Error processing future in {}", tidp, e);
//                            throw new RuntimeException(e);
//                        }
//                        LOG.error("QQQQQQQQQQ Received {} for {}", byteBuffer, tidp);
//
//                        if (!partition.initialized()) {
//                            partition.initLog(task.batchInfo().metadata().baseOffset());
//                        }
//
//                        // TODO move it to more suitable place
//                        final MemoryRecords records = FetchCompleter.createMemoryRecords(byteBuffer, task.batchInfo());
//                        final LogAppendInfo logAppendInfo = partition.log.appendAsFollower(records, 0);
//                        LOG.error("QQQQQQQQQQ Appended: {}", logAppendInfo);
//                        partition.currentOffset = partition.log.logEndOffset();
//                    }
//                }
//            } finally {
//                partition.unlock();
//            }
//        } catch (final RuntimeException e) {
//            LOG.error("QQQQQQQQQQ Error in iteration", e);
//        }
//    }

    public void startReplica(final TopicIdPartition topicIdPartition) {
        LOG.error("QQQQQQQQQQ I'm replica of {}", topicIdPartition);
        final var partition = new MaterializedPartition(topicIdPartition);
//        partitions.computeIfAbsent(topicIdPartition, _ignored -> partition);
        highWatermarkUpdater.addPartition(partition);
        batchRequester.addPartition(partition);
    }

//    private class MaterializedPartition {
//        private final TopicIdPartition tidp;
//        private final ReentrantLock lock = new ReentrantLock();
//        long currentOffset = -1;
//        final ConcurrentLinkedQueue<ObjectFetchTask> taskQueue = new ConcurrentLinkedQueue<>();
//        long lastCheckTimestamp = -1;
//
//        private UnifiedLog log = null;
//
//        private MaterializedPartition(final TopicIdPartition tidp) {
//            this.tidp = Objects.requireNonNull(tidp, "tidp cannot be null");
//        }
//
//        private boolean initialized() {
//            return log != null;
//        }
//
//        private void initLog(final long logStartOffset) {
//            final File dir = new File(String.format("_unified_log/%s-%d", tidp.topicPartition().topic(), tidp.partition()));
//            try {
//                // Always start anew
//                // TODO optimize, check HWM
//                Utils.delete(dir);
//                dir.mkdir();
//
//                // TODO retention, segment size
//                final LogConfig logConfig = LogConfig.fromProps(Map.of(), new Properties());
//                final long recoveryPoint = logStartOffset;  // TODO change this is we start not from beginning
//                this.log = UnifiedLog.create(dir, logConfig, logStartOffset, recoveryPoint, unifiedLogScheduler,
//                    brokerTopicStats, time, 1, producerStateManagerConfig,
//                    1, logDirFailureChannel, true, Optional.of(this.tidp.topicId()));
//
//                this.currentOffset = log.logEndOffset();
//
//                LOG.error("QQQQQQQQQQ Initialized log {} with offset {}", tidp, currentOffset);
//            } catch (final IOException e) {
//                LOG.error("QQQQQQQQQQ Error in {}", tidp, e);
//                throw new RuntimeException(e);
//            }
//        }
//
//        private void lock() {
//            // TODO interruptibly?
//            lock.lock();
//        }
//
//        public void unlock() {
//            lock.unlock();
//        }
//    }

    void shutdown() {
        highWatermarkUpdater.shutdown();
        batchRequester.shutdown();
    }
}
