package io.aiven.inkless.unification;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.server.log.remote.TopicPartitionLog;
import org.apache.kafka.server.log.remote.storage.RemoteLogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * <p>
 *     RemotePartitionDispatcher is a component that provides a bridge between {@link RemoteLogManager}
 *     and the {@link ConsolidationPoolHandler}. It listens for partition change events and updates the
 *     {@link ConsolidationPoolHandler} with any change (deletes, new leaders, followers, leaving partitions).
 * </p><p>
 *     It also runs a thread pool that is used to query the RLMM for offsets. These offsets then will be used to
 *     in the {@link ConsolidationPoolHandler} as starting points for consolidation.
 * </p>
 */
public class RemotePartitionDispatcher implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemotePartitionDispatcher.class);
    private static final long TIMEOUT_SECONDS = 60;

    private final Thread.UncaughtExceptionHandler exceptionHandler = (t, e) -> LOGGER.error("Uncaught exception in remote offset dispatcher", e);
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(
        4,
        8,
        TIMEOUT_SECONDS, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        ThreadUtils.createThreadFactory("RemotePartitionDispatcher-%d", true, exceptionHandler),
        new ThreadPoolExecutor.CallerRunsPolicy()); // or custom handler
    private final RemoteLogManager remoteLogManager;
    private final ConsolidationPoolHandler consolidationPoolHandler;


    public RemotePartitionDispatcher(RemoteLogManager remoteLogManager, ConsolidationPoolHandler consolidationPoolHandler) {
        this.remoteLogManager = remoteLogManager;
        this.consolidationPoolHandler = consolidationPoolHandler;
    }

    public void applyNewLeaders(Set<TopicPartitionLog> partitions, Map<String, Uuid> topicIds) {
        applyPartitionChanges(partitions, topicIds);
    }

    public void applyNewFollowers(Set<TopicPartitionLog> partitions, Map<String, Uuid> topicIds) {
        applyPartitionChanges(partitions, topicIds);
    }

    public void applyPartitionDeletes(Set<TopicPartition> partitions, Map<String, Uuid> topicIds) {
        partitions.stream()
            .filter(partition -> topicIds.containsKey(partition.topic()))
            .map(partition -> new TopicIdPartition(topicIds.get(partition.topic()), partition))
            .forEach(partition -> consolidationPoolHandler.removePartition(partition));
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
        try {
            executor.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.warn("RemotePartitionDispatcher thread interrupted during close");
        }
        executor.shutdownNow();
    }

    private void applyPartitionChanges(Set<TopicPartitionLog> newPartitions, Map<String, Uuid> topicIds) {
        Set<TopicIdPartition> newTips = newPartitions.stream()
            .map(t -> t.topicPartition())
            .filter(t -> topicIds.containsKey(t.topic()))
            .map(tp -> new TopicIdPartition(topicIds.get(tp.topic()), tp))
            .collect(Collectors.toSet());
        removeOldPartitions(newTips);
        var offsetUpdateTasks = remoteLogManager.getRemoteOffsetUpdateTasks(newPartitions, topicIds);
        submitRemoteOffsetUpdateTasks(offsetUpdateTasks);
    }

    private void removeOldPartitions(Set<TopicIdPartition> newPartitions) {
        consolidationPoolHandler.currentPartitions().stream()
            .filter(currentPartition -> !newPartitions.contains(currentPartition))
            .forEach(currentPartition -> consolidationPoolHandler.removePartition(currentPartition));
    }

    private void submitRemoteOffsetUpdateTasks(Set<RemoteLogManager.DisklessConsolidationRemoteOffsetUpdateTask> consolidationTasks) {
        consolidationTasks.forEach(task -> {
            CompletableFuture.supplyAsync(() -> { task.run(); return task.offsetAndEpoch(); }, executor)
                .orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .whenComplete((offsetAndEpoch, e) -> {
                    if (e != null) {
                      if (e instanceof ExecutionException) {
                          LOGGER.error("Remote offset update task execution exception for {}-{}",
                              task.topicIdPartition().topic(), task.topicIdPartition().partition(), e.getCause());
                      } else {
                          LOGGER.error("Couldn't get offsets for {}-{}", task.topicIdPartition().topic(), task.topicIdPartition().partition());
                      }
                    } else if (offsetAndEpoch != null) {
                        consolidationPoolHandler.addOffsetsToPartition(task.topicIdPartition(), task.offsetAndEpoch());
                    } else {
                        LOGGER.warn("Remote offset update task returned empty result for remote offset update");
                    }
                });
        });
    }
}
