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
package io.aiven.inkless.remote_log_metadata.postgres;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Monitorable;
import org.apache.kafka.common.metrics.PluginMetrics;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemotePartitionDeleteMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.slf4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.postgres.JobUtils;

/**
 * A {@link RemoteLogMetadataManager} that stores remote log segment metadata in PostgreSQL.
 *
 * <p>PostgreSQL is the source of truth: this manager doesn't keep a broker-local metadata cache,
 * so every read performs a round trip. Concurrent brokers serialize on the partition row,
 * so concurrent mutations are safe at any replica count.
 */
public final class PostgresRemoteLogMetadataManager implements RemoteLogMetadataManager, Monitorable {
    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30);
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);

    private final Time time;
    private final Duration closeTimeout;
    private final Logger log;
    private final Set<TopicIdPartition> activePartitions = ConcurrentHashMap.newKeySet();
    private final Set<CompletableFuture<Void>> pendingMutations = ConcurrentHashMap.newKeySet();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicBoolean configured = new AtomicBoolean(false);
    private final AtomicBoolean closing = new AtomicBoolean(false);

    private volatile RemoteLogMetadataDao dao;
    private volatile HikariDataSource dataSource;
    private volatile ExecutorService mutationExecutor;
    private volatile String clusterId;
    private volatile PostgresRemoteLogMetadataMetrics metrics;

    public PostgresRemoteLogMetadataManager() {
        this(Time.SYSTEM, DEFAULT_CLOSE_TIMEOUT);
    }

    PostgresRemoteLogMetadataManager(final Time time) {
        this(time, DEFAULT_CLOSE_TIMEOUT);
    }

    PostgresRemoteLogMetadataManager(final Time time, final Duration closeTimeout) {
        this.time = Objects.requireNonNull(time, "time must not be null");
        this.closeTimeout = Objects.requireNonNull(closeTimeout, "closeTimeout must not be null");
        if (closeTimeout.isNegative()) {
            throw new IllegalArgumentException("closeTimeout must not be negative");
        }
        this.log = new LogContext(
            String.format("[PostgresRemoteLogMetadataManager id=%d] ", ID_GENERATOR.getAndIncrement())).logger(
            PostgresRemoteLogMetadataManager.class);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs must not be null");
        lock.writeLock().lock();
        try {
            if (closing.get()) {
                throw new IllegalStateException("PostgresRemoteLogMetadataManager is closing");
            }
            if (!configured.compareAndSet(false, true)) {
                log.info("Already configured, skipping");
                return;
            }
            try {
                configureOrThrow(configs);
            } catch (final RuntimeException e) {
                try {
                    resetAfterConfigureFailure();
                } catch (final RuntimeException cleanupError) {
                    e.addSuppressed(cleanupError);
                } finally {
                    configured.set(false);
                }
                throw e;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void configureOrThrow(final Map<String, ?> configs) {
        final PostgresRemoteLogMetadataConfig rlmmConfig = new PostgresRemoteLogMetadataConfig(configs);
        final Object clusterIdValue = configs.get("cluster.id");
        if (!(clusterIdValue instanceof String) || ((String) clusterIdValue).isEmpty()) {
            throw new ConfigException("cluster.id is required to configure PostgresRemoteLogMetadataManager");
        }
        clusterId = (String) clusterIdValue;
        try {
            RlmmMigrations.migrate(rlmmConfig);
        } catch (final Exception e) {
            throw new KafkaException("PostgresRemoteLogMetadataManager failed to migrate the metadata schema", e);
        }
        try {
            dataSource = new HikariDataSource(poolConfig(rlmmConfig));
        } catch (final Exception e) {
            throw new KafkaException("PostgresRemoteLogMetadataManager failed to create the PostgreSQL pool", e);
        }
        final DSLContext context = DSL.using(dataSource, SQLDialect.POSTGRES);
        dao = new RemoteLogMetadataDao(context, time, clusterId);
        final AtomicInteger threadIndex = new AtomicInteger(0);
        if (rlmmConfig.mutationThreads() > rlmmConfig.maxConnections()) {
            log.warn("Configured {} mutation threads with only {} database connections; "
                    + "mutations and concurrent reads may contend for pool capacity",
                rlmmConfig.mutationThreads(), rlmmConfig.maxConnections());
        }
        mutationExecutor = Executors.newFixedThreadPool(rlmmConfig.mutationThreads(), runnable -> {
            final Thread thread = new Thread(runnable,
                "pg-rlmm-mutation-" + threadIndex.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        });
        log.info("Configured PostgresRemoteLogMetadataManager with {} mutation threads, pool max size {}",
            rlmmConfig.mutationThreads(), rlmmConfig.maxConnections());
    }

    @Override
    public CompletableFuture<Void> addRemoteLogSegmentMetadata(final RemoteLogSegmentMetadata segmentMetadata)
        throws RemoteStorageException {
        Objects.requireNonNull(segmentMetadata, "segmentMetadata must not be null");
        if (segmentMetadata.state() != RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
            throw new IllegalArgumentException(
                "addRemoteLogSegmentMetadata called with a segment in unexpected state: " + segmentMetadata.state());
        }
        ensureReadyForUse();
        final CompletableFuture<Void> future = new CompletableFuture<>();
        submitMutation(future, () -> {
            final MutationOutcome outcome = runJob(() -> dao.addSegment(segmentMetadata), this::recordAddSegment);
            recordTransitionConflict(outcome);
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> updateRemoteLogSegmentMetadata(final RemoteLogSegmentMetadataUpdate segmentMetadataUpdate)
        throws RemoteStorageException {
        Objects.requireNonNull(segmentMetadataUpdate, "segmentMetadataUpdate must not be null");
        if (segmentMetadataUpdate.state() == RemoteLogSegmentState.COPY_SEGMENT_STARTED) {
            throw new IllegalArgumentException(
                "updateRemoteLogSegmentMetadata called with state COPY_SEGMENT_STARTED");
        }
        ensureReadyForUse();
        final CompletableFuture<Void> future = new CompletableFuture<>();
        submitMutation(future, () -> {
            final MutationOutcome outcome = runJob(() -> dao.applyUpdate(segmentMetadataUpdate), this::recordUpdateSegment);
            recordTransitionConflict(outcome);
        });
        return future;
    }

    @Override
    public CompletableFuture<Void> putRemotePartitionDeleteMetadata(final RemotePartitionDeleteMetadata partitionDeleteMetadata)
        throws RemoteStorageException {
        Objects.requireNonNull(partitionDeleteMetadata, "partitionDeleteMetadata must not be null");
        ensureReadyForUse();
        final CompletableFuture<Void> future = new CompletableFuture<>();
        submitMutation(future, () -> {
            final MutationOutcome outcome = runJob(() -> dao.putPartitionDelete(partitionDeleteMetadata), this::recordPutPartitionDelete);
            recordTransitionConflict(outcome);
        });
        return future;
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> remoteLogSegmentMetadata(final TopicIdPartition topicIdPartition,
                                                                        final int epochForOffset,
                                                                        final long offset)
        throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition must not be null");
        return runReadJob(() -> dao.findSegment(topicIdPartition, epochForOffset, offset), this::recordLookupSegment);
    }

    @Override
    public Optional<RemoteLogSegmentMetadata> nextSegmentWithTxnIndex(final TopicIdPartition topicIdPartition,
                                                                       final int epoch,
                                                                       final long offset)
        throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition must not be null");
        return runReadJob(() -> dao.nextSegmentWithTxnIndex(topicIdPartition, epoch, offset), this::recordNextTxnSegment);
    }

    @Override
    public Optional<Long> highestOffsetForEpoch(final TopicIdPartition topicIdPartition, final int leaderEpoch)
        throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition must not be null");
        return runReadJob(() -> dao.highestOffsetForEpoch(topicIdPartition, leaderEpoch), this::recordHighestOffset);
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(final TopicIdPartition topicIdPartition)
        throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition must not be null");
        return runReadJob(() -> dao.listSegments(topicIdPartition), this::recordListSegments).iterator();
    }

    @Override
    public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(final TopicIdPartition topicIdPartition, final int leaderEpoch)
        throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition must not be null");
        return runReadJob(() -> dao.listSegments(topicIdPartition, leaderEpoch), this::recordListSegments).iterator();
    }

    @Override
    public long remoteLogSize(final TopicIdPartition topicIdPartition, final int leaderEpoch) throws RemoteStorageException {
        Objects.requireNonNull(topicIdPartition, "topicIdPartition must not be null");
        return runReadJob(() -> dao.remoteLogSize(topicIdPartition, leaderEpoch), this::recordRemoteLogSize);
    }

    @Override
    public void onPartitionLeadershipChanges(final Set<TopicIdPartition> leaderPartitions,
                                             final Set<TopicIdPartition> followerPartitions) {
        Objects.requireNonNull(leaderPartitions, "leaderPartitions must not be null");
        Objects.requireNonNull(followerPartitions, "followerPartitions must not be null");
        lock.readLock().lock();
        try {
            if (closing.get()) {
                throw new IllegalStateException("PostgresRemoteLogMetadataManager is closing");
            }
            activePartitions.addAll(leaderPartitions);
            activePartitions.addAll(followerPartitions);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean isReady(final TopicIdPartition topicIdPartition) {
        return configured.get() && !closing.get() && activePartitions.contains(topicIdPartition);
    }

    @Override
    public void onStopPartitions(final Set<TopicIdPartition> partitions) {
        Objects.requireNonNull(partitions, "partitions must not be null");
        lock.readLock().lock();
        try {
            if (closing.get()) {
                throw new IllegalStateException("PostgresRemoteLogMetadataManager is closing");
            }
            activePartitions.removeAll(partitions);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (!closing.compareAndSet(false, true)) {
                return;
            }
            final ExecutorService executor = mutationExecutor;
            if (executor != null) {
                executor.shutdown();
                try {
                    if (!executor.awaitTermination(closeTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                        log.warn("Mutation executor did not drain within {}, interrupting remaining tasks", closeTimeout);
                        executor.shutdownNow();
                        failPendingMutations();
                        if (!executor.awaitTermination(closeTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
                            log.error("Mutation executor still did not terminate after {}", closeTimeout);
                        }
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    executor.shutdownNow();
                    failPendingMutations();
                }
            }
            final HikariDataSource source = dataSource;
            if (source != null) {
                source.close();
            }
            mutationExecutor = null;
            dao = null;
            dataSource = null;
            activePartitions.clear();
            metrics = null;
            log.info("Closed PostgresRemoteLogMetadataManager");
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void withPluginMetrics(final PluginMetrics pluginMetrics) {
        Objects.requireNonNull(pluginMetrics, "pluginMetrics must not be null");
        lock.readLock().lock();
        try {
            if (closing.get()) {
                throw new IllegalStateException("PostgresRemoteLogMetadataManager is closing");
            }
            metrics = new PostgresRemoteLogMetadataMetrics(pluginMetrics, activePartitions::size,
                () -> dataSource == null ? null : dataSource.getHikariPoolMXBean());
        } finally {
            lock.readLock().unlock();
        }
    }

    private void ensureReadyForUse() {
        if (!configured.get() || closing.get()) {
            throw new IllegalStateException(
                "PostgresRemoteLogMetadataManager is not ready: configured=" + configured.get()
                    + " closing=" + closing.get());
        }
    }

    private void submitMutation(final CompletableFuture<Void> future, final MutationTask task) {
        lock.readLock().lock();
        try {
            ensureReadyForUse();
            final ExecutorService executor = mutationExecutor;
            if (executor == null) {
                throw new IllegalStateException("PostgresRemoteLogMetadataManager is not configured");
            }
            pendingMutations.add(future);
            try {
                executor.execute(() -> {
                    try {
                        task.run();
                        future.complete(null);
                    } catch (final Throwable t) {
                        // runJob already recorded the error. Complete the future with it.
                        future.completeExceptionally(t);
                    } finally {
                        pendingMutations.remove(future);
                    }
                });
            } catch (final RuntimeException e) {
                pendingMutations.remove(future);
                if (e instanceof RejectedExecutionException) {
                    throw new IllegalStateException("PostgresRemoteLogMetadataManager is closing", e);
                }
                throw e;
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    private void failPendingMutations() {
        final IllegalStateException failure =
            new IllegalStateException("PostgresRemoteLogMetadataManager closed before the mutation completed");
        pendingMutations.forEach(future -> future.completeExceptionally(failure));
        pendingMutations.clear();
    }

    private void resetAfterConfigureFailure() {
        final ExecutorService executor = mutationExecutor;
        if (executor != null) {
            executor.shutdownNow();
        }
        mutationExecutor = null;
        dao = null;
        final HikariDataSource source = dataSource;
        if (source != null) {
            source.close();
        }
        dataSource = null;
        clusterId = null;
    }

    private <T> T runJob(final Callable<T> task, final Consumer<Long> latencyCallback) throws RemoteStorageException {
        try {
            return JobUtils.run(task, time, latencyCallback);
        } catch (final RuntimeException e) {
            final Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (cause instanceof RemoteStorageException rse) {
                recordError(rse);
                throw rse;
            }
            // Programming errors surface unwrapped so callers see the real cause.
            if (cause instanceof IllegalArgumentException iae) {
                recordError(iae);
                throw iae;
            }
            if (cause instanceof IllegalStateException ise) {
                recordError(ise);
                throw ise;
            }
            final RemoteStorageException wrapped = new RemoteStorageException(cause);
            recordError(wrapped);
            throw wrapped;
        }
    }

    private <T> T runReadJob(final Callable<T> task, final Consumer<Long> latencyCallback)
        throws RemoteStorageException {
        lock.readLock().lock();
        try {
            ensureReadyForUse();
            return runJob(task, latencyCallback);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void recordAddSegment(final Long durationMs) {
        final PostgresRemoteLogMetadataMetrics current = metrics;
        if (current != null) {
            current.recordAddSegment(durationMs);
        }
    }

    private void recordUpdateSegment(final Long durationMs) {
        final PostgresRemoteLogMetadataMetrics current = metrics;
        if (current != null) {
            current.recordUpdateSegment(durationMs);
        }
    }

    private void recordPutPartitionDelete(final Long durationMs) {
        final PostgresRemoteLogMetadataMetrics current = metrics;
        if (current != null) {
            current.recordPutPartitionDelete(durationMs);
        }
    }

    private void recordLookupSegment(final Long durationMs) {
        final PostgresRemoteLogMetadataMetrics current = metrics;
        if (current != null) {
            current.recordLookupSegment(durationMs);
        }
    }

    private void recordNextTxnSegment(final Long durationMs) {
        final PostgresRemoteLogMetadataMetrics current = metrics;
        if (current != null) {
            current.recordNextTxnSegment(durationMs);
        }
    }

    private void recordHighestOffset(final Long durationMs) {
        final PostgresRemoteLogMetadataMetrics current = metrics;
        if (current != null) {
            current.recordHighestOffset(durationMs);
        }
    }

    private void recordListSegments(final Long durationMs) {
        final PostgresRemoteLogMetadataMetrics current = metrics;
        if (current != null) {
            current.recordListSegments(durationMs);
        }
    }

    private void recordRemoteLogSize(final Long durationMs) {
        final PostgresRemoteLogMetadataMetrics current = metrics;
        if (current != null) {
            current.recordRemoteLogSize(durationMs);
        }
    }

    private void recordTransitionConflict(final MutationOutcome outcome) {
        final PostgresRemoteLogMetadataMetrics current = metrics;
        if (outcome == MutationOutcome.INVALID_TRANSITION && current != null) {
            current.recordTransitionConflict();
        }
    }

    private void recordError(final Throwable error) {
        final PostgresRemoteLogMetadataMetrics current = metrics;
        if (current != null && !(error instanceof RemoteResourceNotFoundException)) {
            current.recordError();
        }
    }

    private static HikariConfig poolConfig(final PostgresRemoteLogMetadataConfig rlmmConfig) {
        final HikariConfig config = new HikariConfig();
        config.setJdbcUrl(rlmmConfig.connectionString());
        config.setUsername(rlmmConfig.username());
        config.setPassword(rlmmConfig.password());
        config.setPoolName("pg-rlmm");
        config.setMaximumPoolSize(rlmmConfig.maxConnections());
        config.setAutoCommit(true);
        // Fail broker startup loudly when the metadata database is unreachable.
        config.setInitializationFailTimeout(rlmmConfig.connectionPoolTimeoutMs());
        config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
        config.setConnectionTimeout(rlmmConfig.connectionPoolTimeoutMs());
        config.setKeepaliveTime(TimeUnit.SECONDS.toMillis(30));
        config.addDataSourceProperty("connectTimeout", Integer.toString(timeoutSeconds(rlmmConfig.tcpConnectTimeoutMs())));
        config.addDataSourceProperty("socketTimeout", Integer.toString(timeoutSeconds(rlmmConfig.socketTimeoutMs())));
        config.addDataSourceProperty("loginTimeout", Integer.toString(timeoutSeconds(rlmmConfig.tcpConnectTimeoutMs())));
        config.addDataSourceProperty("tcpKeepAlive", "true");
        return config;
    }

    static int timeoutSeconds(final long timeoutMs) {
        return Math.toIntExact((timeoutMs - 1L) / 1_000L + 1L);
    }

    @FunctionalInterface
    private interface MutationTask {
        void run() throws RemoteStorageException;
    }
}
