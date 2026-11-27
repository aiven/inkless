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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Time;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.IsolationLevel;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import io.aiven.inkless.common.ObjectFormat;
import io.aiven.inkless.control_plane.AbstractControlPlane;
import io.aiven.inkless.control_plane.AdvanceCrossTierLogStartOffsetRequest;
import io.aiven.inkless.control_plane.AdvanceCrossTierLogStartOffsetResponse;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CommitBatchResponse;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.ControlPlaneNotConfiguredException;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.DeleteFilesRequest;
import io.aiven.inkless.control_plane.DeleteRecordsRequest;
import io.aiven.inkless.control_plane.DeleteRecordsResponse;
import io.aiven.inkless.control_plane.EnforceRetentionRequest;
import io.aiven.inkless.control_plane.EnforceRetentionResponse;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.control_plane.GetLogInfoRequest;
import io.aiven.inkless.control_plane.GetLogInfoResponse;
import io.aiven.inkless.control_plane.GetProducerStateRequest;
import io.aiven.inkless.control_plane.GetProducerStateResponse;
import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.control_plane.InitDisklessLogResponse;
import io.aiven.inkless.control_plane.ListOffsetsRequest;
import io.aiven.inkless.control_plane.ListOffsetsResponse;
import io.aiven.inkless.control_plane.PruneDisklessLogsRequest;
import io.aiven.inkless.control_plane.PruneDisklessLogsResponse;
import io.aiven.inkless.control_plane.PurgeDeletedLogsResponse;

public class PostgresControlPlane extends AbstractControlPlane {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PostgresControlPlane.class);

    private static final String POOL_NAME = "pg-control-plane";

    private final PostgresControlPlaneMetrics pgMetrics = PostgresControlPlaneMetrics.instance();

    private HikariDataSource jobsDataSource;
    private HikariDataSource readDataSource;
    private HikariDataSource readPrimaryDataSource;
    private HikariDataSource writeDataSource;
    private PostgresControlPlaneConfig controlPlaneConfig;
    private volatile boolean closed;

    private DSLContext readJooqCtx;
    private DSLContext primaryReadJooqCtx;
    private DSLContext writeJooqCtx;
    private DSLContext jobsJooqCtx;
    private volatile boolean standbyProtectionEnabled;
    // Fail-fast concurrency limiter for primary `findBatches` calls (confirmation and fallback) so
    // bursts don't saturate `readPrimaryDataSource` and block `listOffsets`, `getCrossTierLogStart`,
    // and `getProducerState`.
    private Semaphore primaryReadLimiter;
    private StandbyCircuitBreaker standbyCircuitBreaker;

    public PostgresControlPlane(final Time time) {
        super(time);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // Checked structurally, against the raw map, before constructing anything: matching a
        // caught ConfigException's message risks classifying an unrelated error as
        // {@code not configured} whenever some other value happens to contain the text
        // {@code connection.string}.
        requireConnectionStringsConfigured(configs);

        controlPlaneConfig = new PostgresControlPlaneConfig(configs);
        LOGGER.info("Configuring PostgresControlPlane");

        controlPlaneConfig.initializeReadWriteConfigs();
        LOGGER.info("Initialized read/write configurations");

        Migrations.migrate(controlPlaneConfig);
        LOGGER.info("Database migrations completed");

        jobsDataSource = new HikariDataSource(dataSourceConfig(POOL_NAME, controlPlaneConfig));

        // Avoid merger/cleaner waiting on class loading deadlocks between threads
        try {
            Class.forName("org.jooq.generated.DefaultSchema");
        } catch (final ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        jobsJooqCtx = DSL.using(jobsDataSource, SQLDialect.POSTGRES);

        // Set up read and write contexts if configured
        if (controlPlaneConfig.writeConfig() != null) {
            LOGGER.info("Using separate write configuration");
            writeDataSource = new HikariDataSource(dataSourceConfig(POOL_NAME + "-write", controlPlaneConfig.writeConfig()));
            writeJooqCtx = DSL.using(writeDataSource, SQLDialect.POSTGRES);
        } else {
            LOGGER.info("No separate write configuration found, using jobs context for writes");
            writeJooqCtx = jobsJooqCtx;
        }
        if (controlPlaneConfig.readConfig() != null) {
            LOGGER.info("Using separate read configuration");
            readDataSource = new HikariDataSource(dataSourceConfig(POOL_NAME + "-read", controlPlaneConfig.readConfig()));
        }
        if (controlPlaneConfig.readPrimaryConfig() != null) {
            LOGGER.info("Using separate read primary configuration");
            readPrimaryDataSource = new HikariDataSource(
                dataSourceConfig(POOL_NAME + "-read-primary", controlPlaneConfig.readPrimaryConfig()));
        }

        if (readDataSource != null && readPrimaryDataSource != null) {
            readJooqCtx = DSL.using(readDataSource, SQLDialect.POSTGRES);
            primaryReadJooqCtx = DSL.using(readPrimaryDataSource, SQLDialect.POSTGRES);
            standbyProtectionEnabled = true;
            final int configuredMaxConcurrent = controlPlaneConfig.standbyFallbackMaxConcurrent();
            final int maxConcurrentPermits = configuredMaxConcurrent > 0
                ? configuredMaxConcurrent
                : PostgresControlPlaneConfig.defaultStandbyFallbackMaxConcurrent(
                    controlPlaneConfig.readPrimaryConfig().maxConnections());
            primaryReadLimiter = new Semaphore(maxConcurrentPermits);
            standbyCircuitBreaker = new StandbyCircuitBreaker(
                controlPlaneConfig.standbyCircuitBreakerFailureThreshold(),
                controlPlaneConfig.standbyCircuitBreakerProbeIntervalMs(),
                readDataSource,
                pgMetrics
            );
        } else if (readDataSource != null) {
            // Keep `jobsDataSource` isolated for background and controller jobs instead of borrowing
            // it for read fallback when `read.primary.*` is absent.
            readJooqCtx = DSL.using(readDataSource, SQLDialect.POSTGRES);
            primaryReadJooqCtx = readJooqCtx;
            standbyProtectionEnabled = false;
        } else if (readPrimaryDataSource != null) {
            LOGGER.warn("Configured read.primary.* without read.*; routing all reads to read.primary and disabling standby protection");
            readJooqCtx = DSL.using(readPrimaryDataSource, SQLDialect.POSTGRES);
            primaryReadJooqCtx = readJooqCtx;
            standbyProtectionEnabled = false;
        } else {
            LOGGER.info("No separate read configuration found, using jobs context for reads");
            readJooqCtx = jobsJooqCtx;
            primaryReadJooqCtx = jobsJooqCtx;
            standbyProtectionEnabled = false;
        }
    }

    /**
     * Requires the default connection string, and each override connection string whose prefix is
     * present, to exist and be non-empty.
     *
     * <p>A runtime reconfiguration may legitimately empty one of these four keys to take the
     * control plane out of service; nothing else in this map may be missing or malformed without
     * it being a misconfiguration. Checking that structurally, against the raw keys and
     * values, keeps this from being confused with an unrelated {@link ConfigException}, such as a
     * bad numeric property whose value happens to contain the text {@code connection.string}.
     */
    private static void requireConnectionStringsConfigured(final Map<String, ?> configs) {
        requireConnectionString(configs, "");
        final boolean hasReadKey = configs.keySet().stream()
            .anyMatch(key -> key.startsWith(PostgresControlPlaneConfig.READ_CONFIG_PREFIX)
                && !key.startsWith(PostgresControlPlaneConfig.READ_PRIMARY_CONFIG_PREFIX));
        if (hasReadKey) {
            requireConnectionString(configs, PostgresControlPlaneConfig.READ_CONFIG_PREFIX);
        }
        if (hasAnyKeyWithPrefix(configs, PostgresControlPlaneConfig.READ_PRIMARY_CONFIG_PREFIX)) {
            requireConnectionString(configs, PostgresControlPlaneConfig.READ_PRIMARY_CONFIG_PREFIX);
        }
        if (hasAnyKeyWithPrefix(configs, PostgresControlPlaneConfig.WRITE_CONFIG_PREFIX)) {
            requireConnectionString(configs, PostgresControlPlaneConfig.WRITE_CONFIG_PREFIX);
        }
    }

    private static boolean hasAnyKeyWithPrefix(final Map<String, ?> configs, final String prefix) {
        return configs.keySet().stream().anyMatch(key -> key.startsWith(prefix));
    }

    private static void requireConnectionString(final Map<String, ?> configs, final String prefix) {
        final String key = prefix + PostgresConnectionConfig.CONNECTION_STRING_CONFIG;
        final Object value = configs.get(key);
        if (value == null || value.toString().isEmpty()) {
            throw new ControlPlaneNotConfiguredException(
                "No diskless control plane is configured: " + key + " is empty or missing");
        }
    }

    private static HikariConfig dataSourceConfig(final String name, final PostgresConnectionConfig connectionConfig) {
        final HikariConfig config = new HikariConfig();
        config.setPoolName(name);
        config.setJdbcUrl(connectionConfig.connectionString());
        config.setUsername(connectionConfig.username());
        config.setPassword(connectionConfig.password());
        config.setMetricsTrackerFactory(PostgresConnectionPoolMetrics.instance()::tracker);
        config.setTransactionIsolation(IsolationLevel.TRANSACTION_READ_COMMITTED.name());

        config.setMaximumPoolSize(connectionConfig.maxConnections());
        config.setConnectionTimeout(connectionConfig.connectionPoolTimeoutMs());
        config.addDataSourceProperty("connectTimeout", Long.toString(connectionConfig.tcpConnectTimeoutSeconds()));
        config.addDataSourceProperty("socketTimeout", Long.toString(connectionConfig.socketTimeoutSeconds()));
        config.addDataSourceProperty("loginTimeout", Long.toString(connectionConfig.tcpConnectTimeoutSeconds()));
        config.addDataSourceProperty("tcpKeepAlive", "true");

        // We're doing interactive transactions.
        config.setAutoCommit(false);
        return config;
    }

    @Override
    public void createTopicAndPartitions(final Set<CreateTopicAndPartitionsRequest> requests) {
        // Expected to be performed synchronously
        new TopicsAndPartitionsCreateJob(time, jobsJooqCtx, requests, pgMetrics::onTopicCreateCompleted).run();
    }

    @Override
    public List<InitDisklessLogResponse> initDisklessLog(final List<InitDisklessLogRequest> requests) {
        final InitDisklessLogJob job = new InitDisklessLogJob(time, jobsJooqCtx, requests, pgMetrics::onInitDisklessLogCompleted);
        return job.call();
    }

    @Override
    protected Iterator<CommitBatchResponse> commitFileForValidRequests(
        final String objectKey,
        final ObjectFormat format,
        final int uploaderBrokerId,
        final long fileSize,
        final Stream<CommitBatchRequest> requests) {
        final CommitFileJob job = new CommitFileJob(
            time, writeJooqCtx, // use specific write context when committing files
            objectKey, format, uploaderBrokerId, fileSize, requests.toList(),
            controlPlaneConfig.batchCoalescingEnabled(),
            pgMetrics::onCommitFileCompleted);
        return job.call().iterator();
    }

    @Override
    protected Iterator<FindBatchResponse> findBatchesForExistingPartitions(
        final Stream<FindBatchRequest> requests,
        final int fetchMaxBytes,
        final int maxBatchesPerPartition
    ) {
        final List<FindBatchRequest> requestList = requests.toList();
        if (!standbyProtectionEnabled) {
            final List<FindBatchResponse> responses = new FindBatchesJob(
                time, readJooqCtx, // use specific read context when finding batches
                requestList, fetchMaxBytes, maxBatchesPerPartition,
                pgMetrics::onFindBatchesCompleted).call();
            return responses.iterator();
        }
        // Short-circuit empty fetches before checking the breaker so they don't take a limiter
        // permit or increment `StandbyFallbackRate`.
        if (requestList.isEmpty()) {
            return Collections.emptyIterator();
        }

        if (standbyCircuitBreaker.isOpen()) {
            return fallbackFindBatchesOnPrimary(requestList, fetchMaxBytes, maxBatchesPerPartition).iterator();
        }

        final long generation = standbyCircuitBreaker.generation();
        final List<FindBatchResponse> responses;
        try {
            responses = new FindBatchesJob(
                time, readJooqCtx, // use specific read context when finding batches
                requestList, fetchMaxBytes, maxBatchesPerPartition,
                pgMetrics::onFindBatchesCompleted).call();
            standbyCircuitBreaker.recordSuccess(generation);
        } catch (final RuntimeException e) {
            if (!StandbyCircuitBreaker.isConnectivityOrTimeoutFailure(e)) {
                throw e;
            }
            standbyCircuitBreaker.recordFailure(generation, e);
            // Fall back immediately even before reaching the trip threshold so a brief failure on
            // the standby doesn't surface `UNKNOWN_SERVER_ERROR` to callers.
            return fallbackFindBatchesOnPrimary(requestList, fetchMaxBytes, maxBatchesPerPartition).iterator();
        }
        return maybeRetryOutOfRangeOnPrimary(requestList, responses, fetchMaxBytes, maxBatchesPerPartition).iterator();
    }

    /**
     * Retries the fetch against the primary read pool if any standby response reports
     * {@code OFFSET_OUT_OF_RANGE} past the high watermark, so a lagging standby doesn't trigger
     * consumer offset resets or consolidation log truncation.
     */
    private List<FindBatchResponse> maybeRetryOutOfRangeOnPrimary(
        final List<FindBatchRequest> requestList,
        final List<FindBatchResponse> standbyResponses,
        final int fetchMaxBytes,
        final int maxBatchesPerPartition
    ) {
        final List<Integer> positionsToConfirm = new ArrayList<>();
        for (int i = 0; i < standbyResponses.size(); i++) {
            final FindBatchResponse response = standbyResponses.get(i);
            // Confirm `OFFSET_OUT_OF_RANGE` only when the requested offset exceeds the high watermark.
            // Because `log_start_offset` moves forward monotonically, a lagging standby already answers
            // below-LSO requests authoritatively.
            if (response.errors() == Errors.OFFSET_OUT_OF_RANGE
                && requestList.get(i).offset() > response.highWatermark()) {
                positionsToConfirm.add(i);
            }
        }
        if (positionsToConfirm.isEmpty()) {
            return standbyResponses;
        }

        pgMetrics.onFindBatchesPrimaryConfirm(positionsToConfirm.size());
        // Fail fast instead of blocking fetch threads behind `readPrimaryDataSource`: return
        // retriable `KAFKA_STORAGE_ERROR` for only the unconfirmed partitions so callers retry
        // without resetting offsets while healthy standby partitions in this fetch still succeed.
        if (!primaryReadLimiter.tryAcquire()) {
            pgMetrics.onPrimaryReadLimitRejected();
            pgMetrics.onFindBatchesPrimaryConfirmFailed(positionsToConfirm.size());
            return replaceWithKafkaStorageError(standbyResponses, positionsToConfirm);
        }

        // Re-run the full fetch on primary so `find_batches_v2` enforces `fetchMaxBytes`,
        // per-partition limits, and the minimum-batch allowance in one pass with fresh watermarks.
        // If primary fails, downgrade only the unconfirmed partitions to retriable
        // `KAFKA_STORAGE_ERROR` so valid standby answers for the other partitions still succeed.
        final List<FindBatchResponse> primaryResponses;
        try {
            primaryResponses = new FindBatchesJob(
                time, primaryReadJooqCtx,
                requestList, fetchMaxBytes, maxBatchesPerPartition,
                pgMetrics::onFindBatchesPrimaryCompleted).call();
        } catch (final RuntimeException e) {
            LOGGER.warn("Failed to confirm {} above-HWM OFFSET_OUT_OF_RANGE partition(s) on primary; returning KAFKA_STORAGE_ERROR",
                positionsToConfirm.size(), e);
            pgMetrics.onFindBatchesPrimaryConfirmFailed(positionsToConfirm.size());
            return replaceWithKafkaStorageError(standbyResponses, positionsToConfirm);
        } finally {
            primaryReadLimiter.release();
        }

        int overrideCount = 0;
        for (final int position : positionsToConfirm) {
            if (primaryResponses.get(position).errors() == Errors.NONE) {
                overrideCount++;
            }
        }
        if (overrideCount > 0) {
            pgMetrics.onFindBatchesPrimaryOverride(overrideCount);
        }
        return primaryResponses;
    }

    private List<FindBatchResponse> fallbackFindBatchesOnPrimary(
        final List<FindBatchRequest> requestList,
        final int fetchMaxBytes,
        final int maxBatchesPerPartition
    ) {
        pgMetrics.onStandbyFallback();
        // Fail fast instead of queuing fetch threads behind `readPrimaryDataSource`: return
        // retriable `KAFKA_STORAGE_ERROR` so callers back off without resetting offsets.
        if (!primaryReadLimiter.tryAcquire()) {
            pgMetrics.onPrimaryReadLimitRejected();
            pgMetrics.onStandbyFallbackFailed();
            return Collections.nCopies(requestList.size(), FindBatchResponse.kafkaStorageError());
        }
        try {
            return new FindBatchesJob(
                time, primaryReadJooqCtx,
                requestList, fetchMaxBytes, maxBatchesPerPartition,
                pgMetrics::onFindBatchesPrimaryCompleted).call();
        } catch (final RuntimeException e) {
            LOGGER.warn(
                "Standby fallback findBatches failed on primary for {} partition(s); returning KAFKA_STORAGE_ERROR",
                requestList.size(), e);
            pgMetrics.onStandbyFallbackFailed();
            // Return per-partition retriable `KAFKA_STORAGE_ERROR` instead of letting
            // `ControlPlaneException` escape to `FetchHandler`, which maps it to
            // non-retriable `UNKNOWN_SERVER_ERROR`.
            return Collections.nCopies(requestList.size(), FindBatchResponse.kafkaStorageError());
        } finally {
            primaryReadLimiter.release();
        }
    }

    private static List<FindBatchResponse> replaceWithKafkaStorageError(
        final List<FindBatchResponse> responses,
        final List<Integer> positions
    ) {
        final List<FindBatchResponse> merged = new ArrayList<>(responses);
        for (final int position : positions) {
            merged.set(position, FindBatchResponse.kafkaStorageError());
        }
        return merged;
    }

    @Override
    protected Iterator<ListOffsetsResponse> listOffsetsForExistingPartitions(Stream<ListOffsetsRequest> requests) {
        // `list_offsets_v1` never returns `OFFSET_OUT_OF_RANGE`, so callers can't detect standby
        // staleness from its response. Route directly to `primaryReadJooqCtx`.
        final ListOffsetsJob job = new ListOffsetsJob(
            time, primaryReadJooqCtx,
            requests.toList(),
            pgMetrics::onListOffsetsCompleted);
        return job.call().iterator();
    }

    @Override
    public void deleteTopics(final Set<Uuid> topicIds) {
        final DeleteTopicJob job = new DeleteTopicJob(time, jobsJooqCtx, topicIds, pgMetrics::onTopicDeleteCompleted);
        job.run();
    }

    @Override
    public PurgeDeletedLogsResponse purgeDeletedLogs(final int maxBatches) {
        try {
            final PurgeDeletedLogsJob job = new PurgeDeletedLogsJob(
                time, jobsJooqCtx, maxBatches, pgMetrics::onPurgeDeletedLogsCompleted);
            return job.call();
        } catch (final Exception e) {
            throw new ControlPlaneException("Failed to purge deleted logs", e);
        }
    }

    @Override
    public List<DeleteRecordsResponse> deleteRecords(final List<DeleteRecordsRequest> requests) {
        final DeleteRecordsJob job = new DeleteRecordsJob(time, jobsJooqCtx, requests, pgMetrics::onDeleteRecordsCompleted);
        return job.call();
    }

    @Override
    public List<EnforceRetentionResponse> enforceRetention(final List<EnforceRetentionRequest> requests, final int maxBatchesPerRequest) {
        try {
            final EnforceRetentionJob job = new EnforceRetentionJob(time, jobsJooqCtx, requests, maxBatchesPerRequest, pgMetrics::onEnforceRetentionCompleted);
            return job.call();
        } catch (final Exception e) {
            throw new ControlPlaneException("Failed to enforce retention", e);
        }
    }

    @Override
    public List<AdvanceCrossTierLogStartOffsetResponse> advanceCrossTierLogStartOffset(final List<AdvanceCrossTierLogStartOffsetRequest> requests) {
        try {
            final AdvanceCrossTierLogStartOffsetJob job = new AdvanceCrossTierLogStartOffsetJob(
                time, writeJooqCtx, requests, pgMetrics::onAdvanceCrossTierLogStartCompleted);
            return job.call();
        } catch (final Exception e) {
            throw new ControlPlaneException("Failed to advance cross-tier log start offset", e);
        }
    }

    @Override
    public OptionalLong getCrossTierLogStart(final TopicIdPartition topicIdPartition) {
        try {
            return new GetCrossTierLogStartJob(time, primaryReadJooqCtx, topicIdPartition, pgMetrics::onGetCrossTierLogStartCompleted).call();
        } catch (final Exception e) {
            throw new ControlPlaneException("Failed to get cross-tier log start offset", e);
        }
    }

    @Override
    public List<FileToDelete> getFilesToDelete(final Instant markedBefore, final int limit) {
        try {
            final FindFilesToDeleteJob job = new FindFilesToDeleteJob(
                time, jobsJooqCtx, markedBefore, limit, pgMetrics::onGetFilesToDeleteCompleted);
            return job.call();
        } catch (final Exception e) {
            throw new ControlPlaneException("Failed to get files to delete", e);
        }
    }

    @Override
    public void deleteFiles(DeleteFilesRequest request) {
        try {
            final DeleteFilesJob job = new DeleteFilesJob(time, jobsJooqCtx, request, pgMetrics::onFilesDeleteCompleted);
            job.run();
        } catch (final Exception e) {
            throw new ControlPlaneException("Failed to delete files", e);
        }
    }

    @Override
    public boolean isSafeToDeleteFile(String objectKeyPath) {
        try {
            final SafeDeleteFileCheckJob job =
                new SafeDeleteFileCheckJob(time, jobsJooqCtx, objectKeyPath, pgMetrics::onSafeDeleteFileCheckCompleted);
            return job.call();
        } catch (Exception e) {
            throw new ControlPlaneException("Error when checking if safe to delete file " + objectKeyPath, e);
        }
    }

    @Override
    public List<GetLogInfoResponse> getLogInfo(final List<GetLogInfoRequest> requests) {
        try {
            // used for testing purposes only, so using jobs connection pool is fine
            final GetLogInfoJob job = new GetLogInfoJob(time, jobsJooqCtx, requests, pgMetrics::onGetLogInfoCompleted);
            return job.call();
        } catch (final Exception e) {
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new ControlPlaneException("Failed to get log info", e);
            }
        }
    }

    @Override
    public List<GetProducerStateResponse> getProducerState(final List<GetProducerStateRequest> requests) {
        try {
            final GetProducerStateJob job = new GetProducerStateJob(time, primaryReadJooqCtx, requests, pgMetrics::onGetProducerStateCompleted);
            return job.call();
        } catch (final Exception e) {
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new ControlPlaneException("Failed to get producer state", e);
            }
        }
    }

    @Override
    public List<PruneDisklessLogsResponse> pruneDisklessLogs(
            final List<PruneDisklessLogsRequest> pruneDisklessLogsRequests,
            final int maxBatchesPerRequest) {
        try {
            final PruneDisklessLogsJob job = new PruneDisklessLogsJob(
                time, jobsJooqCtx, pruneDisklessLogsRequests, maxBatchesPerRequest, pgMetrics::onPruneDisklessLogsCompleted);
            return job.call();
        } catch (Exception e) {
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new ControlPlaneException("Failed to prune diskless logs", e);
            }
        }
    }

    // Visible for testing.
    boolean standbyProtectionEnabled() {
        return standbyProtectionEnabled;
    }

    // Visible for testing.
    Semaphore primaryReadLimiter() {
        return primaryReadLimiter;
    }

    // Visible for testing.
    StandbyCircuitBreaker standbyCircuitBreaker() {
        return standbyCircuitBreaker;
    }

    /**
     * Closes whatever {@link #configure} managed to open before this is discarded, whether that is
     * everything, nothing, because it failed before opening any pool, or something in between,
     * because it failed partway through. Idempotent, so {@link ControlPlane#create} closing this on
     * a {@code configure()} failure and the reconciler closing it again later, if it ever got that
     * far, don't double-close the same pool.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // Stop the standby probe thread before closing `readDataSource` so it doesn't probe a
        // closing pool.
        if (standbyCircuitBreaker != null) {
            standbyCircuitBreaker.close();
        }
        if (jobsDataSource != null) {
            jobsDataSource.close();
        }
        if (writeDataSource != null) {
            writeDataSource.close();
        }
        if (readDataSource != null) {
            readDataSource.close();
        }
        if (readPrimaryDataSource != null) {
            readPrimaryDataSource.close();
        }
    }
}
