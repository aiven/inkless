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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.util.IsolationLevel;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
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
import io.aiven.inkless.control_plane.RepairDisklessLogRequest;
import io.aiven.inkless.control_plane.RepairDisklessLogResponse;

public class PostgresControlPlane extends AbstractControlPlane {
    private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PostgresControlPlane.class);

    private static final String POOL_NAME = "pg-control-plane";

    private final KafkaMetricsGroup metrics = new KafkaMetricsGroup(
        PostgresConnectionPoolMetrics.class.getPackageName(), PostgresConnectionPoolMetrics.class.getSimpleName());
    private final PostgresControlPlaneMetrics pgMetrics;

    private HikariDataSource jobsDataSource;
    private HikariDataSource readDataSource;
    private HikariDataSource writeDataSource;
    private PostgresControlPlaneConfig controlPlaneConfig;

    private DSLContext readJooqCtx;
    private DSLContext writeJooqCtx;
    private DSLContext jobsJooqCtx;

    public PostgresControlPlane(final Time time) {
        super(time);

        this.pgMetrics = new PostgresControlPlaneMetrics(time);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        // Checked structurally, against the raw map, before constructing anything: matching a
        // caught ConfigException's message risks classifying an unrelated error as "not
        // configured" whenever some other value happens to contain the text "connection.string".
        requireConnectionStringsConfigured(configs);

        controlPlaneConfig = new PostgresControlPlaneConfig(configs);
        LOGGER.info("Configuring PostgresControlPlane");

        controlPlaneConfig.initializeReadWriteConfigs();
        LOGGER.info("Initialized read/write configurations");

        Migrations.migrate(controlPlaneConfig);
        LOGGER.info("Database migrations completed");

        jobsDataSource = new HikariDataSource(dataSourceConfig(metrics, POOL_NAME, controlPlaneConfig));

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
            writeDataSource = new HikariDataSource(dataSourceConfig(metrics, POOL_NAME + "-write", controlPlaneConfig.writeConfig()));
            writeJooqCtx = DSL.using(writeDataSource, SQLDialect.POSTGRES);
        } else {
            LOGGER.info("No separate write configuration found, using jobs context for writes");
            writeJooqCtx = jobsJooqCtx;
        }
        if (controlPlaneConfig.readConfig() != null) {
            LOGGER.info("Using separate read configuration");
            readDataSource = new HikariDataSource(dataSourceConfig(metrics, POOL_NAME + "-read", controlPlaneConfig.readConfig()));
            readJooqCtx = DSL.using(readDataSource, SQLDialect.POSTGRES);
        } else {
            LOGGER.info("No separate write configuration found, using jobs context for reads");
            readJooqCtx = jobsJooqCtx;
        }
    }

    /**
     * Requires the default connection string, and the read or write connection string if either
     * override is present at all, to exist and be non-empty.
     *
     * <p>A dynamic reconfiguration may legitimately empty one of these three keys to take the
     * control plane out of service; nothing else in this map may be missing or malformed without
     * it being a genuine misconfiguration. Checking that structurally, against the raw keys and
     * values, keeps this from being confused with an unrelated {@link ConfigException}, such as a
     * bad numeric property whose value happens to contain the text "connection.string".
     */
    private static void requireConnectionStringsConfigured(final Map<String, ?> configs) {
        requireConnectionString(configs, "");
        if (hasAnyKeyWithPrefix(configs, PostgresControlPlaneConfig.READ_CONFIG_PREFIX)) {
            requireConnectionString(configs, PostgresControlPlaneConfig.READ_CONFIG_PREFIX);
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

    private static HikariConfig dataSourceConfig(final KafkaMetricsGroup metrics, final String name, final PostgresConnectionConfig connectionConfig) {
        final HikariConfig config = new HikariConfig();
        config.setPoolName(name);
        config.setJdbcUrl(connectionConfig.connectionString());
        config.setUsername(connectionConfig.username());
        config.setPassword(connectionConfig.password());
        config.setMetricsTrackerFactory((poolName, poolStats) -> new PostgresConnectionPoolMetrics(metrics, poolName, poolStats));
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
    public List<RepairDisklessLogResponse> repairDisklessLog(final List<RepairDisklessLogRequest> requests) {
        final RepairDisklessLogJob job = new RepairDisklessLogJob(time, jobsJooqCtx, requests, pgMetrics::onRepairDisklessLogCompleted);
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
        final FindBatchesJob job = new FindBatchesJob(
            time, readJooqCtx, // use specific read context when finding batches
            requests.toList(), fetchMaxBytes, maxBatchesPerPartition,
            pgMetrics::onFindBatchesCompleted);
        return job.call().iterator();
    }

    @Override
    protected Iterator<ListOffsetsResponse> listOffsetsForExistingPartitions(Stream<ListOffsetsRequest> requests) {
        final ListOffsetsJob job = new ListOffsetsJob(
            time, readJooqCtx,
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
            return new GetCrossTierLogStartJob(time, readJooqCtx, topicIdPartition, pgMetrics::onGetCrossTierLogStartCompleted).call();
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
            final GetProducerStateJob job = new GetProducerStateJob(time, readJooqCtx, requests, pgMetrics::onGetProducerStateCompleted);
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
    public List<PruneDisklessLogsResponse> pruneDisklessLogs(List<PruneDisklessLogsRequest> pruneDisklessLogsRequests) {
        try {
            final PruneDisklessLogsJob job = new PruneDisklessLogsJob(time, jobsJooqCtx, pruneDisklessLogsRequests, pgMetrics::onPruneDisklessLogsCompleted);
            return job.call();
        } catch (Exception e) {
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new ControlPlaneException("Failed to prune diskless logs", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        jobsDataSource.close();
        if (writeDataSource != null) {
            writeDataSource.close();
        }
        if (readDataSource != null) {
            readDataSource.close();
        }
        pgMetrics.close();
    }
}
