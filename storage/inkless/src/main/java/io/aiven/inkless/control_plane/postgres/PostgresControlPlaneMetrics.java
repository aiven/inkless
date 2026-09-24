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

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Query metrics shared by every {@code PostgresControlPlane} in the process.
 *
 * <p>A rebuilt control plane runs the same queries under the same metric names. Yammer's registry
 * keeps whichever metric registered a name first, so the metrics are registered once, by the single
 * instance, and are never removed.
 */
public final class PostgresControlPlaneMetrics {
    private static final String GROUP = PostgresControlPlane.class.getSimpleName();

    private static final List<String> QUERY_NAMES = List.of(
        "FindBatches", "GetLogs", "CommitFile",
        "TopicCreate", "TopicDelete", "PurgeDeletedLogs", "FilesDelete", "ListOffsets",
        "DeleteRecords", "EnforceRetention", "GetFilesToDelete",
        "SafeDeleteFileCheck",
        "GetLogInfo", "InitDisklessLog", "GetProducerState",
        "AdvanceCrossTierLogStart", "RepairDisklessLog", "GetCrossTierLogStart"
    );

    /**
     * This method returns a list of all the metric name templates for the PostgresControlPlaneMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        final List<MetricNameTemplate> templates = new ArrayList<>();
        for (final String name : QUERY_NAMES) {
            templates.add(new MetricNameTemplate(name + "QueryTime", GROUP,
                "Time spent executing the " + name + " query in milliseconds"));
            templates.add(new MetricNameTemplate(name + "QueryRate", GROUP,
                "Total number of " + name + " queries executed"));
            templates.add(new MetricNameTemplate(name + "LastSuccessfulQueryAgeMs", GROUP,
                "Milliseconds since the last successful " + name + " query completed; -1 if no query has succeeded since startup"));
        }
        return templates;
    }

    // Registers the metrics on first use, not when MetricsDocs loads this class to call all().
    private static final class Holder {
        private static final PostgresControlPlaneMetrics INSTANCE = new PostgresControlPlaneMetrics();
    }

    private final Time time = Time.SYSTEM;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        PostgresControlPlane.class.getPackageName(), PostgresControlPlane.class.getSimpleName());
    // Visible for testing.
    final QueryMetrics findBatchesMetrics = new QueryMetrics("FindBatches");
    private final QueryMetrics getLogsMetrics = new QueryMetrics("GetLogs");
    // Visible for testing.
    final QueryMetrics commitFileMetrics = new QueryMetrics("CommitFile");
    private final QueryMetrics topicCreateMetrics = new QueryMetrics("TopicCreate");
    private final QueryMetrics topicDeleteMetrics = new QueryMetrics("TopicDelete");
    private final QueryMetrics purgeDeletedLogsMetrics = new QueryMetrics("PurgeDeletedLogs");
    private final QueryMetrics fileDeleteMetrics = new QueryMetrics("FilesDelete");
    private final QueryMetrics listOffsetsMetrics = new QueryMetrics("ListOffsets");
    private final QueryMetrics deleteRecordsMetrics = new QueryMetrics("DeleteRecords");
    private final QueryMetrics enforceRetentionMetrics = new QueryMetrics("EnforceRetention");
    private final QueryMetrics getFilesToDeleteMetrics = new QueryMetrics("GetFilesToDelete");
    private final QueryMetrics safeDeleteFileCheckMetrics = new QueryMetrics("SafeDeleteFileCheck");
    private final QueryMetrics getLogInfoMetrics = new QueryMetrics("GetLogInfo");
    private final QueryMetrics initDisklessLogMetrics = new QueryMetrics("InitDisklessLog");
    private final QueryMetrics repairDisklessLogMetrics = new QueryMetrics("RepairDisklessLog");
    private final QueryMetrics getProducerStateMetrics = new QueryMetrics("GetProducerState");
    private final QueryMetrics pruneDisklessLogsMetrics = new QueryMetrics("PruneDisklessLogs");
    private final QueryMetrics advanceCrossTierLogStartMetrics = new QueryMetrics("AdvanceCrossTierLogStart");
    private final QueryMetrics getCrossTierLogStartMetrics = new QueryMetrics("GetCrossTierLogStart");

    private PostgresControlPlaneMetrics() {
    }

    public static PostgresControlPlaneMetrics instance() {
        return Holder.INSTANCE;
    }

    public void onFindBatchesCompleted(Long duration) {
        findBatchesMetrics.record(duration);
    }

    public void onGetLogsCompleted(Long duration) {
        getLogsMetrics.record(duration);
    }

    public void onCommitFileCompleted(Long duration) {
        commitFileMetrics.record(duration);
    }

    public void onTopicDeleteCompleted(Long duration) {
        topicDeleteMetrics.record(duration);
    }

    public void onPurgeDeletedLogsCompleted(Long duration) {
        purgeDeletedLogsMetrics.record(duration);
    }

    public void onTopicCreateCompleted(Long duration) {
        topicCreateMetrics.record(duration);
    }

    public void onFilesDeleteCompleted(Long duration) {
        fileDeleteMetrics.record(duration);
    }

    public void onListOffsetsCompleted(Long duration) {
        listOffsetsMetrics.record(duration);
    }

    public void onDeleteRecordsCompleted(Long duration) {
        deleteRecordsMetrics.record(duration);
    }

    public void onEnforceRetentionCompleted(Long duration) {
        enforceRetentionMetrics.record(duration);
    }

    public void onGetFilesToDeleteCompleted(Long duration) {
        getFilesToDeleteMetrics.record(duration);
    }

    public void onSafeDeleteFileCheckCompleted(Long duration) {
        safeDeleteFileCheckMetrics.record(duration);
    }

    public void onGetLogInfoCompleted(Long duration) {
        getLogInfoMetrics.record(duration);
    }

    public void onInitDisklessLogCompleted(Long duration) {
        initDisklessLogMetrics.record(duration);
    }

    public void onRepairDisklessLogCompleted(Long duration) {
        repairDisklessLogMetrics.record(duration);
    }

    public void onGetProducerStateCompleted(Long duration) {
        getProducerStateMetrics.record(duration);
    }

    public void onPruneDisklessLogsCompleted(Long duration) {
        pruneDisklessLogsMetrics.record(duration);
    }

    public void onAdvanceCrossTierLogStartCompleted(Long duration) {
        advanceCrossTierLogStartMetrics.record(duration);
    }

    public void onGetCrossTierLogStartCompleted(Long duration) {
        getCrossTierLogStartMetrics.record(duration);
    }

    // Visible for testing.
    static long ageMs(final long lastSuccessfulQueryTimeMs, final long nowMs) {
        return lastSuccessfulQueryTimeMs == -1 ? -1L : nowMs - lastSuccessfulQueryTimeMs;
    }

    // Visible for testing.
    class QueryMetrics {
        private final Histogram queryTimeHistogram;
        private final LongAdder queryRate = new LongAdder();
        // -1 means no successful query has occurred since startup.
        // Visible for testing.
        final AtomicLong lastSuccessfulQueryTimeMs = new AtomicLong(-1);

        private QueryMetrics(final String name) {
            this.queryTimeHistogram = metricsGroup.newHistogram(name + "QueryTime", true, Map.of());
            metricsGroup.newGauge(name + "QueryRate", queryRate::intValue);
            metricsGroup.newGauge(name + "LastSuccessfulQueryAgeMs",
                () -> ageMs(lastSuccessfulQueryTimeMs.get(), time.milliseconds()));
        }

        private void record(final long duration) {
            queryTimeHistogram.update(duration);
            queryRate.increment();
            lastSuccessfulQueryTimeMs.set(time.milliseconds());
        }
    }
}
