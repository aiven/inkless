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

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;

import java.io.Closeable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

public class PostgresControlPlaneMetrics implements Closeable {
    final Time time;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(PostgresControlPlane.class);
    private final QueryMetrics findBatchesMetrics = new QueryMetrics("FindBatches");
    private final QueryMetrics getLogsMetrics = new QueryMetrics("GetLogs");
    private final QueryMetrics commitFileMetrics = new QueryMetrics("CommitFile");
    private final QueryMetrics commitFileMergeWorkItemMetrics = new QueryMetrics("CommitFileMergeWorkItem");
    private final QueryMetrics topicCreateMetrics = new QueryMetrics("TopicCreate");
    private final QueryMetrics initLogDisklessStartOffsetMetrics = new QueryMetrics("InitLogDisklessStartOffset");
    private final QueryMetrics topicDeleteMetrics = new QueryMetrics("TopicDelete");
    private final QueryMetrics fileDeleteMetrics = new QueryMetrics("FilesDelete");
    private final QueryMetrics listOffsetsMetrics = new QueryMetrics("ListOffsets");
    private final QueryMetrics deleteRecordsMetrics = new QueryMetrics("DeleteRecords");
    private final QueryMetrics enforceRetentionMetrics = new QueryMetrics("EnforceRetention");
    private final QueryMetrics getFilesToDeleteMetrics = new QueryMetrics("GetFilesToDelete");
    private final QueryMetrics getFileMergeWorkItemMetrics = new QueryMetrics("GetFileMergeWorkItem");
    private final QueryMetrics releaseFileMergeWorkItemMetrics = new QueryMetrics("ReleaseFileMergeWorkItem");
    private final QueryMetrics safeDeleteFileCheckMetrics = new QueryMetrics("SafeDeleteFileCheck");
    private final QueryMetrics getLogInfoMetrics = new QueryMetrics("GetLogInfo");
    private final QueryMetrics getDisklessLogMetrics = new QueryMetrics("GetDisklessLog");

    public PostgresControlPlaneMetrics(Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
    }

    public void onCommitFileMergeWorkItemCompleted(final Long duration) {
        commitFileMergeWorkItemMetrics.record(duration);
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

    public void onTopicCreateCompleted(Long duration) {
        topicCreateMetrics.record(duration);
    }

    public void onInitLogDisklessStartOffsetCompleted(Long duration) {
        initLogDisklessStartOffsetMetrics.record(duration);
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

    public void onGetFileMergeWorkItemCompleted(Long duration) {
        getFileMergeWorkItemMetrics.record(duration);
    }

    public void onReleaseFileMergeWorkItemCompleted(Long duration) {
        releaseFileMergeWorkItemMetrics.record(duration);
    }

    public void onSafeDeleteFileCheckCompleted(Long duration) {
        safeDeleteFileCheckMetrics.record(duration);
    }

    public void onGetLogInfoCompleted(Long duration) {
        getLogInfoMetrics.record(duration);
    }

    public void onGetDisklessLogCompleted(Long duration) {
        getDisklessLogMetrics.record(duration);
    }

    @Override
    public void close() {
        findBatchesMetrics.remove();
        getLogsMetrics.remove();
        commitFileMetrics.remove();
        commitFileMergeWorkItemMetrics.remove();
        topicCreateMetrics.remove();
        initLogDisklessStartOffsetMetrics.remove();
        topicDeleteMetrics.remove();
        fileDeleteMetrics.remove();
        listOffsetsMetrics.remove();
        deleteRecordsMetrics.remove();
        enforceRetentionMetrics.remove();
        getFilesToDeleteMetrics.remove();
        getFileMergeWorkItemMetrics.remove();
        releaseFileMergeWorkItemMetrics.remove();
        safeDeleteFileCheckMetrics.remove();
        getLogInfoMetrics.remove();
        getDisklessLogMetrics.remove();
    }

    private class QueryMetrics {
        private final String queryTimeMetricName;
        private final String queryRateMetricName;
        private final Histogram queryTimeHistogram;
        private final LongAdder queryRate = new LongAdder();

        private QueryMetrics(final String name) {
            this.queryTimeMetricName = name + "QueryTime";
            this.queryRateMetricName = name + "QueryRate";
            this.queryTimeHistogram = metricsGroup.newHistogram(queryTimeMetricName, true, Map.of());
            metricsGroup.newGauge(queryRateMetricName, queryRate::intValue);
        }

        private void record(final long duration) {
            queryTimeHistogram.update(duration);
            queryRate.increment();
        }

        private void remove() {
            metricsGroup.removeMetric(this.queryTimeMetricName);
            metricsGroup.removeMetric(this.queryRateMetricName);
        }
    }
}
