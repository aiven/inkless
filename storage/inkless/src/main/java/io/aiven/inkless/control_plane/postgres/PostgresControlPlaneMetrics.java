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

    private static final String FIND_BATCHES_QUERY_TIME = "FindBatchesQueryTime";
    private static final String GET_LOGS_QUERY_TIME = "GetLogsQueryTime";
    private static final String COMMIT_FILE_QUERY_TIME = "CommitFileQueryTime";
    private static final String TOPIC_DELETE_QUERY_TIME = "TopicDeleteQueryTime";
    private static final String TOPIC_CREATE_QUERY_TIME = "TopicCreateQueryTime";
    private static final String FILES_DELETE_QUERY_TIME = "FilesDeleteQueryTime";

    public static final String FIND_BATCHES_QUERY_RATE = "FindBatchesQueryRate";
    public static final String GET_LOGS_QUERY_RATE = "GetLogsQueryRate";
    public static final String COMMIT_FILE_QUERY_RATE = "CommitFileQueryRate";
    public static final String TOPIC_CREATE_QUERY_RATE = "TopicCreateQueryRate";
    public static final String TOPIC_DELETE_QUERY_RATE = "TopicDeleteQueryRate";
    public static final String FILES_DELETE_QUERY_RATE = "FilesDeleteQueryRate";

    final Time time;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(PostgresControlPlane.class);
    private final Histogram findBatchesQueryTimeHistogram;
    private final Histogram getLogsQueryTimeHistogram;
    private final Histogram commitFileQueryTimeHistogram;
    private final Histogram topicDeleteQueryTimeHistogram;
    private final Histogram topicCreateQueryTimeHistogram;
    private final Histogram filesDeleteQueryTimeHistogram;

    private final LongAdder findBatchesQueryRate = new LongAdder();
    private final LongAdder getLogsQueryRate = new LongAdder();
    private final LongAdder commitFileQueryRate = new LongAdder();
    private final LongAdder topicCreateQueryRate = new LongAdder();
    private final LongAdder topicDeleteQueryRate = new LongAdder();
    private final LongAdder filesDeleteQueryRate = new LongAdder();

    public PostgresControlPlaneMetrics(Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");

        findBatchesQueryTimeHistogram = metricsGroup.newHistogram(FIND_BATCHES_QUERY_TIME, true, Map.of());
        getLogsQueryTimeHistogram = metricsGroup.newHistogram(GET_LOGS_QUERY_TIME, true, Map.of());
        commitFileQueryTimeHistogram = metricsGroup.newHistogram(COMMIT_FILE_QUERY_TIME, true, Map.of());
        topicDeleteQueryTimeHistogram = metricsGroup.newHistogram(TOPIC_DELETE_QUERY_TIME, true, Map.of());
        topicCreateQueryTimeHistogram = metricsGroup.newHistogram(TOPIC_CREATE_QUERY_TIME, true, Map.of());
        filesDeleteQueryTimeHistogram = metricsGroup.newHistogram(FILES_DELETE_QUERY_TIME, true, Map.of());

        metricsGroup.newGauge(FIND_BATCHES_QUERY_RATE, findBatchesQueryRate::intValue);
        metricsGroup.newGauge(GET_LOGS_QUERY_RATE, getLogsQueryRate::intValue);
        metricsGroup.newGauge(COMMIT_FILE_QUERY_RATE, commitFileQueryRate::intValue);
        metricsGroup.newGauge(TOPIC_CREATE_QUERY_RATE, topicCreateQueryRate::intValue);
        metricsGroup.newGauge(TOPIC_DELETE_QUERY_RATE, topicDeleteQueryRate::intValue);
        metricsGroup.newGauge(FILES_DELETE_QUERY_RATE, filesDeleteQueryRate::intValue);
    }

    public void onFindBatchesCompleted(Long duration) {
        findBatchesQueryTimeHistogram.update(duration);
        findBatchesQueryRate.increment();
    }

    public void onGetLogsCompleted(Long duration) {
        getLogsQueryTimeHistogram.update(duration);
        getLogsQueryRate.increment();
    }

    public void onCommitFileCompleted(Long duration) {
        commitFileQueryTimeHistogram.update(duration);
        commitFileQueryRate.increment();
    }

    public void onTopicDeleteCompleted(Long duration) {
        topicDeleteQueryTimeHistogram.update(duration);
        topicDeleteQueryRate.increment();
    }

    public void onTopicCreateCompleted(Long duration) {
        topicCreateQueryTimeHistogram.update(duration);
        topicCreateQueryRate.increment();
    }

    public void onFilesDeleteCompleted(Long duration) {
        filesDeleteQueryTimeHistogram.update(duration);
        filesDeleteQueryRate.increment();
    }

    @Override
    public void close() {
        metricsGroup.removeMetric(FIND_BATCHES_QUERY_RATE);
        metricsGroup.removeMetric(GET_LOGS_QUERY_RATE);
        metricsGroup.removeMetric(COMMIT_FILE_QUERY_RATE);
        metricsGroup.removeMetric(TOPIC_CREATE_QUERY_RATE);
        metricsGroup.removeMetric(TOPIC_DELETE_QUERY_RATE);
        metricsGroup.removeMetric(FIND_BATCHES_QUERY_TIME);
        metricsGroup.removeMetric(GET_LOGS_QUERY_TIME);
        metricsGroup.removeMetric(COMMIT_FILE_QUERY_TIME);
        metricsGroup.removeMetric(TOPIC_CREATE_QUERY_TIME);
        metricsGroup.removeMetric(TOPIC_DELETE_QUERY_TIME);
    }
}
