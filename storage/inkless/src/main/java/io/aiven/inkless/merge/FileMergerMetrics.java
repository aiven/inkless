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
package io.aiven.inkless.merge;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class FileMergerMetrics {
    private static final String GROUP = FileMerger.class.getSimpleName();

    static final String FILE_MERGE_TOTAL_TIME = "FileMergeTotalTime";
    private static final String FILE_MERGE_TOTAL_TIME_DOC = "Total time spent on a file merge operation in milliseconds";
    static final String FILE_UPLOAD_TIME = "FileUploadTime";
    private static final String FILE_UPLOAD_TIME_DOC = "Time spent uploading the merged file to object storage in milliseconds";
    static final String FILE_MERGE_RATE = "FileMergeRate";
    private static final String FILE_MERGE_RATE_DOC = "Total number of file merge operations started";
    static final String FILE_MERGE_FILES_RATE = "FileMergeFilesRate";
    private static final String FILE_MERGE_FILES_RATE_DOC = "Total number of files produced by merge operations";
    static final String FILE_MERGE_ERROR_RATE = "FileMergeErrorRate";
    private static final String FILE_MERGE_ERROR_RATE_DOC = "Total number of file merge errors";

    /**
     * This method returns a list of all the metric name templates for the FileMergerMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(FILE_MERGE_TOTAL_TIME, GROUP, FILE_MERGE_TOTAL_TIME_DOC),
            new MetricNameTemplate(FILE_UPLOAD_TIME, GROUP, FILE_UPLOAD_TIME_DOC),
            new MetricNameTemplate(FILE_MERGE_RATE, GROUP, FILE_MERGE_RATE_DOC),
            new MetricNameTemplate(FILE_MERGE_FILES_RATE, GROUP, FILE_MERGE_FILES_RATE_DOC),
            new MetricNameTemplate(FILE_MERGE_ERROR_RATE, GROUP, FILE_MERGE_ERROR_RATE_DOC)
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(FileMerger.class);
    private final Histogram fileMergeTotalTime;
    private final Histogram fileUploadTime;
    private final LongAdder fileMergeRate = new LongAdder();
    private final LongAdder fileMergeFiles = new LongAdder();
    private final LongAdder fileMergeErrorRate = new LongAdder();

    public FileMergerMetrics() {
        fileMergeTotalTime = metricsGroup.newHistogram(FILE_MERGE_TOTAL_TIME, true, Map.of());
        fileUploadTime = metricsGroup.newHistogram(FILE_UPLOAD_TIME, true, Map.of());
        metricsGroup.newGauge(FILE_MERGE_RATE, fileMergeRate::intValue);
        metricsGroup.newGauge(FILE_MERGE_FILES_RATE, fileMergeFiles::intValue);
        metricsGroup.newGauge(FILE_MERGE_ERROR_RATE, fileMergeErrorRate::intValue);
    }

    public void recordFileMergeStarted() {
        fileMergeRate.increment();
    }

    public void recordFileMergeError() {
        fileMergeErrorRate.increment();
    }

    public void recordFileUploadTime(final long timeMs) {
        fileUploadTime.update(timeMs);
    }

    public void recordFileMergeTotalTime(long duration) {
        fileMergeTotalTime.update(duration);
    }

    public void recordFileMergeCompleted(int size) {
        fileMergeFiles.add(size);
    }

    public void close() {
        metricsGroup.removeMetric(FILE_MERGE_TOTAL_TIME);
        metricsGroup.removeMetric(FILE_UPLOAD_TIME);
        metricsGroup.removeMetric(FILE_MERGE_RATE);
        metricsGroup.removeMetric(FILE_MERGE_FILES_RATE);
        metricsGroup.removeMetric(FILE_MERGE_ERROR_RATE);
    }
}
