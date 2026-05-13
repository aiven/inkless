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
package io.aiven.inkless.delete;

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class FileCleanerMetrics {
    private static final String GROUP = FileCleaner.class.getSimpleName();

    static final String FILE_CLEANER_TOTAL_TIME = "FileCleanerTotalTime";
    private static final String FILE_CLEANER_TOTAL_TIME_DOC = "Total time spent on a file cleaning cycle in milliseconds";
    static final String FILE_CLEANER_RATE = "FileCleanerRate";
    private static final String FILE_CLEANER_RATE_DOC = "Total number of file cleaning cycles started";
    static final String FILE_CLEANER_FILES_RATE = "FileCleanerFilesRate";
    private static final String FILE_CLEANER_FILES_RATE_DOC = "Total number of files cleaned";
    static final String FILE_CLEANER_ERROR_RATE = "FileCleanerErrorRate";
    private static final String FILE_CLEANER_ERROR_RATE_DOC = "Total number of file cleaning errors";

    /**
     * This method returns a list of all the metric name templates for the FileCleanerMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(FILE_CLEANER_TOTAL_TIME, GROUP, FILE_CLEANER_TOTAL_TIME_DOC),
            new MetricNameTemplate(FILE_CLEANER_RATE, GROUP, FILE_CLEANER_RATE_DOC),
            new MetricNameTemplate(FILE_CLEANER_FILES_RATE, GROUP, FILE_CLEANER_FILES_RATE_DOC),
            new MetricNameTemplate(FILE_CLEANER_ERROR_RATE, GROUP, FILE_CLEANER_ERROR_RATE_DOC)
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(FileCleaner.class);
    private final Histogram fileCleanerTotalTime;
    private final LongAdder fileCleanerRate = new LongAdder();
    private final LongAdder fileCleanerFiles = new LongAdder();
    private final LongAdder fileCleanerErrorRate = new LongAdder();

    public FileCleanerMetrics() {
        fileCleanerTotalTime = metricsGroup.newHistogram(FILE_CLEANER_TOTAL_TIME, true, Map.of());
        metricsGroup.newGauge(FILE_CLEANER_RATE, fileCleanerRate::intValue);
        metricsGroup.newGauge(FILE_CLEANER_FILES_RATE, fileCleanerFiles::intValue);
        metricsGroup.newGauge(FILE_CLEANER_ERROR_RATE, fileCleanerErrorRate::intValue);
    }

    public void recordFileCleanerStart() {
        fileCleanerRate.increment();
    }

    public void recordFileCleanerError() {
        fileCleanerErrorRate.increment();
    }

    public void recordFileCleanerTotalTime(long durationMs) {
        fileCleanerTotalTime.update(durationMs);
    }

    public void recordFileCleanerCompleted(int filesSize) {
        fileCleanerFiles.add(filesSize);
    }

    public void close() {
        metricsGroup.removeMetric(FILE_CLEANER_TOTAL_TIME);
        metricsGroup.removeMetric(FILE_CLEANER_RATE);
        metricsGroup.removeMetric(FILE_CLEANER_FILES_RATE);
        metricsGroup.removeMetric(FILE_CLEANER_ERROR_RATE);
    }
}
