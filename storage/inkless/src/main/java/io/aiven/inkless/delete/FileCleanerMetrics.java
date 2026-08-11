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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
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
    static final String FILE_CLEANER_FILES_FAILED_RATE = "FileCleanerFilesFailedRate";
    private static final String FILE_CLEANER_FILES_FAILED_RATE_DOC = "Total number of files the storage backend did "
        + "not confirm deleted; they stay marked for deletion and are retried on a later cycle";
    static final String LAST_SUCCESSFUL_FILE_CLEANUP_AGE_MS = "LastSuccessfulFileCleanupAgeMs";
    private static final String LAST_SUCCESSFUL_FILE_CLEANUP_AGE_MS_DOC = "Milliseconds since the last file cleaning "
        + "cycle completed without error, including cycles that found nothing to delete; -1 if no cycle has "
        + "completed since startup";

    /**
     * This method returns a list of all the metric name templates for the FileCleanerMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(FILE_CLEANER_TOTAL_TIME, GROUP, FILE_CLEANER_TOTAL_TIME_DOC),
            new MetricNameTemplate(FILE_CLEANER_RATE, GROUP, FILE_CLEANER_RATE_DOC),
            new MetricNameTemplate(FILE_CLEANER_FILES_RATE, GROUP, FILE_CLEANER_FILES_RATE_DOC),
            new MetricNameTemplate(FILE_CLEANER_ERROR_RATE, GROUP, FILE_CLEANER_ERROR_RATE_DOC),
            new MetricNameTemplate(FILE_CLEANER_FILES_FAILED_RATE, GROUP, FILE_CLEANER_FILES_FAILED_RATE_DOC),
            new MetricNameTemplate(LAST_SUCCESSFUL_FILE_CLEANUP_AGE_MS, GROUP, LAST_SUCCESSFUL_FILE_CLEANUP_AGE_MS_DOC)
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        FileCleaner.class.getPackageName(), FileCleaner.class.getSimpleName());
    private final Time time;
    private final Histogram fileCleanerTotalTime;
    private final LongAdder fileCleanerRate = new LongAdder();
    private final LongAdder fileCleanerFiles = new LongAdder();
    private final LongAdder fileCleanerErrorRate = new LongAdder();
    // package-private for tests, following ClientAzAwarenessMetrics
    final LongAdder fileCleanerFilesFailed = new LongAdder();
    // package-private for tests, following FileCommitterMetrics
    final AtomicLong lastSuccessfulCleanupTimeMs = new AtomicLong(-1);

    public FileCleanerMetrics(final Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        fileCleanerTotalTime = metricsGroup.newHistogram(FILE_CLEANER_TOTAL_TIME, true, Map.of());
        metricsGroup.newGauge(FILE_CLEANER_RATE, fileCleanerRate::intValue);
        metricsGroup.newGauge(FILE_CLEANER_FILES_RATE, fileCleanerFiles::intValue);
        metricsGroup.newGauge(FILE_CLEANER_ERROR_RATE, fileCleanerErrorRate::intValue);
        metricsGroup.newGauge(FILE_CLEANER_FILES_FAILED_RATE, fileCleanerFilesFailed::intValue);
        metricsGroup.newGauge(LAST_SUCCESSFUL_FILE_CLEANUP_AGE_MS, () -> {
            final long last = lastSuccessfulCleanupTimeMs.get();
            return last == -1 ? -1L : time.milliseconds() - last;
        });
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

    public void recordFileCleanerFilesFailed(int filesSize) {
        fileCleanerFilesFailed.add(filesSize);
    }

    /**
     * Marks a cycle as having completed without error. Called for every such cycle, including ones with
     * no work, so the gauge tracks liveness rather than delete volume.
     */
    public void recordFileCleanerCycleSucceeded() {
        lastSuccessfulCleanupTimeMs.set(time.milliseconds());
    }

    public void close() {
        metricsGroup.removeMetric(FILE_CLEANER_TOTAL_TIME);
        metricsGroup.removeMetric(FILE_CLEANER_RATE);
        metricsGroup.removeMetric(FILE_CLEANER_FILES_RATE);
        metricsGroup.removeMetric(FILE_CLEANER_ERROR_RATE);
        metricsGroup.removeMetric(FILE_CLEANER_FILES_FAILED_RATE);
        metricsGroup.removeMetric(LAST_SUCCESSFUL_FILE_CLEANUP_AGE_MS);
    }
}
