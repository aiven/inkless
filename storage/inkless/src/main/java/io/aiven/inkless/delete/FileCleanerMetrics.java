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

import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class FileCleanerMetrics {
    static final String FILE_CLEANER_TOTAL_TIME = "FileCleanerTotalTime";
    static final String FILE_CLEANER_RATE = "FileCleanerRate";
    static final String FILE_CLEANER_FILES_RATE = "FileCleanerFilesRate";
    static final String FILE_CLEANER_ERROR_RATE = "FileCleanerErrorRate";

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(FileCleaner.class.getPackageName(), FileCleaner.class.getSimpleName());
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
