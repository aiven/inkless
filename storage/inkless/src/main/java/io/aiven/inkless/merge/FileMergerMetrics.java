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

import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.Histogram;

import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class FileMergerMetrics {
    static final String FILE_MERGE_TOTAL_TIME = "FileMergeTotalTime";
    static final String FILE_UPLOAD_TIME = "FileUploadTime";
    static final String FILE_MERGE_RATE = "FileMergeRate";
    static final String FILE_MERGE_FILES_RATE = "FileMergeFilesRate";
    static final String FILE_MERGE_ERROR_RATE = "FileMergeErrorRate";

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(FileMerger.class.getPackageName(), FileMerger.class.getSimpleName());
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
