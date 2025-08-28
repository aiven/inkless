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
package io.aiven.inkless.produce;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.yammer.metrics.core.Histogram;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;

@CoverageIgnore
class FileCommitterMetrics implements Closeable {
    private static final String FILE_TOTAL_LIFE_TIME = "FileTotalLifeTime";
    private static final String FILE_UPLOAD_AND_COMMIT_TIME = "FileUploadAndCommitTime";
    private static final String FILE_UPLOAD_TIME = "FileUploadTime";
    private static final String FILE_UPLOAD_RATE = "FileUploadRate";
    private static final String FILE_COMMIT_WAIT_TIME = "FileCommitWaitTime";
    private static final String FILE_COMMIT_TIME = "FileCommitTime";
    private static final String FILE_COMMIT_RATE = "FileCommitRate";
    private static final String CACHE_STORE_TIME = "CacheStoreTime";
    private static final String COMMIT_QUEUE_FILES = "CommitQueueFiles";
    private static final String COMMIT_QUEUE_BYTES = "CommitQueueBytes";
    private static final String FILE_SIZE = "FileSize";
    private static final String BATCHES_COUNT = "BatchesCount";
    private static final String BATCHES_COMMIT_RATE = "BatchesCommitRate";

    private final Time time;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(FileCommitter.class);
    private final Histogram fileTotalLifeTimeHistogram;
    private final Histogram fileUploadAndCommitTimeHistogram;
    private final Histogram fileUploadTimeHistogram;
    private final Histogram fileCommitTimeHistogram;
    private final Histogram fileCommitWaitTimeHistogram;
    private final Histogram fileSizeHistogram;
    private final Histogram batchesCountHistogram;
    private final Histogram cacheStoreTimeHistogram;
    private final LongAdder fileUploadRate = new LongAdder();
    private final LongAdder fileCommitRate = new LongAdder();
    private final LongAdder batchesCommitRate = new LongAdder();

    FileCommitterMetrics(final Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        fileTotalLifeTimeHistogram = metricsGroup.newHistogram(FILE_TOTAL_LIFE_TIME, true, Map.of());
        fileUploadAndCommitTimeHistogram = metricsGroup.newHistogram(FILE_UPLOAD_AND_COMMIT_TIME, true, Map.of());
        fileUploadTimeHistogram = metricsGroup.newHistogram(FILE_UPLOAD_TIME, true, Map.of());
        metricsGroup.newGauge(FILE_UPLOAD_RATE, fileUploadRate::intValue);
        fileCommitTimeHistogram = metricsGroup.newHistogram(FILE_COMMIT_TIME, true, Map.of());
        fileCommitWaitTimeHistogram = metricsGroup.newHistogram(FILE_COMMIT_WAIT_TIME, true, Map.of());
        metricsGroup.newGauge(FILE_COMMIT_RATE, fileCommitRate::intValue);
        metricsGroup.newGauge(BATCHES_COMMIT_RATE, batchesCommitRate::intValue);
        fileSizeHistogram = metricsGroup.newHistogram(FILE_SIZE, true, Map.of());
        batchesCountHistogram = metricsGroup.newHistogram(BATCHES_COUNT, true, Map.of());
        cacheStoreTimeHistogram = metricsGroup.newHistogram(CACHE_STORE_TIME, true, Map.of());
    }

    void initTotalFilesInProgressMetric(final Supplier<Integer> supplier) {
        metricsGroup.newGauge(COMMIT_QUEUE_FILES, Objects.requireNonNull(supplier, "supplier cannot be null"));
    }

    void initTotalBytesInProgressMetric(final Supplier<Integer> supplier) {
        metricsGroup.newGauge(COMMIT_QUEUE_BYTES, Objects.requireNonNull(supplier, "supplier cannot be null"));
    }

    void fileAdded(final int size) {
        fileSizeHistogram.update(size);
    }

    void batchesAdded(final int size) {
        batchesCountHistogram.update(size);
        batchesCommitRate.add(size);
    }

    void fileUploadFinished(final long durationMs) {
        fileUploadTimeHistogram.update(durationMs);
        fileUploadRate.increment();
    }

    void fileCommitFinished(final long durationMs) {
        fileCommitTimeHistogram.update(durationMs);
        fileCommitRate.increment();
    }

    void fileCommitWaitFinished(final long durationMs) {
        fileCommitWaitTimeHistogram.update(durationMs);
    }

    void fileFinished(final Instant fileStart, final Instant uploadAndCommitStart) {
        final Instant now = TimeUtils.durationMeasurementNow(time);
        fileTotalLifeTimeHistogram.update(Duration.between(fileStart, now).toMillis());
        fileUploadAndCommitTimeHistogram.update(Duration.between(uploadAndCommitStart, now).toMillis());
    }

    void cacheStoreFinished(final long durationMs) {
        cacheStoreTimeHistogram.update(durationMs);
    }

    @Override
    public void close() throws IOException {
        metricsGroup.removeMetric(COMMIT_QUEUE_FILES);
        metricsGroup.removeMetric(COMMIT_QUEUE_BYTES);
        metricsGroup.removeMetric(FILE_TOTAL_LIFE_TIME);
        metricsGroup.removeMetric(FILE_UPLOAD_AND_COMMIT_TIME);
        metricsGroup.removeMetric(FILE_UPLOAD_TIME);
        metricsGroup.removeMetric(FILE_UPLOAD_RATE);
        metricsGroup.removeMetric(FILE_COMMIT_TIME);
        metricsGroup.removeMetric(FILE_COMMIT_RATE);
        metricsGroup.removeMetric(FILE_SIZE);
        metricsGroup.removeMetric(FILE_COMMIT_WAIT_TIME);
        metricsGroup.removeMetric(CACHE_STORE_TIME);
        metricsGroup.removeMetric(BATCHES_COUNT);
        metricsGroup.removeMetric(BATCHES_COMMIT_RATE);
    }
}
