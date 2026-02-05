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
import com.yammer.metrics.core.Meter;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;

@CoverageIgnore
class FileCommitterMetrics implements Closeable {
    private static final String FILE_TOTAL_LIFE_TIME = "FileTotalLifeTime";
    private static final String FILE_UPLOAD_AND_COMMIT_TIME = "FileUploadAndCommitTime";
    private static final String FILE_UPLOAD_TIME = "FileUploadTime";
    private static final String FILE_UPLOAD_RATE = "FileUploadRate";
    private static final String FILE_UPLOAD_ERROR_RATE = "FileUploadErrorRate";
    private static final String FILE_COMMIT_WAIT_TIME = "FileCommitWaitTime";
    private static final String FILE_COMMIT_TIME = "FileCommitTime";
    private static final String FILE_COMMIT_RATE = "FileCommitRate";
    private static final String FILE_COMMIT_ERROR_RATE = "FileCommitErrorRate";
    private static final String CACHE_STORE_TIME = "CacheStoreTime";
    private static final String COMMIT_QUEUE_FILES = "CommitQueueFiles";
    private static final String COMMIT_QUEUE_BYTES = "CommitQueueBytes";
    private static final String FILE_SIZE = "FileSize";
    private static final String BATCHES_COUNT = "BatchesCount";
    private static final String BATCHES_COMMIT_RATE = "BatchesCommitRate";
    private static final String WRITE_RATE = "WriteRate";
    private static final String WRITE_ERROR_RATE = "WriteErrorRate";

    private final Time time;

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        FileCommitter.class.getPackageName(), FileCommitter.class.getSimpleName());
    private final Histogram fileTotalLifeTimeHistogram;
    private final Histogram fileUploadAndCommitTimeHistogram;
    private final Histogram fileUploadTimeHistogram;
    private final Histogram fileCommitTimeHistogram;
    private final Histogram fileCommitWaitTimeHistogram;
    private final Histogram fileSizeHistogram;
    private final Histogram batchesCountHistogram;
    private final Histogram cacheStoreTimeHistogram;
    private final Meter fileUploadRate;
    private final Meter fileUploadErrorRate;
    private final Meter fileCommitRate;
    private final Meter fileCommitErrorRate;
    private final Meter batchesCommitRate;
    private final Meter writeRate;
    private final Meter writeErrorRate;

    FileCommitterMetrics(final Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        fileTotalLifeTimeHistogram = metricsGroup.newHistogram(FILE_TOTAL_LIFE_TIME, true, Map.of());
        fileUploadAndCommitTimeHistogram = metricsGroup.newHistogram(FILE_UPLOAD_AND_COMMIT_TIME, true, Map.of());
        fileUploadTimeHistogram = metricsGroup.newHistogram(FILE_UPLOAD_TIME, true, Map.of());
        fileUploadRate = metricsGroup.newMeter(FILE_UPLOAD_RATE, "uploads", TimeUnit.SECONDS, Map.of());
        fileUploadErrorRate = metricsGroup.newMeter(FILE_UPLOAD_ERROR_RATE, "errors", TimeUnit.SECONDS, Map.of());
        fileCommitTimeHistogram = metricsGroup.newHistogram(FILE_COMMIT_TIME, true, Map.of());
        fileCommitWaitTimeHistogram = metricsGroup.newHistogram(FILE_COMMIT_WAIT_TIME, true, Map.of());
        fileCommitRate = metricsGroup.newMeter(FILE_COMMIT_RATE, "commits", TimeUnit.SECONDS, Map.of());
        fileCommitErrorRate = metricsGroup.newMeter(FILE_COMMIT_ERROR_RATE, "errors", TimeUnit.SECONDS, Map.of());
        batchesCommitRate = metricsGroup.newMeter(BATCHES_COMMIT_RATE, "batches", TimeUnit.SECONDS, Map.of());
        fileSizeHistogram = metricsGroup.newHistogram(FILE_SIZE, true, Map.of());
        batchesCountHistogram = metricsGroup.newHistogram(BATCHES_COUNT, true, Map.of());
        cacheStoreTimeHistogram = metricsGroup.newHistogram(CACHE_STORE_TIME, true, Map.of());
        writeRate = metricsGroup.newMeter(WRITE_RATE, "writes", TimeUnit.SECONDS, Map.of());
        writeErrorRate = metricsGroup.newMeter(WRITE_ERROR_RATE, "errors", TimeUnit.SECONDS, Map.of());
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
        batchesCommitRate.mark(size);
    }

    void fileUploadFinished(final long durationMs) {
        fileUploadTimeHistogram.update(durationMs);
        fileUploadRate.mark();
    }

    void fileUploadFailed() {
        fileUploadErrorRate.mark();
    }

    void fileCommitFinished(final long durationMs) {
        fileCommitTimeHistogram.update(durationMs);
        fileCommitRate.mark();
    }

    void fileCommitFailed() {
        fileCommitErrorRate.mark();
    }

    void fileCommitWaitFinished(final long durationMs) {
        fileCommitWaitTimeHistogram.update(durationMs);
    }

    void fileFinished(final Instant fileStart, final Instant uploadAndCommitStart) {
        final Instant now = TimeUtils.durationMeasurementNow(time);
        fileTotalLifeTimeHistogram.update(Duration.between(fileStart, now).toMillis());
        fileUploadAndCommitTimeHistogram.update(Duration.between(uploadAndCommitStart, now).toMillis());
    }

    void writeCompleted() {
        writeRate.mark();
    }

    void writeFailed() {
        writeErrorRate.mark();
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
        metricsGroup.removeMetric(FILE_UPLOAD_ERROR_RATE);
        metricsGroup.removeMetric(FILE_COMMIT_TIME);
        metricsGroup.removeMetric(FILE_COMMIT_RATE);
        metricsGroup.removeMetric(FILE_COMMIT_ERROR_RATE);
        metricsGroup.removeMetric(FILE_SIZE);
        metricsGroup.removeMetric(FILE_COMMIT_WAIT_TIME);
        metricsGroup.removeMetric(CACHE_STORE_TIME);
        metricsGroup.removeMetric(BATCHES_COUNT);
        metricsGroup.removeMetric(BATCHES_COMMIT_RATE);
        metricsGroup.removeMetric(WRITE_RATE);
        metricsGroup.removeMetric(WRITE_ERROR_RATE);
    }
}
