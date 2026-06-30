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

import org.apache.kafka.common.MetricNameTemplate;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.groupcdg.pitest.annotations.CoverageIgnore;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.CommitBatchRequest;

@CoverageIgnore
public class FileCommitterMetrics implements Closeable {
    private static final String GROUP = FileCommitter.class.getSimpleName();

    private static final String FILE_TOTAL_LIFE_TIME = "FileTotalLifeTime";
    private static final String FILE_TOTAL_LIFE_TIME_DOC = "Total lifetime of a file from creation to commit completion in milliseconds";
    private static final String FILE_UPLOAD_AND_COMMIT_TIME = "FileUploadAndCommitTime";
    private static final String FILE_UPLOAD_AND_COMMIT_TIME_DOC = "Time spent uploading and committing a file in milliseconds";
    private static final String FILE_UPLOAD_TIME = "FileUploadTime";
    private static final String FILE_UPLOAD_TIME_DOC = "Time spent uploading a file to object storage in milliseconds";
    private static final String FILE_UPLOAD_RATE = "FileUploadRate";
    private static final String FILE_UPLOAD_RATE_DOC = "Rate of successful file uploads per second";
    private static final String FILE_UPLOAD_ERROR_RATE = "FileUploadErrorRate";
    private static final String FILE_UPLOAD_ERROR_RATE_DOC = "Rate of failed file uploads per second";
    private static final String FILE_COMMIT_WAIT_TIME = "FileCommitWaitTime";
    private static final String FILE_COMMIT_WAIT_TIME_DOC = "Time a file waits before commit starts in milliseconds";
    private static final String FILE_COMMIT_TIME = "FileCommitTime";
    private static final String FILE_COMMIT_TIME_DOC = "Time spent committing a file to the control plane in milliseconds";
    private static final String FILE_COMMIT_RATE = "FileCommitRate";
    private static final String FILE_COMMIT_RATE_DOC = "Rate of successful file commits per second";
    private static final String FILE_COMMIT_ERROR_RATE = "FileCommitErrorRate";
    private static final String FILE_COMMIT_ERROR_RATE_DOC = "Rate of failed file commits per second";
    private static final String CACHE_STORE_TIME = "CacheStoreTime";
    private static final String CACHE_STORE_TIME_DOC = "Time spent storing file data in the cache after commit in milliseconds";
    private static final String COMMIT_QUEUE_FILES = "CommitQueueFiles";
    private static final String COMMIT_QUEUE_FILES_DOC = "Current number of files waiting to be committed";
    private static final String COMMIT_QUEUE_BYTES = "CommitQueueBytes";
    private static final String COMMIT_QUEUE_BYTES_DOC = "Current total bytes of files waiting to be committed";
    private static final String FILE_SIZE = "FileSize";
    private static final String FILE_SIZE_DOC = "Size of committed files in bytes";
    private static final String BATCHES_COUNT = "BatchesCount";
    private static final String BATCHES_COUNT_DOC = "Number of batches per committed file";
    private static final String BATCHES_COMMIT_RATE = "BatchesCommitRate";
    private static final String BATCHES_COMMIT_RATE_DOC = "Rate of batches committed per second";
    private static final String PARTITIONS_PER_COMMIT = "PartitionsPerCommit";
    private static final String PARTITIONS_PER_COMMIT_DOC = "Number of distinct topic-partitions in a committed file";
    private static final String BATCHES_PER_PARTITION_PER_COMMIT = "BatchesPerPartitionPerCommit";
    private static final String BATCHES_PER_PARTITION_PER_COMMIT_DOC = "Number of batches a single topic-partition contributes to a committed file (per-partition fan-in).";
    private static final String WRITE_RATE = "WriteRate";
    private static final String WRITE_RATE_DOC = "Rate of successful write operations per second";
    private static final String WRITE_ERROR_RATE = "WriteErrorRate";
    private static final String WRITE_ERROR_RATE_DOC = "Rate of failed write operations per second";

    /**
     * This method returns a list of all the metric name templates for the FileCommitterMetrics class.
     * This is used for documentation purposes only.
     */
    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(FILE_TOTAL_LIFE_TIME, GROUP, FILE_TOTAL_LIFE_TIME_DOC),
            new MetricNameTemplate(FILE_UPLOAD_AND_COMMIT_TIME, GROUP, FILE_UPLOAD_AND_COMMIT_TIME_DOC),
            new MetricNameTemplate(FILE_UPLOAD_TIME, GROUP, FILE_UPLOAD_TIME_DOC),
            new MetricNameTemplate(FILE_UPLOAD_RATE, GROUP, FILE_UPLOAD_RATE_DOC),
            new MetricNameTemplate(FILE_UPLOAD_ERROR_RATE, GROUP, FILE_UPLOAD_ERROR_RATE_DOC),
            new MetricNameTemplate(FILE_COMMIT_TIME, GROUP, FILE_COMMIT_TIME_DOC),
            new MetricNameTemplate(FILE_COMMIT_WAIT_TIME, GROUP, FILE_COMMIT_WAIT_TIME_DOC),
            new MetricNameTemplate(FILE_COMMIT_RATE, GROUP, FILE_COMMIT_RATE_DOC),
            new MetricNameTemplate(FILE_COMMIT_ERROR_RATE, GROUP, FILE_COMMIT_ERROR_RATE_DOC),
            new MetricNameTemplate(BATCHES_COMMIT_RATE, GROUP, BATCHES_COMMIT_RATE_DOC),
            new MetricNameTemplate(FILE_SIZE, GROUP, FILE_SIZE_DOC),
            new MetricNameTemplate(BATCHES_COUNT, GROUP, BATCHES_COUNT_DOC),
            new MetricNameTemplate(PARTITIONS_PER_COMMIT, GROUP, PARTITIONS_PER_COMMIT_DOC),
            new MetricNameTemplate(BATCHES_PER_PARTITION_PER_COMMIT, GROUP, BATCHES_PER_PARTITION_PER_COMMIT_DOC),
            new MetricNameTemplate(CACHE_STORE_TIME, GROUP, CACHE_STORE_TIME_DOC),
            new MetricNameTemplate(WRITE_RATE, GROUP, WRITE_RATE_DOC),
            new MetricNameTemplate(WRITE_ERROR_RATE, GROUP, WRITE_ERROR_RATE_DOC),
            new MetricNameTemplate(COMMIT_QUEUE_FILES, GROUP, COMMIT_QUEUE_FILES_DOC),
            new MetricNameTemplate(COMMIT_QUEUE_BYTES, GROUP, COMMIT_QUEUE_BYTES_DOC)
        );
    }

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
    // Visible for testing.
    final Histogram partitionsPerCommitHistogram;
    final Histogram batchesPerPartitionPerCommitHistogram;
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
        partitionsPerCommitHistogram = metricsGroup.newHistogram(PARTITIONS_PER_COMMIT, true, Map.of());
        batchesPerPartitionPerCommitHistogram = metricsGroup.newHistogram(BATCHES_PER_PARTITION_PER_COMMIT, true, Map.of());
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

    /**
     * Records the partition fan-in of a committed file: the number of distinct topic-partitions, and the
     * number of batches each one contributed.
     *
     * <p>Relies on {@code commitBatchRequests} being grouped by topic-partition (they are sorted that way
     * in {@link BatchBuffer#close()}), so partitions and their batch counts are computed in a single
     * linear pass over contiguous runs — no intermediate grouping map.
     *
     * @param sortedRequests the committed batch requests, grouped by topic-partition
     */
    void partitionFanInAdded(final List<CommitBatchRequest> sortedRequests) {
        if (sortedRequests.isEmpty()) {
            return;
        }
        int partitions = 0;
        int runLength = 0;
        TopicIdPartition current = null;
        for (final CommitBatchRequest request : sortedRequests) {
            final TopicIdPartition partition = request.topicIdPartition();
            if (!partition.equals(current)) {
                if (current != null) {
                    batchesPerPartitionPerCommitHistogram.update(runLength);
                }
                partitions++;
                runLength = 0;
                current = partition;
            }
            runLength++;
        }
        batchesPerPartitionPerCommitHistogram.update(runLength); // flush the final run
        partitionsPerCommitHistogram.update(partitions);
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
        metricsGroup.removeMetric(PARTITIONS_PER_COMMIT);
        metricsGroup.removeMetric(BATCHES_PER_PARTITION_PER_COMMIT);
        metricsGroup.removeMetric(WRITE_RATE);
        metricsGroup.removeMetric(WRITE_ERROR_RATE);
    }
}
