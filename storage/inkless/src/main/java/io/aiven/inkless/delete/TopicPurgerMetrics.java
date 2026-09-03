/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class TopicPurgerMetrics {
    private static final String GROUP = TopicPurger.class.getSimpleName();

    static final String TOPIC_PURGER_TOTAL_TIME = "TopicPurgerTotalTime";
    private static final String TOPIC_PURGER_TOTAL_TIME_DOC = "Total time spent on a topic purge cycle in milliseconds";
    static final String TOPIC_PURGER_RATE = "TopicPurgerRate";
    private static final String TOPIC_PURGER_RATE_DOC = "Total number of topic purge cycles started";
    static final String TOPIC_PURGER_BATCHES_RATE = "TopicPurgerBatchesRate";
    private static final String TOPIC_PURGER_BATCHES_RATE_DOC = "Total number of batches deleted from soft-deleted logs";
    static final String TOPIC_PURGER_LOGS_RATE = "TopicPurgerLogsRate";
    private static final String TOPIC_PURGER_LOGS_RATE_DOC = "Total number of soft-deleted logs fully purged";
    static final String TOPIC_PURGER_FILES_MARKED_RATE = "TopicPurgerFilesMarkedRate";
    private static final String TOPIC_PURGER_FILES_MARKED_RATE_DOC = "Total number of files marked for deletion by topic purge";
    static final String TOPIC_PURGER_ERROR_RATE = "TopicPurgerErrorRate";
    private static final String TOPIC_PURGER_ERROR_RATE_DOC = "Total number of topic purge errors";
    static final String LAST_SUCCESSFUL_TOPIC_PURGE_AGE_MS = "LastSuccessfulTopicPurgeAgeMs";
    private static final String LAST_SUCCESSFUL_TOPIC_PURGE_AGE_MS_DOC = "Milliseconds since the last topic purge "
        + "cycle completed without error, including cycles that found nothing to purge; -1 if no cycle has "
        + "completed since startup. This stays fresh while a backlog remains, so pair it with "
        + "TopicPurgerWorkRemain and TopicPurgerBatchesRate.";
    static final String TOPIC_PURGER_CYCLE_SATURATED_RATE = "TopicPurgerCycleSaturatedRate";
    private static final String TOPIC_PURGER_CYCLE_SATURATED_RATE_DOC = "Total number of topic purge cycles that "
        + "hit topic.purger.max.batches.per.cycle, leaving work for the next cycle";
    static final String TOPIC_PURGER_WORK_REMAIN = "TopicPurgerWorkRemain";
    private static final String TOPIC_PURGER_WORK_REMAIN_DOC = "1 if the last successful topic purge cycle left "
        + "soft-deleted logs, including rows another broker holds; 0 otherwise";

    public static List<MetricNameTemplate> all() {
        return List.of(
            new MetricNameTemplate(TOPIC_PURGER_TOTAL_TIME, GROUP, TOPIC_PURGER_TOTAL_TIME_DOC),
            new MetricNameTemplate(TOPIC_PURGER_RATE, GROUP, TOPIC_PURGER_RATE_DOC),
            new MetricNameTemplate(TOPIC_PURGER_BATCHES_RATE, GROUP, TOPIC_PURGER_BATCHES_RATE_DOC),
            new MetricNameTemplate(TOPIC_PURGER_LOGS_RATE, GROUP, TOPIC_PURGER_LOGS_RATE_DOC),
            new MetricNameTemplate(TOPIC_PURGER_FILES_MARKED_RATE, GROUP, TOPIC_PURGER_FILES_MARKED_RATE_DOC),
            new MetricNameTemplate(TOPIC_PURGER_ERROR_RATE, GROUP, TOPIC_PURGER_ERROR_RATE_DOC),
            new MetricNameTemplate(LAST_SUCCESSFUL_TOPIC_PURGE_AGE_MS, GROUP, LAST_SUCCESSFUL_TOPIC_PURGE_AGE_MS_DOC),
            new MetricNameTemplate(TOPIC_PURGER_CYCLE_SATURATED_RATE, GROUP, TOPIC_PURGER_CYCLE_SATURATED_RATE_DOC),
            new MetricNameTemplate(TOPIC_PURGER_WORK_REMAIN, GROUP, TOPIC_PURGER_WORK_REMAIN_DOC)
        );
    }

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        TopicPurger.class.getPackageName(), TopicPurger.class.getSimpleName());
    private final Time time;
    private final Histogram topicPurgerTotalTime;
    private final LongAdder topicPurgerRate = new LongAdder();
    private final LongAdder topicPurgerBatches = new LongAdder();
    private final LongAdder topicPurgerLogs = new LongAdder();
    private final LongAdder topicPurgerFilesMarked = new LongAdder();
    private final LongAdder topicPurgerErrorRate = new LongAdder();
    final AtomicLong lastSuccessfulPurgeTimeMs = new AtomicLong(-1);
    final LongAdder topicPurgerCycleSaturated = new LongAdder();
    final AtomicInteger topicPurgerWorkRemain = new AtomicInteger();

    public TopicPurgerMetrics(final Time time) {
        this.time = Objects.requireNonNull(time, "time cannot be null");
        topicPurgerTotalTime = metricsGroup.newHistogram(TOPIC_PURGER_TOTAL_TIME, true, Map.of());
        metricsGroup.newGauge(TOPIC_PURGER_RATE, topicPurgerRate::sum);
        metricsGroup.newGauge(TOPIC_PURGER_BATCHES_RATE, topicPurgerBatches::sum);
        metricsGroup.newGauge(TOPIC_PURGER_LOGS_RATE, topicPurgerLogs::sum);
        metricsGroup.newGauge(TOPIC_PURGER_FILES_MARKED_RATE, topicPurgerFilesMarked::sum);
        metricsGroup.newGauge(TOPIC_PURGER_ERROR_RATE, topicPurgerErrorRate::sum);
        metricsGroup.newGauge(LAST_SUCCESSFUL_TOPIC_PURGE_AGE_MS, () -> {
            final long last = lastSuccessfulPurgeTimeMs.get();
            return last == -1 ? -1L : time.milliseconds() - last;
        });
        metricsGroup.newGauge(TOPIC_PURGER_CYCLE_SATURATED_RATE, topicPurgerCycleSaturated::sum);
        metricsGroup.newGauge(TOPIC_PURGER_WORK_REMAIN, topicPurgerWorkRemain::intValue);
    }

    public void recordTopicPurgerStart() {
        topicPurgerRate.increment();
    }

    public void recordTopicPurgerError() {
        topicPurgerErrorRate.increment();
    }

    public void recordTopicPurgerTotalTime(long durationMs) {
        topicPurgerTotalTime.update(durationMs);
    }

    public void recordTopicPurgerCompleted(long batchesDeleted, int logsPurged, int filesMarked) {
        topicPurgerBatches.add(batchesDeleted);
        topicPurgerLogs.add(logsPurged);
        topicPurgerFilesMarked.add(filesMarked);
    }

    public void recordTopicPurgerCycleSucceeded() {
        lastSuccessfulPurgeTimeMs.set(time.milliseconds());
    }

    public void recordTopicPurgerCycleSaturated() {
        topicPurgerCycleSaturated.increment();
    }

    public void recordTopicPurgerWorkRemain(boolean moreRemain) {
        topicPurgerWorkRemain.set(moreRemain ? 1 : 0);
    }

    public void close() {
        metricsGroup.removeMetric(TOPIC_PURGER_TOTAL_TIME);
        metricsGroup.removeMetric(TOPIC_PURGER_RATE);
        metricsGroup.removeMetric(TOPIC_PURGER_BATCHES_RATE);
        metricsGroup.removeMetric(TOPIC_PURGER_LOGS_RATE);
        metricsGroup.removeMetric(TOPIC_PURGER_FILES_MARKED_RATE);
        metricsGroup.removeMetric(TOPIC_PURGER_ERROR_RATE);
        metricsGroup.removeMetric(LAST_SUCCESSFUL_TOPIC_PURGE_AGE_MS);
        metricsGroup.removeMetric(TOPIC_PURGER_CYCLE_SATURATED_RATE);
        metricsGroup.removeMetric(TOPIC_PURGER_WORK_REMAIN);
    }
}
