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
package io.aiven.inkless.common.metrics;

import org.apache.kafka.common.MetricNameTemplate;

import java.util.List;

public class ThreadPoolMonitorMetricsRegistry {
    public static final String METRIC_CONFIG = "io.aiven.inkless.thread-pool";

    static final String ACTIVE_THREADS = "active-thread-count";
    static final String ACTIVE_THREADS_TOTAL = ACTIVE_THREADS + "-total";
    static final String ACTIVE_THREADS_TOTAL_DOC = "Number of threads currently executing tasks";
    static final String POOL_SIZE = "pool-size";
    static final String POOL_SIZE_TOTAL = POOL_SIZE + "-total";
    static final String POOL_SIZE_TOTAL_DOC = "Current number of threads in the pool";
    static final String PARALLELISM = "parallelism";
    static final String PARALLELISM_TOTAL = PARALLELISM + "-total";
    static final String PARALLELISM_TOTAL_DOC = "Targeted parallelism level of the pool";
    static final String QUEUED_TASK_COUNT = "queued-task-count";
    static final String QUEUED_TASK_COUNT_TOTAL = QUEUED_TASK_COUNT + "-total";
    static final String QUEUED_TASK_COUNT_TOTAL_DOC = "Tasks submitted to the pool "
        + "that have not yet begun executing.";
    static final String AVG_IDLE_PERCENT = "avg-idle-percent";
    static final String AVG_IDLE_PERCENT_DOC = "Average idle percent of the pool";

    final MetricNameTemplate activeThreadsTotalMetricName;
    final MetricNameTemplate poolSizeTotalMetricName;
    final MetricNameTemplate parallelismTotalMetricName;
    final MetricNameTemplate queuedTaskCountTotalMetricName;
    final MetricNameTemplate avgIdlePercentMetricName;

    public ThreadPoolMonitorMetricsRegistry(final String groupName) {
        activeThreadsTotalMetricName = new MetricNameTemplate(
            ACTIVE_THREADS_TOTAL,
            groupName,
            ACTIVE_THREADS_TOTAL_DOC
        );
        poolSizeTotalMetricName = new MetricNameTemplate(
            POOL_SIZE_TOTAL,
            groupName,
            POOL_SIZE_TOTAL_DOC
        );
        parallelismTotalMetricName = new MetricNameTemplate(
            PARALLELISM_TOTAL,
            groupName,
            PARALLELISM_TOTAL_DOC
        );
        queuedTaskCountTotalMetricName = new MetricNameTemplate(
            QUEUED_TASK_COUNT_TOTAL,
            groupName,
            QUEUED_TASK_COUNT_TOTAL_DOC
        );
        avgIdlePercentMetricName = new MetricNameTemplate(
            "avg-idle-percent",
            groupName,
            "Average idle percent of the pool"
        );
    }

    public List<MetricNameTemplate> all() {
        return List.of(
            activeThreadsTotalMetricName,
            poolSizeTotalMetricName,
            parallelismTotalMetricName,
            queuedTaskCountTotalMetricName
        );
    }
}
