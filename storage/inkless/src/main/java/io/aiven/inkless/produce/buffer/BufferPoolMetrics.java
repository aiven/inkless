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
package io.aiven.inkless.produce.buffer;

import org.apache.kafka.server.metrics.KafkaMetricsGroup;

import com.yammer.metrics.core.MetricName;

import java.io.Closeable;
import java.util.Map;

/**
 * JMX metrics for monitoring {@link BufferPool} health.
 *
 * <h2>Gauge Metrics (point-in-time values)</h2>
 * <ul>
 *   <li><b>ActiveBuffersCount:</b> Buffers currently in use (acquired but not yet released)</li>
 *   <li><b>TotalPoolSizeBytes:</b> Total memory allocated by pool across all size classes</li>
 * </ul>
 *
 * <h2>Counter Metrics (monotonically increasing since pool creation)</h2>
 * <ul>
 *   <li><b>HeapFallbackCount:</b> Cumulative times heap allocation was used when pool exhausted</li>
 *   <li><b>HeapFallbackBytesTotal:</b> Cumulative bytes allocated on heap as fallback</li>
 * </ul>
 *
 * <h2>Additional Counters for {@link ElasticBufferPool}</h2>
 * <ul>
 *   <li><b>PoolHitCount:</b> Cumulative times a buffer was successfully reused from pool</li>
 *   <li><b>PoolGrowthCount:</b> Cumulative times a new buffer was allocated to grow the pool</li>
 * </ul>
 *
 * <p><b>Monitoring guidance:</b> A high ratio of HeapFallbackCount to (PoolHitCount + PoolGrowthCount)
 * indicates the pool is frequently exhausted. Consider increasing {@code produce.buffer.pool.size.per.class}.
 */
public class BufferPoolMetrics implements Closeable {

    public static final String ACTIVE_BUFFERS_COUNT = "ActiveBuffersCount";
    public static final String TOTAL_POOL_SIZE_BYTES = "TotalPoolSizeBytes";
    public static final String HEAP_FALLBACK_COUNT = "HeapFallbackCount";
    public static final String HEAP_FALLBACK_BYTES_TOTAL = "HeapFallbackBytesTotal";
    public static final String POOL_HIT_COUNT = "PoolHitCount";
    public static final String POOL_GROWTH_COUNT = "PoolGrowthCount";

    private final KafkaMetricsGroup metricsGroup = new KafkaMetricsGroup(
        BufferPoolMetrics.class.getPackageName(), BufferPoolMetrics.class.getSimpleName());

    private final MetricName activeBuffersCountMetricName;
    private final MetricName totalPoolSizeBytesMetricName;
    private final MetricName heapFallbackCountMetricName;
    private final MetricName heapFallbackBytesTotalMetricName;
    // These may be null if the pool is not an ElasticBufferPool
    private final MetricName poolHitCountMetricName;
    private final MetricName poolGrowthCountMetricName;

    /**
     * Creates metrics for a buffer pool.
     *
     * <p>All {@link BufferPool} implementations will have the base metrics registered.
     * If the pool is an {@link ElasticBufferPool}, additional elastic-specific metrics
     * (PoolHitCount, PoolGrowthCount) will also be registered.
     *
     * @param pool the buffer pool to monitor
     */
    public BufferPoolMetrics(BufferPool pool) {
        activeBuffersCountMetricName = metricsGroup.metricName(ACTIVE_BUFFERS_COUNT, Map.of());
        metricsGroup.newGauge(activeBuffersCountMetricName, pool::activeBufferCount);

        totalPoolSizeBytesMetricName = metricsGroup.metricName(TOTAL_POOL_SIZE_BYTES, Map.of());
        metricsGroup.newGauge(totalPoolSizeBytesMetricName, pool::totalPoolSizeBytes);

        heapFallbackCountMetricName = metricsGroup.metricName(HEAP_FALLBACK_COUNT, Map.of());
        metricsGroup.newGauge(heapFallbackCountMetricName, pool::heapFallbackCount);

        heapFallbackBytesTotalMetricName = metricsGroup.metricName(HEAP_FALLBACK_BYTES_TOTAL, Map.of());
        metricsGroup.newGauge(heapFallbackBytesTotalMetricName, pool::heapFallbackBytes);

        // Register elastic-specific metrics if applicable
        if (pool instanceof ElasticBufferPool elasticPool) {
            poolHitCountMetricName = metricsGroup.metricName(POOL_HIT_COUNT, Map.of());
            metricsGroup.newGauge(poolHitCountMetricName, elasticPool::poolHitCount);

            poolGrowthCountMetricName = metricsGroup.metricName(POOL_GROWTH_COUNT, Map.of());
            metricsGroup.newGauge(poolGrowthCountMetricName, elasticPool::poolGrowthCount);
        } else {
            poolHitCountMetricName = null;
            poolGrowthCountMetricName = null;
        }
    }

    @Override
    public void close() {
        metricsGroup.removeMetric(activeBuffersCountMetricName);
        metricsGroup.removeMetric(totalPoolSizeBytesMetricName);
        metricsGroup.removeMetric(heapFallbackCountMetricName);
        metricsGroup.removeMetric(heapFallbackBytesTotalMetricName);
        if (poolHitCountMetricName != null) {
            metricsGroup.removeMetric(poolHitCountMetricName);
        }
        if (poolGrowthCountMetricName != null) {
            metricsGroup.removeMetric(poolGrowthCountMetricName);
        }
    }
}
