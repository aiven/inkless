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
package io.aiven.inkless.storage_backend.s3;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import software.amazon.awssdk.core.internal.metrics.SdkErrorType;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.metrics.MetricCollection;

import static org.assertj.core.api.Assertions.assertThat;

class MetricCollectorTest {
    private static final String GROUP = "s3-client-metrics";

    private final Metrics metrics = new Metrics();
    private final MetricCollector collector = new MetricCollector(metrics);

    @AfterEach
    void tearDown() {
        metrics.close();
    }

    @Test
    void recordsRequestAndLatencyForOperation() {
        collector.publish(collection("DeleteObjects", Duration.ofMillis(7), null));

        assertThat(doubleValue("delete-objects-requests-total")).isEqualTo(1.0);
        assertThat(doubleValue("delete-objects-time-max")).isEqualTo(7.0);
    }

    @Test
    void recordsErrorBothGloballyAndPerOperation() {
        collector.publish(collection("DeleteObjects", Duration.ofMillis(1), SdkErrorType.THROTTLING));

        // Untagged global metric preserves existing behavior.
        assertThat(doubleValue("throttling-errors-total")).isEqualTo(1.0);
        // New per-operation variant attributes the throttle to DeleteObjects.
        assertThat(doubleValue("throttling-errors-total", Map.of("operation", "DeleteObjects")))
            .isEqualTo(1.0);
        // A different operation is not credited.
        assertThat(taggedMetric("throttling-errors-total", Map.of("operation", "PutObject")))
            .isNull();
    }

    @Test
    void perOperationErrorsAreSeparatedByOperation() {
        collector.publish(collection("DeleteObjects", Duration.ofMillis(1), SdkErrorType.THROTTLING));
        collector.publish(collection("GetObject", Duration.ofMillis(1), SdkErrorType.THROTTLING));

        assertThat(doubleValue("throttling-errors-total")).isEqualTo(2.0);
        assertThat(doubleValue("throttling-errors-total", Map.of("operation", "DeleteObjects")))
            .isEqualTo(1.0);
        assertThat(doubleValue("throttling-errors-total", Map.of("operation", "GetObject")))
            .isEqualTo(1.0);
    }

    @Test
    void ambiguousOperationSkipsPerOperationButKeepsGlobal() {
        // Two OPERATION_NAME values make the operation ambiguous; the global error is still recorded
        // but no per-operation attribution is possible.
        final software.amazon.awssdk.metrics.MetricCollector root =
            software.amazon.awssdk.metrics.MetricCollector.create("ApiCall");
        root.reportMetric(CoreMetric.OPERATION_NAME, "DeleteObjects");
        root.reportMetric(CoreMetric.OPERATION_NAME, "GetObject");
        root.createChild("ApiCallAttempt").reportMetric(CoreMetric.ERROR_TYPE, SdkErrorType.THROTTLING.toString());

        collector.publish(root.collect());

        assertThat(doubleValue("throttling-errors-total")).isEqualTo(1.0);
        assertThat(taggedMetric("throttling-errors-total", Map.of("operation", "DeleteObjects")))
            .isNull();
    }

    private static MetricCollection collection(final String operation,
                                               final Duration duration,
                                               final SdkErrorType errorType) {
        final software.amazon.awssdk.metrics.MetricCollector root =
            software.amazon.awssdk.metrics.MetricCollector.create("ApiCall");
        root.reportMetric(CoreMetric.OPERATION_NAME, operation);
        root.reportMetric(CoreMetric.API_CALL_DURATION, duration);
        if (errorType != null) {
            root.createChild("ApiCallAttempt").reportMetric(CoreMetric.ERROR_TYPE, errorType.toString());
        }
        return root.collect();
    }

    private double doubleValue(final String name) {
        return (double) metrics.metric(metrics.metricName(name, GROUP)).metricValue();
    }

    private double doubleValue(final String name, final Map<String, String> tags) {
        return (double) taggedMetric(name, tags).metricValue();
    }

    private KafkaMetric taggedMetric(final String name, final Map<String, String> tags) {
        return metrics.metric(metrics.metricName(name, GROUP, "", tags));
    }
}
