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
package io.aiven.inkless.storage_backend.s3.integration;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.fixtures.TestObjectKey;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.test_utils.MinioContainer;
import io.aiven.inkless.test_utils.S3TestContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

@Tag("integration")
@Testcontainers
class S3StorageMetricsTest {

    @Container
    private static final MinioContainer S3_CONTAINER = S3TestContainer.minio();

    private static final String BUCKET_NAME = "test-bucket";
    public static final String S3_CLIENT_METRICS = "s3-client-metrics";

    private final Metrics metrics = new Metrics();
    private final S3Storage storage = new S3Storage(metrics);

    @BeforeAll
    static void setupS3() {
        S3_CONTAINER.createBucket(BUCKET_NAME);
    }

    @BeforeEach
    void setupStorage() {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", S3_CONTAINER.getRegion(),
            "s3.endpoint.url", S3_CONTAINER.getEndpoint(),
            "aws.access.key.id", S3_CONTAINER.getAccessKey(),
            "aws.secret.access.key", S3_CONTAINER.getSecretKey(),
            "s3.path.style.access.enabled", true
        );
        storage.configure(configs);
    }

    @AfterEach
    void tearDown() throws Exception {
        storage.close();
    }

    @Test
    void metricsShouldBeReported() throws Exception {
        final byte[] data = new byte[100];

        final ObjectKey key = new TestObjectKey("x");

        storage.upload(key, new ByteArrayInputStream(data), data.length);
        storage.fetch(key, ByteRange.maxRange());
        storage.fetch(key, new ByteRange(0, 1));
        storage.delete(key);
        storage.delete(Set.of(key));

        assertThat(getMetric("get-object-requests-rate").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(getMetric("get-object-requests-total").metricValue())
            .isEqualTo(2.0);
        assertThat(getMetric("get-object-time-avg").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(getMetric("get-object-time-max").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(getMetric("put-object-requests-rate").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(getMetric("put-object-requests-total").metricValue())
            .isEqualTo(1.0);
        assertThat(getMetric("put-object-time-avg").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(getMetric("put-object-time-max").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(getMetric("delete-object-requests-rate").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(getMetric("delete-object-requests-total").metricValue())
            .isEqualTo(1.0);
        assertThat(getMetric("delete-object-time-avg").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(getMetric("delete-object-time-max").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(getMetric("delete-objects-requests-rate").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(getMetric("delete-objects-requests-total").metricValue())
            .isEqualTo(1.0);
        assertThat(getMetric("delete-objects-time-avg").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(getMetric("delete-objects-time-max").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }

    private KafkaMetric getMetric(String metricName) {
        return metrics.metric(metrics.metricName(metricName, S3_CLIENT_METRICS));
    }
}
