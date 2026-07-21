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

package io.aiven.inkless.storage_backend.gcs.integration;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Metrics;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayInputStream;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.fixtures.TestObjectKey;
import io.aiven.inkless.storage_backend.common.fixtures.TestUtils;
import io.aiven.inkless.storage_backend.gcs.GcsStorage;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

@ExtendWith(MockitoExtension.class)
@Testcontainers
public class GcsStorageMetricsTest {
    private static final String GCS_CLIENT_METRICS = "gcs-client-metrics";
    private static final int RESUMABLE_UPLOAD_CHUNK_SIZE = 256 * 1024;

    @Container
    static final FakeGcsServerContainer GCS_SERVER = new FakeGcsServerContainer();

    static Storage storageClient;

    final Metrics metrics = new Metrics();
    final GcsStorage storage = new GcsStorage(metrics);

    @BeforeAll
    static void setUpClass() {
        storageClient = StorageOptions.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .setHost(GCS_SERVER.url())
            .setProjectId("test-project")
            .build()
            .getService();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        final String bucketName = TestUtils.testNameToBucketName(testInfo);
        storageClient.create(BucketInfo.newBuilder(bucketName).build());

        final Map<String, Object> configs = Map.of(
            "gcs.bucket.name", bucketName,
            "gcs.endpoint.url", GCS_SERVER.url(),
            "gcs.resumable.upload.chunk.size", Integer.toString(RESUMABLE_UPLOAD_CHUNK_SIZE),
            "gcs.credentials.default", "false"
        );
        storage.configure(configs);
    }

    @Test
    void metricsShouldBeReported() throws Exception {
        final byte[] data = new byte[RESUMABLE_UPLOAD_CHUNK_SIZE + 1];

        final ObjectKey key = new TestObjectKey("x");

        storage.upload(key, new ByteArrayInputStream(data), data.length);
        try (final ReadableByteChannel channel = storage.fetch(key, ByteRange.maxRange())) {
            storage.readToByteBuffer(channel);
        }
        try (final ReadableByteChannel channel = storage.fetch(key, new ByteRange(0, 1))) {
            storage.readToByteBuffer(channel);
        }
        storage.delete(key);

        assertThat(getMetric("object-metadata-get-rate").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(getMetric("object-metadata-get-total").metricValue())
            .isEqualTo(2.0);

        assertThat(getMetric("object-get-rate").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(getMetric("object-get-total").metricValue())
            .isEqualTo(2.0);

        assertThat(getMetric("object-delete-rate").metricValue())
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(getMetric("object-delete-total").metricValue())
            .isEqualTo(1.0);
    }

    private KafkaMetric getMetric(String metricName) {
        final KafkaMetric metric = metrics.metric(metrics.metricName(metricName, GCS_CLIENT_METRICS));
        assertThat(metric).isNotNull();
        return metric;
    }
}
