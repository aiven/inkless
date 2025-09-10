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
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.fixtures.TestObjectKey;
import io.aiven.inkless.storage_backend.common.fixtures.TestUtils;
import io.aiven.inkless.storage_backend.gcs.GcsStorage;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

@ExtendWith(MockitoExtension.class)
@Testcontainers
public class GcsStorageMetricsTest {
    private static final int RESUMABLE_UPLOAD_CHUNK_SIZE = 256 * 1024;

    @Container
    static final FakeGcsServerContainer GCS_SERVER = new FakeGcsServerContainer();

    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
    static Storage storageClient;

    GcsStorage storage;

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

        storage = new GcsStorage();
        final Map<String, Object> configs = Map.of(
            "gcs.bucket.name", bucketName,
            "gcs.endpoint.url", GCS_SERVER.url(),
            "gcs.resumable.upload.chunk.size", Integer.toString(RESUMABLE_UPLOAD_CHUNK_SIZE),
            "gcs.credentials.default", "false"
        );
        storage.configure(configs);
    }

    @Test
    void metricsShouldBeReported() throws StorageBackendException, IOException, JMException {
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

        final ObjectName gcsMetricsObjectName =
            ObjectName.getInstance("io.aiven.inkless.storage.gcs:type=gcs-client-metrics");
        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-metadata-get-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-metadata-get-total"))
            .isEqualTo(2.0);

        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-get-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-get-total"))
            .isEqualTo(2.0);

        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-delete-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-delete-total"))
            .isEqualTo(1.0);
    }
}
