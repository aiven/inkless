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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayInputStream;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

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

    private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    private static final String BUCKET_NAME = "test-bucket";

    private S3Storage storage;

    @BeforeAll
    static void setupS3() {
        S3_CONTAINER.createBucket(BUCKET_NAME);
    }

    @BeforeEach
    void setupStorage() {
        storage = new S3Storage();
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

    @Test
    void metricsShouldBeReported() throws Exception {
        final byte[] data = new byte[100];

        final ObjectKey key = new TestObjectKey("x");

        storage.upload(key, new ByteArrayInputStream(data), data.length);
        storage.fetch(key, ByteRange.maxRange());
        storage.fetch(key, new ByteRange(0, 1));
        storage.delete(key);
        storage.delete(Set.of(key));

        final ObjectName segmentCopyPerSecName = ObjectName.getInstance(
            "aiven.inkless.server.s3:type=s3-client-metrics");
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "get-object-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "get-object-requests-total"))
            .isEqualTo(2.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "get-object-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "get-object-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "put-object-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "put-object-requests-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "put-object-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "put-object-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-object-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-object-requests-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-object-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-object-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-objects-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-objects-requests-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-objects-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-objects-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }
}
