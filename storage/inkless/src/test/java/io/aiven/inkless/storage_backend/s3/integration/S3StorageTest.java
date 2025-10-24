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

import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.fixtures.BaseStorageTest;
import io.aiven.inkless.storage_backend.common.fixtures.TestUtils;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.test_utils.MinioContainer;
import io.aiven.inkless.test_utils.S3TestContainer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@Tag("integration")
public class S3StorageTest extends BaseStorageTest {
    @Container
    private static final MinioContainer S3_CONTAINER = S3TestContainer.minio();

    private static S3Client s3Client;
    private String bucketName;

    @BeforeAll
    static void setUpClass() {
        s3Client = S3_CONTAINER.getS3Client();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        bucketName = TestUtils.testNameToBucketName(testInfo);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }

    @AfterAll
    static void closeAll() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Override
    protected StorageBackend storage() {
        final S3Storage s3Storage = new S3Storage(new Metrics());
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", S3_CONTAINER.getRegion(),
            "s3.endpoint.url", S3_CONTAINER.getEndpoint(),
            "aws.access.key.id", S3_CONTAINER.getAccessKey(),
            "aws.secret.access.key", S3_CONTAINER.getSecretKey(),
            "s3.path.style.access.enabled", true
        );
        s3Storage.configure(configs);
        return s3Storage;
    }

    @Override
    protected void testUploadUndersizedStream() throws IOException {
        try (StorageBackend storage = storage()) {
            final byte[] content = "content".getBytes();
            final long expectedLength = content.length + 1;

            assertThatThrownBy(() -> storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(content), expectedLength))
                    .isInstanceOf(StorageBackendException.class)
                    // This implementation has a different message
                    .hasMessage("Failed to upload key")
                    .cause()
                    .hasMessageContaining("You did not provide the number of bytes specified by the Content-Length HTTP header");
        }
    }

    @Test
    protected void testUploadOversizeStream() throws IOException {
        try (StorageBackend storage = storage()) {
            final byte[] content = "content".getBytes();
            final long expectedLength = content.length - 1;

            assertThatThrownBy(() -> storage.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(content), expectedLength))
                    .isInstanceOf(StorageBackendException.class)
                    // This implementation has a different message
                    .hasMessage("Object key created with incorrect length, input stream has remaining content");
        }
    }
}
