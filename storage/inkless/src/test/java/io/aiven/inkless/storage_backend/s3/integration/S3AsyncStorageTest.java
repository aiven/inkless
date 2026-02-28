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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.fixtures.TestObjectKey;
import io.aiven.inkless.storage_backend.common.fixtures.TestUtils;
import io.aiven.inkless.storage_backend.s3.S3AsyncStorage;
import io.aiven.inkless.test_utils.MinioContainer;
import io.aiven.inkless.test_utils.S3TestContainer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
@Tag("integration")
public class S3AsyncStorageTest {

    private static String readBuffer(ByteBuffer buffer) {
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
    @Container
    private static final MinioContainer S3_CONTAINER = S3TestContainer.minio();

    private static final ObjectKey TEST_KEY = new TestObjectKey("test-key");

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

    private S3AsyncStorage createStorage() {
        final S3AsyncStorage storage = new S3AsyncStorage(new Metrics());
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", S3_CONTAINER.getRegion(),
            "s3.endpoint.url", S3_CONTAINER.getEndpoint(),
            "aws.access.key.id", S3_CONTAINER.getAccessKey(),
            "aws.secret.access.key", S3_CONTAINER.getSecretKey(),
            "s3.path.style.access.enabled", true
        );
        storage.configure(configs);
        return storage;
    }

    @Test
    void testUploadFetchDelete() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            final byte[] data = "some file content".getBytes(StandardCharsets.UTF_8);
            final ByteBuffer uploadBuffer = ByteBuffer.wrap(data);

            // Upload
            storage.upload(TEST_KEY, uploadBuffer).get();

            // Fetch full content
            final ByteBuffer fetchResult = storage.fetch(TEST_KEY, new ByteRange(0, data.length)).get();
            assertThat(readBuffer(fetchResult)).isEqualTo("some file content");

            // Fetch partial content
            final ByteBuffer partialResult = storage.fetch(TEST_KEY, new ByteRange(5, 4)).get();
            assertThat(readBuffer(partialResult)).isEqualTo("file");

            // Delete
            storage.delete(TEST_KEY).get();

            // Verify deleted
            assertThatThrownBy(() -> storage.fetch(TEST_KEY, new ByteRange(0, data.length)).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(KeyNotFoundException.class);
        }
    }

    @Test
    void testUploadWithEmptyBuffer() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            final ByteBuffer emptyBuffer = ByteBuffer.allocate(0);

            assertThatThrownBy(() -> storage.upload(TEST_KEY, emptyBuffer).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("byteBuffer must have remaining bytes");
        }
    }

    @Test
    void testUploadWithNullKey() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            final ByteBuffer buffer = ByteBuffer.wrap("test".getBytes());

            assertThatThrownBy(() -> storage.upload(null, buffer))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("key cannot be null");
        }
    }

    @Test
    void testUploadWithNullBuffer() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            assertThatThrownBy(() -> storage.upload(TEST_KEY, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("byteBuffer cannot be null");
        }
    }

    @Test
    void testFetchNonExistingKey() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            final ObjectKey nonExistingKey = new TestObjectKey("non-existing");

            assertThatThrownBy(() -> storage.fetch(nonExistingKey, new ByteRange(0, 10)).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(KeyNotFoundException.class);
        }
    }

    @Test
    void testFetchWithEmptyRange() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            final byte[] data = "test content".getBytes(StandardCharsets.UTF_8);
            storage.upload(TEST_KEY, ByteBuffer.wrap(data)).get();

            // Empty range should return empty buffer
            final ByteBuffer result = storage.fetch(TEST_KEY, new ByteRange(0, 0)).get();
            assertThat(result.remaining()).isEqualTo(0);
        }
    }

    @Test
    void testFetchWithNullRange() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            final byte[] data = "test content".getBytes(StandardCharsets.UTF_8);
            storage.upload(TEST_KEY, ByteBuffer.wrap(data)).get();

            // Null range should return full content
            final ByteBuffer result = storage.fetch(TEST_KEY, null).get();
            assertThat(readBuffer(result)).isEqualTo("test content");
        }
    }

    @Test
    void testFetchWithInvalidRange() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            final byte[] data = "ABC".getBytes(StandardCharsets.UTF_8);
            storage.upload(TEST_KEY, ByteBuffer.wrap(data)).get();

            // Range starting beyond file size
            assertThatThrownBy(() -> storage.fetch(TEST_KEY, new ByteRange(10, 5)).get())
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InvalidRangeException.class);
        }
    }

    @Test
    void testDeleteNonExistingKey() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            final ObjectKey nonExistingKey = new TestObjectKey("non-existing");

            // Delete should be idempotent - no error for non-existing keys
            storage.delete(nonExistingKey).get();
        }
    }

    @Test
    void testBatchDelete() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            final Set<ObjectKey> keys = IntStream.range(0, 5)
                .mapToObj(i -> new TestObjectKey("key-" + i))
                .collect(Collectors.toSet());

            // Upload all keys
            for (final ObjectKey key : keys) {
                storage.upload(key, ByteBuffer.wrap("content".getBytes())).get();
            }

            // Batch delete
            storage.delete(keys).get();

            // Verify all deleted
            for (final ObjectKey key : keys) {
                assertThatThrownBy(() -> storage.fetch(key, new ByteRange(0, 10)).get())
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(KeyNotFoundException.class);
            }
        }
    }

    @Test
    void testBatchDeleteEmptySet() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            // Empty set should complete successfully
            storage.delete(Set.of()).get();
        }
    }

    @Test
    void testOverwriteExistingKey() throws Exception {
        try (S3AsyncStorage storage = createStorage()) {
            final byte[] data1 = "content1".getBytes(StandardCharsets.UTF_8);
            final byte[] data2 = "content2".getBytes(StandardCharsets.UTF_8);

            // Upload first version
            storage.upload(TEST_KEY, ByteBuffer.wrap(data1)).get();

            // Overwrite with second version
            storage.upload(TEST_KEY, ByteBuffer.wrap(data2)).get();

            // Fetch should return the second version
            final ByteBuffer result = storage.fetch(TEST_KEY, new ByteRange(0, data2.length)).get();
            assertThat(readBuffer(result)).isEqualTo("content2");
        }
    }
}
