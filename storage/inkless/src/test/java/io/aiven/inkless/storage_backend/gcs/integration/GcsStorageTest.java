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

import org.apache.kafka.common.metrics.Metrics;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.KeyNotFoundException;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.fixtures.BaseStorageTest;
import io.aiven.inkless.storage_backend.common.fixtures.TestObjectKey;
import io.aiven.inkless.storage_backend.common.fixtures.TestUtils;
import io.aiven.inkless.storage_backend.gcs.GcsStorage;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
class GcsStorageTest extends BaseStorageTest {
    @Container
    static final FakeGcsServerContainer GCS_SERVER = new FakeGcsServerContainer();

    static Storage storage;
    private String bucketName;

    @BeforeAll
    static void setUpClass() {
        storage = StorageOptions.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .setHost(GCS_SERVER.url())
            .setProjectId("test-project")
            .build()
            .getService();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        bucketName = TestUtils.testNameToBucketName(testInfo);
        storage.create(BucketInfo.newBuilder(bucketName).build());
    }

    @Test
    void deletesSpanningMultipleBatches() throws Exception {
        // More than one batch, with a partial last one, so the chunking loop's bounds and the
        // accumulation of confirmed keys across batches are both exercised.
        try (StorageBackend storage = storage()) {
            final Set<ObjectKey> keys = IntStream.range(0, 101)
                .mapToObj(i -> (ObjectKey) new TestObjectKey("batched-" + i))
                .collect(Collectors.toSet());
            final byte[] data = "test".getBytes();
            for (final ObjectKey key : keys) {
                storage.upload(key, new ByteArrayInputStream(data), data.length);
            }

            assertThat(storage.delete(keys)).containsExactlyInAnyOrderElementsOf(keys);

            for (final ObjectKey key : keys) {
                assertThatThrownBy(() -> storage.fetch(key, ByteRange.maxRange()))
                    .isInstanceOf(KeyNotFoundException.class);
            }
        }
    }

    @Override
    protected StorageBackend storage() {
        final Metrics metrics = new Metrics();
        final GcsStorage gcsStorage = new GcsStorage(metrics);
        final Map<String, Object> configs = Map.of(
            "gcs.bucket.name", bucketName,
            "gcs.endpoint.url", GCS_SERVER.url(),
            "gcs.credentials.default", "false"
        );
        gcsStorage.configure(configs);
        return gcsStorage;
    }
}
