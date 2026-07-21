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
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.fixtures.BaseStorageTest;
import io.aiven.inkless.storage_backend.common.fixtures.TestUtils;
import io.aiven.inkless.storage_backend.gcs.GcsStorage;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

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
