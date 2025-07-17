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

package io.aiven.inkless.storage_backend.azure.integration;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayInputStream;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.storage_backend.common.InvalidRangeException;
import io.aiven.inkless.storage_backend.common.ObjectUploader;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.fixtures.BaseStorageTest;
import io.aiven.inkless.storage_backend.common.fixtures.TestUtils;

import static io.aiven.inkless.storage_backend.azure.integration.AzuriteBlobStorageUtils.azuriteContainer;
import static io.aiven.inkless.storage_backend.azure.integration.AzuriteBlobStorageUtils.connectionString;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
abstract class AzureBlobStorageTest extends BaseStorageTest {
    static final int BLOB_STORAGE_PORT = 10000;
    @Container
    static final GenericContainer<?> AZURITE_SERVER = azuriteContainer(BLOB_STORAGE_PORT);

    static BlobServiceClient blobServiceClient;

    protected String azureContainerName;

    @BeforeAll
    static void setUpClass() {
        blobServiceClient = new BlobServiceClientBuilder()
            .connectionString(connectionString(AZURITE_SERVER, BLOB_STORAGE_PORT))
            .buildClient();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        azureContainerName = TestUtils.testNameToBucketName(testInfo);
        blobServiceClient.createBlobContainer(azureContainerName);
    }

    @Override
    protected void testFetchWithRangeOutsideFileSize() throws StorageBackendException {
        // For some reason, Azure (or only Azurite) considers range 3-5 valid for a 3-byte blob,
        // so the generic test fails.
        final String content = "ABC";
        ObjectUploader objectUploader = storage();
        byte[] data = content.getBytes();
        objectUploader.upload(TOPIC_PARTITION_SEGMENT_KEY, new ByteArrayInputStream(data), data.length);

        assertThatThrownBy(() -> storage().fetch(TOPIC_PARTITION_SEGMENT_KEY, new ByteRange(4, 6)))
            .isInstanceOf(InvalidRangeException.class);
    }
}
