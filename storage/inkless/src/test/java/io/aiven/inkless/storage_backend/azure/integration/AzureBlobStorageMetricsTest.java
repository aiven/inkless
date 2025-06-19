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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.stream.Stream;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.azure.AzureBlobStorage;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import io.aiven.inkless.storage_backend.common.fixtures.TestObjectKey;
import io.aiven.inkless.storage_backend.common.fixtures.TestUtils;

import static io.aiven.inkless.storage_backend.azure.integration.AzuriteBlobStorageUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

@Testcontainers
public class AzureBlobStorageMetricsTest {
    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final int UPLOAD_BLOCK_SIZE = 256 * 1024;
    private static final int BLOB_STORAGE_PORT = 10000;
    @Container
    static final GenericContainer<?> AZURITE_SERVER = azuriteContainer(BLOB_STORAGE_PORT);

    static BlobServiceClient blobServiceClient;
    static AzureBlobStorage storage;

    protected String azureContainerName;

    @BeforeAll
    static void setUpClass() {
        blobServiceClient = new BlobServiceClientBuilder()
            .connectionString(connectionString(AZURITE_SERVER, BLOB_STORAGE_PORT))
            .buildClient();

        storage = new AzureBlobStorage();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        azureContainerName = TestUtils.testNameToBucketName(testInfo);
        blobServiceClient.createBlobContainer(azureContainerName);

        // The well-known Azurite account name and key.
        final String accountName = "devstoreaccount1";
        final String accountKey =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        final Map<String, Object> configs = Map.of(
            "azure.container.name", azureContainerName,
            "azure.account.name", accountName,
            "azure.account.key", accountKey,
            "azure.endpoint.url", endpoint(AZURITE_SERVER, BLOB_STORAGE_PORT),
            "azure.upload.block.size", UPLOAD_BLOCK_SIZE
        );
        storage.configure(configs);
    }

    @AfterEach
    void tearDown() throws Exception {
        storage.close();
    }

    static Stream<Arguments> metricsShouldBeReported() {
        // The metrics are cumulative, so the expected values are based on previous test cases.
        return Stream.of(
            Arguments.of(
                Named.of("smaller-than-block-size-payload", UPLOAD_BLOCK_SIZE - 1),
                1,
                0,
                0,
                2,
                1
            ),
            Arguments.of(
                Named.of("larger-than-block-size-payload", UPLOAD_BLOCK_SIZE + 1),
                0, // from previous test case
                2,
                1,
                2,
                1
            )
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource
    void metricsShouldBeReported(
        final int uploadBlockSize,
        final double expectedPutBlob,
        final double expectedPutBlock,
        final double expectedPutBlockList,
        final double expectedGetBlob,
        final double expectedDeleteBlob
    ) throws StorageBackendException, IOException, JMException {
        final byte[] data = new byte[uploadBlockSize];

        final ObjectKey key = new TestObjectKey("test-object-key");

        storage.upload(key, new ByteArrayInputStream(data), data.length);
        storage.fetch(key, null);
        storage.fetch(key, new ByteRange(0, 1));
        storage.delete(key);

        final ObjectName objectName =
            ObjectName.getInstance("io.aiven.inkless.storage.azure:type=azure-blob-storage-client-metrics");
        assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-get-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-get-total"))
            .asInstanceOf(DOUBLE)
            .isEqualTo(expectedGetBlob);

        if (expectedPutBlob > 0) {
            assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-upload-rate"))
                .asInstanceOf(DOUBLE)
                .isGreaterThan(0.0);
        }
        assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-upload-total"))
            .isEqualTo(expectedPutBlob);

        if (expectedPutBlock > 0) {
            assertThat(MBEAN_SERVER.getAttribute(objectName, "block-upload-rate"))
                .asInstanceOf(DOUBLE)
                .isGreaterThan(0.0);
        }
        assertThat(MBEAN_SERVER.getAttribute(objectName, "block-upload-total"))
            .isEqualTo(expectedPutBlock);

        if (expectedPutBlockList > 0) {
            assertThat(MBEAN_SERVER.getAttribute(objectName, "block-list-upload-rate"))
                .asInstanceOf(DOUBLE)
                .isGreaterThan(0.0);
        }
        assertThat(MBEAN_SERVER.getAttribute(objectName, "block-list-upload-total"))
            .isEqualTo(expectedPutBlockList);

        assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-delete-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-delete-total"))
            .asInstanceOf(DOUBLE)
            .isEqualTo(expectedDeleteBlob);
    }
}
