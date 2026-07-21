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

import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;

import java.time.OffsetDateTime;
import java.util.Map;

import io.aiven.inkless.storage_backend.azure.AzureBlobStorage;
import io.aiven.inkless.storage_backend.common.StorageBackend;

import static io.aiven.inkless.storage_backend.azure.integration.AzuriteBlobStorageUtils.endpoint;


class AzureBlobStorageSasTokenTest extends AzureBlobStorageTest {
    @Override
    protected StorageBackend storage() {
        final var permissions = new BlobContainerSasPermission()
            .setCreatePermission(true)
            .setDeletePermission(true)
            .setReadPermission(true)
            .setWritePermission(true);
        final var sasSignatureValues =
            new BlobServiceSasSignatureValues(OffsetDateTime.now().plusDays(1), permissions)
                .setStartTime(OffsetDateTime.now().minusMinutes(5));
        final String sasToken = blobServiceClient.getBlobContainerClient(azureContainerName)
            .generateSas(sasSignatureValues);

        final AzureBlobStorage azureBlobStorage = new AzureBlobStorage();
        final Map<String, Object> configs = Map.of(
            "azure.container.name", azureContainerName,
            "azure.sas.token", sasToken,
            "azure.endpoint.url", endpoint(AZURITE_SERVER, BLOB_STORAGE_PORT)
        );
        azureBlobStorage.configure(configs);
        return azureBlobStorage;
    }
}
