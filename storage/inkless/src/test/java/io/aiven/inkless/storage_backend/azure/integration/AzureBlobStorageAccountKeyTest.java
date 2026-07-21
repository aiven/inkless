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

import java.util.Map;

import io.aiven.inkless.storage_backend.azure.AzureBlobStorage;
import io.aiven.inkless.storage_backend.common.StorageBackend;

import static io.aiven.inkless.storage_backend.azure.integration.AzuriteBlobStorageUtils.endpoint;

class AzureBlobStorageAccountKeyTest extends AzureBlobStorageTest {
    @Override
    protected StorageBackend storage() {
        final AzureBlobStorage azureBlobStorage = new AzureBlobStorage();
        // The well-known Azurite account name and key.
        final String accountName = "devstoreaccount1";
        final String accountKey =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        final Map<String, Object> configs = Map.of(
            "azure.container.name", azureContainerName,
            "azure.account.name", accountName,
            "azure.account.key", accountKey,
            "azure.endpoint.url", endpoint(AZURITE_SERVER, BLOB_STORAGE_PORT)
        );
        azureBlobStorage.configure(configs);
        return azureBlobStorage;
    }
}
