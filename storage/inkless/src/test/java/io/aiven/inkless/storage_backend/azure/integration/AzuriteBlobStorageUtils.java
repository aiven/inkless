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

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class AzuriteBlobStorageUtils {
    static GenericContainer<?> azuriteContainer(final int port) {
        return
            new GenericContainer<>(DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite"))
                .withExposedPorts(port)
                .withCommand("azurite-blob --blobHost 0.0.0.0");
    }


    static String endpoint(final GenericContainer<?> azuriteContainer, final int port) {
        return "http://127.0.0.1:" + azuriteContainer.getMappedPort(port) + "/devstoreaccount1";
    }

    static String connectionString(final GenericContainer<?> azuriteContainer, final int port) {
        // The well-known Azurite connection string.
        return "DefaultEndpointsProtocol=http;"
            + "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            + "BlobEndpoint=" + endpoint(azuriteContainer, port) + ";";
    }
}
