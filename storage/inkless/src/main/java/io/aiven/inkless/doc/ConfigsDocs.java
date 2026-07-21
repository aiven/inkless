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

package io.aiven.inkless.doc;

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.InMemoryControlPlaneConfig;
import io.aiven.inkless.control_plane.postgres.PostgresConnectionConfig;
import io.aiven.inkless.control_plane.postgres.PostgresControlPlaneConfig;
import io.aiven.inkless.storage_backend.azure.AzureBlobStorageConfig;
import io.aiven.inkless.storage_backend.gcs.GcsStorageConfig;
import io.aiven.inkless.storage_backend.s3.S3StorageConfig;

import static java.lang.System.out;

/**
 * Gather all config definitions across the project and generate a documentation page
 **/
public class ConfigsDocs {
    public static void main(final String[] args) {
        printSectionTitle("Diskless Configs");
        out.println(".. Generated from *Config.java classes by " + ConfigsDocs.class.getCanonicalName());
        out.println();

        printSubsectionTitle("InklessConfig");
        out.println("Under ``" + InklessConfig.PREFIX + "``\n");
        final var inklessConfigDef = InklessConfig.configDef();
        out.println(inklessConfigDef.toEnrichedRst());
        out.println();

        printSubsectionTitle("InMemoryControlPlaneConfig");
        out.println("Under ``" + InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + "``\n");
        final var inMemoryControlPlaneConfigDef = InMemoryControlPlaneConfig.configDef();
        out.println(inMemoryControlPlaneConfigDef.toEnrichedRst());
        out.println();

        printSubsectionTitle("PostgresControlPlaneConfig");
        out.println("Under ``" + InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + "``\n");
        final var postgresControlPlaneConfig = PostgresControlPlaneConfig.configDef();
        out.println(postgresControlPlaneConfig.toEnrichedRst());
        out.println();
        printSubsectionTitle("PostgresControlPlaneConfig - read overrides");
        out.println("Under ``" + InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.READ_CONFIG_PREFIX + "``\n");
        out.println(PostgresConnectionConfig.configDef().toEnrichedRst());
        out.println();
        printSubsectionTitle("PostgresControlPlaneConfig - write overrides");
        out.println("Under ``" + InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.WRITE_CONFIG_PREFIX + "``\n");
        out.println(PostgresConnectionConfig.configDef().toEnrichedRst());
        out.println();

        printSubsectionTitle("AzureBlobStorageConfig");
        out.println("Under ``" + InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + "``\n");
        final var azureBlobStorageConfig = AzureBlobStorageConfig.configDef();
        out.println(azureBlobStorageConfig.toEnrichedRst());
        out.println();

        printSubsectionTitle("GcsStorageConfig");
        out.println("Under ``" + InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX  + "``\n");
        final var gcsStorageConfig = GcsStorageConfig.configDef();
        out.println(gcsStorageConfig.toEnrichedRst());
        out.println();

        printSubsectionTitle("S3StorageConfig");
        out.println("Under ``" + InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX  + "``\n");
        final var s3StorageConfig = S3StorageConfig.configDef();
        out.println(s3StorageConfig.toEnrichedRst());
        out.println();
    }

    static void printSectionTitle(final String title) {
        out.println("=================\n"
                + title + "\n"
                + "=================");
    }

    static void printSubsectionTitle(final String title) {
        out.println("-----------------\n"
                + title + "\n"
                + "-----------------");
    }
}