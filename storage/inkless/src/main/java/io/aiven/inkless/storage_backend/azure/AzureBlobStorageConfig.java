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

package io.aiven.inkless.storage_backend.azure;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;

import io.aiven.inkless.common.config.validators.NonEmptyPassword;
import io.aiven.inkless.common.config.validators.Null;
import io.aiven.inkless.common.config.validators.ValidUrl;

public class AzureBlobStorageConfig extends AbstractConfig {
    static final String AZURE_ACCOUNT_NAME_CONFIG = "azure.account.name";
    private static final String AZURE_ACCOUNT_NAME_DOC = "Azure account name";

    static final String AZURE_ACCOUNT_KEY_CONFIG = "azure.account.key";
    private static final String AZURE_ACCOUNT_KEY_DOC = "Azure account key";

    static final String AZURE_SAS_TOKEN_CONFIG = "azure.sas.token";
    private static final String AZURE_SAS_TOKEN_DOC = "Azure SAS token";

    static final String AZURE_CONTAINER_NAME_CONFIG = "azure.container.name";
    private static final String AZURE_CONTAINER_NAME_DOC = "Azure container to store log segments";

    static final String AZURE_ENDPOINT_URL_CONFIG = "azure.endpoint.url";
    private static final String AZURE_ENDPOINT_URL_DOC = "Custom Azure Blob Storage endpoint URL";

    static final String AZURE_CONNECTION_STRING_CONFIG = "azure.connection.string";
    private static final String AZURE_CONNECTION_STRING_DOC = "Azure connection string. "
        + "Cannot be used together with azure.account.name, azure.account.key, and azure.endpoint.url";

    static final String AZURE_UPLOAD_BLOCK_SIZE_CONFIG = "azure.upload.block.size";
    private static final String AZURE_UPLOAD_BLOCK_SIZE_DOC = "Size of blocks to use when uploading objects to Azure";
    static final int AZURE_UPLOAD_BLOCK_SIZE_DEFAULT = 5 * 1024 * 1024; // 5MiB
    static final int AZURE_UPLOAD_BLOCK_SIZE_MIN = 100 * 1024;
    static final int AZURE_UPLOAD_BLOCK_SIZE_MAX = Integer.MAX_VALUE;

    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(
                AZURE_ACCOUNT_NAME_CONFIG,
                ConfigDef.Type.STRING,
                null,
                Null.or(new ConfigDef.NonEmptyString()),
                ConfigDef.Importance.HIGH,
                AZURE_ACCOUNT_NAME_DOC)
            .define(
                AZURE_ACCOUNT_KEY_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                Null.or(new NonEmptyPassword()),
                ConfigDef.Importance.MEDIUM,
                AZURE_ACCOUNT_KEY_DOC)
            .define(
                AZURE_SAS_TOKEN_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                Null.or(new NonEmptyPassword()),
                ConfigDef.Importance.MEDIUM,
                AZURE_SAS_TOKEN_DOC)
            .define(
                AZURE_CONTAINER_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                AZURE_CONTAINER_NAME_DOC)
            .define(
                AZURE_ENDPOINT_URL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                Null.or(new ValidUrl()),
                ConfigDef.Importance.LOW,
                AZURE_ENDPOINT_URL_DOC)
            .define(
                AZURE_CONNECTION_STRING_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                Null.or(new NonEmptyPassword()),
                ConfigDef.Importance.MEDIUM,
                AZURE_CONNECTION_STRING_DOC)
            .define(
                AZURE_UPLOAD_BLOCK_SIZE_CONFIG,
                ConfigDef.Type.INT,
                AZURE_UPLOAD_BLOCK_SIZE_DEFAULT,
                ConfigDef.Range.between(AZURE_UPLOAD_BLOCK_SIZE_MIN, AZURE_UPLOAD_BLOCK_SIZE_MAX),
                ConfigDef.Importance.MEDIUM,
                AZURE_UPLOAD_BLOCK_SIZE_DOC);
    }

    public AzureBlobStorageConfig(final Map<String, ?> props) {
        super(configDef(), props);
        validate(props);
    }

    private void validate(final Map<String, ?> props) {
        if (props.get(AZURE_CONNECTION_STRING_CONFIG) != null) {
            if (props.get(AZURE_ACCOUNT_NAME_CONFIG) != null) {
                throw new ConfigException(
                    "\"azure.connection.string\" cannot be set together with \"azure.account.name\".");
            }
            if (props.get(AZURE_ACCOUNT_KEY_CONFIG) != null) {
                throw new ConfigException(
                    "\"azure.connection.string\" cannot be set together with \"azure.account.key\".");
            }
            if (props.get(AZURE_SAS_TOKEN_CONFIG) != null) {
                throw new ConfigException(
                    "\"azure.connection.string\" cannot be set together with \"azure.sas.token\".");
            }
            if (props.get(AZURE_ENDPOINT_URL_CONFIG) != null) {
                throw new ConfigException(
                    "\"azure.connection.string\" cannot be set together with \"azure.endpoint.url\".");
            }
        } else {
            if (props.get(AZURE_ACCOUNT_NAME_CONFIG) == null && props.get(AZURE_SAS_TOKEN_CONFIG) == null) {
                throw new ConfigException(
                    "\"azure.account.name\" and/or \"azure.sas.token\" "
                        + "must be set if \"azure.connection.string\" is not set.");
            }
        }
    }

    String accountName() {
        return getString(AZURE_ACCOUNT_NAME_CONFIG);
    }

    String accountKey() {
        final Password key = getPassword(AZURE_ACCOUNT_KEY_CONFIG);
        return key == null ? null : key.value();
    }

    String sasToken() {
        final Password key = getPassword(AZURE_SAS_TOKEN_CONFIG);
        return key == null ? null : key.value();
    }

    String containerName() {
        return getString(AZURE_CONTAINER_NAME_CONFIG);
    }

    String endpointUrl() {
        return getString(AZURE_ENDPOINT_URL_CONFIG);
    }

    String connectionString() {
        final Password connectionString = getPassword(AZURE_CONNECTION_STRING_CONFIG);
        return connectionString == null ? null : connectionString.value();
    }

    int uploadBlockSize() {
        return getInt(AZURE_UPLOAD_BLOCK_SIZE_CONFIG);
    }
}
