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

package io.aiven.inkless.storage_backend.gcs;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import java.io.IOException;
import java.util.Map;

import io.aiven.inkless.common.config.validators.NonEmptyPassword;
import io.aiven.inkless.common.config.validators.ValidUrl;

public class GcsStorageConfig extends AbstractConfig {
    static final String GCS_BUCKET_NAME_CONFIG = "gcs.bucket.name";
    private static final String GCS_BUCKET_NAME_DOC = "GCS bucket to store log segments";

    static final String GCS_ENDPOINT_URL_CONFIG = "gcs.endpoint.url";
    private static final String GCS_ENDPOINT_URL_DOC = "Custom GCS endpoint URL. "
        + "To be used with custom GCS-compatible backends.";

    static final String GCP_CREDENTIALS_JSON_CONFIG = "gcs.credentials.json";
    static final String GCP_CREDENTIALS_PATH_CONFIG = "gcs.credentials.path";
    static final String GCP_CREDENTIALS_DEFAULT_CONFIG = "gcs.credentials.default";

    private static final String GCP_CREDENTIALS_JSON_DOC = "GCP credentials as a JSON string. "
        + "Cannot be set together with \"" + GCP_CREDENTIALS_PATH_CONFIG + "\" "
        + "or \"" + GCP_CREDENTIALS_DEFAULT_CONFIG + "\"";
    private static final String GCP_CREDENTIALS_PATH_DOC = "The path to a GCP credentials file. "
        + "This can be standard GCP credentials format, or JSON with a single `access_token` field. "
        + "Cannot be set together with \"" + GCP_CREDENTIALS_JSON_CONFIG + "\" "
        + "or \"" + GCP_CREDENTIALS_DEFAULT_CONFIG + "\"";
    private static final String GCP_CREDENTIALS_DEFAULT_DOC = "Use the default GCP credentials. "
        + "Cannot be set together with \"" + GCP_CREDENTIALS_JSON_CONFIG + "\" "
        + "or \"" + GCP_CREDENTIALS_PATH_CONFIG + "\"";

    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(
                GCS_BUCKET_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                GCS_BUCKET_NAME_DOC)
            .define(
                GCS_ENDPOINT_URL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ValidUrl(),
                ConfigDef.Importance.LOW,
                GCS_ENDPOINT_URL_DOC)
            .define(
                GCP_CREDENTIALS_JSON_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                GCP_CREDENTIALS_JSON_DOC)
            .define(
                GCP_CREDENTIALS_PATH_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                GCP_CREDENTIALS_PATH_DOC)
            .define(
                GCP_CREDENTIALS_DEFAULT_CONFIG,
                ConfigDef.Type.BOOLEAN,
                null,
                ConfigDef.Importance.MEDIUM,
                GCP_CREDENTIALS_DEFAULT_DOC);
    }

    public GcsStorageConfig(final Map<String, ?> props) {
        super(configDef(), props);
        validate(props);
    }

    private void validate(final Map<String, ?> props) {
        try {
            CredentialsBuilder.validate(
                props.get(GCP_CREDENTIALS_DEFAULT_CONFIG),
                props.get(GCP_CREDENTIALS_JSON_CONFIG),
                props.get(GCP_CREDENTIALS_PATH_CONFIG)
            );
        } catch (final IllegalArgumentException e) {
            final String message = e.getMessage()
                .replace("credentialsPath", GCP_CREDENTIALS_PATH_CONFIG)
                .replace("credentialsJson", GCP_CREDENTIALS_JSON_CONFIG)
                .replace("defaultCredentials", GCP_CREDENTIALS_DEFAULT_CONFIG);
            throw new ConfigException(message);
        }
    }

    String bucketName() {
        return getString(GCS_BUCKET_NAME_CONFIG);
    }

    String endpointUrl() {
        return getString(GCS_ENDPOINT_URL_CONFIG);
    }

    /**
     * Creates a reloadable credentials provider that automatically reloads credentials
     * when the credentials file is modified during runtime (if using file-based credentials).
     *
     * @return a ReloadableCredentialsProvider instance
     * @throws ConfigException if credentials cannot be created
     */
    ReloadableCredentialsProvider reloadableCredentials() {
        final Boolean defaultCredentials = getBoolean(GCP_CREDENTIALS_DEFAULT_CONFIG);
        final Password credentialsJsonPwd = getPassword(GCP_CREDENTIALS_JSON_CONFIG);
        final String credentialsJson = credentialsJsonPwd == null ? null : credentialsJsonPwd.value();
        final String credentialsPath = getString(GCP_CREDENTIALS_PATH_CONFIG);

        try {
            final var credentialsProvider = new ReloadableCredentialsProvider(defaultCredentials, credentialsJson, credentialsPath);
            credentialsProvider.enableWatching();
            return credentialsProvider;
        } catch (final IOException e) {
            throw new ConfigException("Failed to create GCS credentials: " + e.getMessage());
        }
    }
}
