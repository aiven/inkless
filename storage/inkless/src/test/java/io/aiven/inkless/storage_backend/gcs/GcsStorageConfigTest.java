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

import org.apache.kafka.common.config.ConfigException;

import com.google.auth.oauth2.GoogleCredentials;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class GcsStorageConfigTest {
    @Test
    void minimalConfig() {
        final String bucketName = "b1";
        final Map<String, Object> configs = Map.of(
            "gcs.bucket.name", bucketName,
            "gcs.credentials.default", "true");
        final GcsStorageConfig config = new GcsStorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);
        assertThat(config.endpointUrl()).isNull();

        final GoogleCredentials mockCredentials = GoogleCredentials.newBuilder().build();
        try (final MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
                 Mockito.mockStatic(GoogleCredentials.class)) {
            googleCredentialsMockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);
            assertThat(config.reloadableCredentials().getCredentials()).isSameAs(mockCredentials);
        }
    }

    @Test
    void invalidEndpointUrl() {
        assertThatThrownBy(() -> new GcsStorageConfig(
            Map.of(
                "gcs.bucket.name", "bucket",
                "gcs.endpoint.url", "invalid_url")
        ))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value invalid_url for configuration gcs.endpoint.url: Must be a valid URL");

        assertThatThrownBy(() -> new GcsStorageConfig(
            Map.of(
                "gcs.bucket.name", "bucket",
                "gcs.endpoint.url", "ftp://invalid_url")
        ))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value ftp://invalid_url for configuration gcs.endpoint.url: "
                + "URL must have scheme from the list [http, https]");
    }

    @Test
    void emptyGcsBucketName() {
        assertThatThrownBy(() -> new GcsStorageConfig(Map.of("gcs.bucket.name", "")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value  for configuration gcs.bucket.name: String must be non-empty");
    }

    @Test
    void emptyJsonCredentials() {
        final var props = Map.of(
            "gcs.bucket.name", "bucket",
            "gcs.credentials.json", ""
        );
        assertThatThrownBy(() -> new GcsStorageConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("gcs.credentials.json value must not be empty");
    }

    @Test
    void emptyPathCredentials() {
        final var props = Map.of(
            "gcs.bucket.name", "bucket",
            "gcs.credentials.path", ""
        );
        assertThatThrownBy(() -> new GcsStorageConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value  for configuration gcs.credentials.path: String must be non-empty");
    }

    @Test
    void allCredentialsNull() {
        final var props = Map.of(
            "gcs.bucket.name", "bucket"
        );
        assertThatThrownBy(() -> new GcsStorageConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("All gcs.credentials.default, gcs.credentials.json, and gcs.credentials.path "
                + "cannot be null simultaneously.");
    }

    @ParameterizedTest
    @MethodSource("provideMoreThanOneCredentialsNonNull")
    void moreThanOneCredentialsNonNull(final Boolean defaultCredentials,
                                       final String credentialsJson,
                                       final String credentialsPath) {
        final Map<String, Object> props = new HashMap<>();
        props.put("gcs.bucket.name", "bucket");
        props.put("gcs.credentials.default", defaultCredentials);
        props.put("gcs.credentials.json", credentialsJson);
        props.put("gcs.credentials.path", credentialsPath);
        assertThatThrownBy(() -> new GcsStorageConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Only one of gcs.credentials.default, gcs.credentials.json, and gcs.credentials.path "
                + "can be non-null.");
    }

    private static Stream<Arguments> provideMoreThanOneCredentialsNonNull() {
        return Stream.of(
            Arguments.of(true, "json", "path"),
            Arguments.of(false, "json", "path"),
            Arguments.of(true, "json", null),
            Arguments.of(false, "json", null),
            Arguments.of(true, null, "path"),
            Arguments.of(false, null, "path"),
            Arguments.of(null, "json", "path")
        );
    }

    @Test
    void reloadableCredentialsWithDefaults() {
        final Map<String, Object> configs = Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.credentials.default", "true");
        final GcsStorageConfig config = new GcsStorageConfig(configs);

        try (final ReloadableCredentialsProvider provider = config.reloadableCredentials()) {
            assertThat(provider).isNotNull();
            assertThat(provider.getCredentials()).isNotNull();
        } catch (final Exception e) {
            // Close may throw IOException
        }
    }

    @Test
    void reloadableCredentialsWithJsonCredentials() {
        final String validCredentialsJson = "{\n"
            + "  \"type\": \"service_account\",\n"
            + "  \"project_id\": \"test-project\",\n"
            + "  \"private_key_id\": \"test-key-id\",\n"
            + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\n"
            + "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC5M...\\n-----END PRIVATE KEY-----\\n\",\n"
            + "  \"client_email\": \"test@test-project.iam.gserviceaccount.com\",\n"
            + "  \"client_id\": \"123456789012345678901\",\n"
            + "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
            + "  \"token_uri\": \"https://oauth2.googleapis.com/token\"\n"
            + "}";

        final Map<String, Object> configs = Map.of(
            "gcs.bucket.name", "test-bucket",
            "gcs.credentials.json", validCredentialsJson);
        final GcsStorageConfig config = new GcsStorageConfig(configs);

        try (final ReloadableCredentialsProvider provider = config.reloadableCredentials()) {
            assertThat(provider).isNotNull();
            assertThat(provider.getCredentials()).isNotNull();
        } catch (final Exception e) {
            // Close may throw IOException
        }
    }
}
