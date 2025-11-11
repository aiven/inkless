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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.stream.Stream;

final class CredentialsBuilder {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CredentialsBuilder() {
        // hide constructor
    }

    public static void validate(final Object defaultCredentials,
                                final Object credentialsJson,
                                final Object credentialsPath) {
        final long nonNulls = Stream.of(defaultCredentials, credentialsJson, credentialsPath)
            .filter(Objects::nonNull).count();
        if (nonNulls == 0) {
            throw new IllegalArgumentException(
                "All defaultCredentials, credentialsJson, and credentialsPath cannot be null simultaneously.");
        }
        if (nonNulls > 1) {
            throw new IllegalArgumentException(
                "Only one of defaultCredentials, credentialsJson, and credentialsPath can be non-null.");
        }
    }

    /**
     * Builds {@link GoogleCredentials}.
     *
     * <p>{@code credentialsPath}, {@code credentialsJson}, and {@code defaultCredentials true}
     * are mutually exclusive: if more than one is provided (are non-{@code null}), this is an error.
     *
     * <p>If either {@code credentialsPath} or {@code credentialsJson} is provided, it's used to
     * construct the credentials.
     *
     * <p>If none are provided, the default GCP SDK credentials acquisition mechanism is used
     * or the no-credentials object is returned.
     *
     * @param defaultCredentials use the default credentials.
     * @param credentialsJson    the credential JSON string, can be {@code null}.
     * @param credentialsPath    the credential path, can be {@code null}.
     * @return a {@link GoogleCredentials} constructed based on the input.
     * @throws IOException              if some error getting the credentials happen.
     * @throws IllegalArgumentException if a combination of parameters is invalid.
     */
    public static Credentials build(final Boolean defaultCredentials,
                                    final String credentialsJson,
                                    final String credentialsPath)
        throws IOException {
        validate(defaultCredentials, credentialsJson, credentialsPath);

        if (credentialsJson != null) {
            return getCredentialsFromJson(credentialsJson);
        }

        if (credentialsPath != null) {
            return getCredentialsFromPath(credentialsPath);
        }

        if (Boolean.TRUE.equals(defaultCredentials)) {
            return GoogleCredentials.getApplicationDefault();
        } else {
            return NoCredentials.getInstance();
        }
    }

    private static GoogleCredentials getCredentialsFromBytes(final byte[] credentialsBytes) throws IOException {
        final JsonNode jsonNode = MAPPER.readTree(credentialsBytes);

        // Google credentials always have a "type" field. If it's not there, use custom parsing for an access token.
        if (jsonNode.isObject() && !jsonNode.has("type")
                && jsonNode.has("access_token")) {
            final AccessToken accessToken = AccessToken.newBuilder().setTokenValue(
                    jsonNode.get("access_token").asText()).build();
            return GoogleCredentials.create(accessToken);
        }

        try (final InputStream byteStream = new ByteArrayInputStream(credentialsBytes)) {
            return GoogleCredentials.fromStream(byteStream);
        }
    }

    private static GoogleCredentials getCredentialsFromPath(final String credentialsPath) throws IOException {
        try (final InputStream stream = Files.newInputStream(Paths.get(credentialsPath))) {
            return getCredentialsFromBytes(stream.readAllBytes());
        } catch (final IOException e) {
            throw new IOException("Failed to read GCS credentials from " + credentialsPath, e);
        }
    }

    private static GoogleCredentials getCredentialsFromJson(final String credentialsJson) throws IOException {
        try {
            return getCredentialsFromBytes(credentialsJson.getBytes(StandardCharsets.UTF_8));
        } catch (final IOException e) {
            throw new IOException("Failed to read credentials from JSON string", e);
        }
    }
}
