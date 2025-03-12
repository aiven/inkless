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

import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.NoCredentials;
import com.google.common.io.Resources;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CredentialsBuilderTest {
    @Test
    void allNull() {
        assertThatThrownBy(() -> CredentialsBuilder.build(null, null, null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("All defaultCredentials, credentialsJson, and credentialsPath cannot be null simultaneously.");
    }

    @ParameterizedTest
    @MethodSource("provideMoreThanOneNonNull")
    void moreThanOneNonNull(final Boolean defaultCredentials,
                            final String credentialsJson,
                            final String credentialsPath) {
        assertThatThrownBy(() -> CredentialsBuilder.build(defaultCredentials, credentialsJson, credentialsPath))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Only one of defaultCredentials, credentialsJson, and credentialsPath can be non-null.");
    }

    private static Stream<Arguments> provideMoreThanOneNonNull() {
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
    void defaultCredentials() throws IOException {
        final GoogleCredentials mockCredentials = GoogleCredentials.newBuilder().build();
        try (final MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
                 Mockito.mockStatic(GoogleCredentials.class)) {
            googleCredentialsMockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);
            final Credentials r = CredentialsBuilder.build(true, null, null);
            assertThat(r).isSameAs(mockCredentials);
        }
    }

    @Test
    void noCredentials() throws IOException {
        final Credentials r = CredentialsBuilder.build(false, null, null);
        assertThat(r).isSameAs(NoCredentials.getInstance());
    }

    @Test
    void fromJson() throws IOException {
        final String credentialsJson = Resources.toString(
            Thread.currentThread().getContextClassLoader().getResource("test_gcs_credentials.json"),
            StandardCharsets.UTF_8);
        final Credentials credentials = CredentialsBuilder.build(null, credentialsJson, null);
        assertCredentials(credentials);
    }

    @Test
    void fromPath() throws IOException {
        final String credentialsPath = Thread.currentThread()
            .getContextClassLoader()
            .getResource("test_gcs_credentials.json")
            .getPath();
        final Credentials credentials = CredentialsBuilder.build(null, null, credentialsPath);
        assertCredentials(credentials);
    }

    private static void assertCredentials(final Credentials credentials) {
        assertThat(credentials).isInstanceOf(UserCredentials.class);
        final UserCredentials userCredentials = (UserCredentials) credentials;
        assertThat(userCredentials.getClientId()).isEqualTo("test-client-id");
        assertThat(userCredentials.getClientSecret()).isEqualTo("test-client-secret");
        assertThat(userCredentials.getRefreshToken()).isEqualTo("x");
    }
}
