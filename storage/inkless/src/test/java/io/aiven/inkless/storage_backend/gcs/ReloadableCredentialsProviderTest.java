/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.NoCredentials;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class ReloadableCredentialsProviderTest {

    private static final String VALID_CREDENTIALS_JSON = "{\n"
        + "  \"type\": \"service_account\",\n"
        + "  \"project_id\": \"test-project\",\n"
        + "  \"private_key_id\": \"test-key-id\",\n"
        + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\n"
        + "MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAkEAwyPCXjWv30y+ZGJH\\njKGsIem4OlXEwsgsl6bJr0vKga/GYEVZXsKz/1Uv"
        + "KArCQNLOfJh/CpUE+cSLn+H7\\ngZ1uSwIDAQABAkAF1H2sHuKAQ2S0zxLgKrxfzwHIDGPyhdR/O2ZvLE6CjVZ0J4PD\\n+Gt3nJQUcELCEj"
        + "c3y3RnlOsGd7TTPsZHP7CRAiEA8f75YoDbDcPpd6SK4/PoWmTD\\nBBprsvsQbWL5Vpx0AH8CIQDObqMNKTCtz64tDULI0JSECu7RniRFyQ"
        + "CQ6H/ZMLys\\nNQIgM68eOjCFGGqIOXpWA5t7O5sbn4u5Bs/iUUp7MElX6ScCIHJBOAvDvYamCOA0\\nk78z+s9ugaoRXkAltSN/G6vpVrP1"
        + "AiBhNDs+MZSYh92/A8j/GC/I8yvlkOSFo/ME\\n/Va0X/P2Ng==\\n-----END PRIVATE KEY-----\\n\",\n"
        + "  \"client_email\": \"test@test-project.iam.gserviceaccount.com\",\n"
        + "  \"client_id\": \"123456789012345678901\",\n"
        + "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
        + "  \"token_uri\": \"https://oauth2.googleapis.com/token\"\n"
        + "}";

    private static final String UPDATED_CREDENTIALS_JSON = "{\n"
        + "  \"type\": \"service_account\",\n"
        + "  \"project_id\": \"updated-project\",\n"
        + "  \"private_key_id\": \"updated-key-id\",\n"
        + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\n"
        + "MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAkEAwyPCXjWv30y+ZGJH\\njKGsIem4OlXEwsgsl6bJr0vKga/GYEVZXsKz/1Uv"
        + "KArCQNLOfJh/CpUE+cSLn+H7\\ngZ1uSwIDAQABAkAF1H2sHuKAQ2S0zxLgKrxfzwHIDGPyhdR/O2ZvLE6CjVZ0J4PD\\n+Gt3nJQUcELCEj"
        + "c3y3RnlOsGd7TTPsZHP7CRAiEA8f75YoDbDcPpd6SK4/PoWmTD\\nBBprsvsQbWL5Vpx0AH8CIQDObqMNKTCtz64tDULI0JSECu7RniRFyQ"
        + "CQ6H/ZMLys\\nNQIgM68eOjCFGGqIOXpWA5t7O5sbn4u5Bs/iUUp7MElX6ScCIHJBOAvDvYamCOA0\\nk78z+s9ugaoRXkAltSN/G6vpVrP1"
        + "AiBhNDs+MZSYh92/A8j/GC/I8yvlkOSFo/ME\\n/Va0X/P2Ng==\\n-----END PRIVATE KEY-----\\n\",\n"
        + "  \"client_email\": \"updated@updated-project.iam.gserviceaccount.com\",\n"
        + "  \"client_id\": \"123456789012345678902\",\n"
        + "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
        + "  \"token_uri\": \"https://oauth2.googleapis.com/token\"\n"
        + "}";

    private static final String ACCESS_TOKEN_CREDENTIALS_JSON = "{\n"
        + "  \"access_token\": \"ya29...token1\"\n"
        + "}";

    private static final String ACCESS_TOKEN_CREDENTIALS2_JSON = "{\n"
        + "  \"access_token\": \"ya29...token2\"\n"
        + "}";

    private static final String ACCESS_TOKEN_CREDENTIALS3_JSON = "{\n"
        + "  \"access_token\": \"ya29...token3\"\n"
        + "}";


    @Test
    void testJsonCredentials() throws IOException {
        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, VALID_CREDENTIALS_JSON, null)) {

            final Credentials credentials = provider.getCredentials();
            assertThat(credentials).isNotNull();
        }
    }

    @Test
    void testNoCredentials() throws IOException {
        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            false, null, null)) {

            final Credentials credentials = provider.getCredentials();
            assertThat(credentials).isInstanceOf(NoCredentials.class);
        }
    }

    @Test
    void testAccessTokenCredentials() throws IOException {
        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, ACCESS_TOKEN_CREDENTIALS_JSON, null)) {

            final Credentials credentials = provider.getCredentials();
            assertThat(credentials).isNotNull();
            assertThat(credentials.getAuthenticationType()).isEqualTo("OAuth2");
            assertThat(credentials.getRequestMetadata()).containsEntry("Authorization", List.of("Bearer ya29...token1"));
        }
    }

    @Test
    void testPathCredentialsWithAutoReload(@TempDir final Path tempDir) throws IOException {
        final Path credentialsFile = tempDir.resolve("credentials.json");
        Files.write(credentialsFile, VALID_CREDENTIALS_JSON.getBytes());

        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, null, credentialsFile.toString())) {

            provider.enableWatching(1);

            final Credentials credentials = provider.getCredentials();
            assertThat(credentials).isNotNull();
        }
    }

    @Test
    void testPathCredentialsWithAutoReloadDetection(@TempDir final Path tempDir)
        throws IOException {
        final Path credentialsFile = tempDir.resolve("credentials.json");
        Files.write(credentialsFile, VALID_CREDENTIALS_JSON.getBytes());

        final AtomicInteger callbackCount = new AtomicInteger(0);
        final AtomicReference<Credentials> latestCredentials = new AtomicReference<>();

        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, null, credentialsFile.toString())) {

            provider.enableWatching(1);

            provider.setCredentialsUpdateCallback(credentials -> {
                callbackCount.incrementAndGet();
                latestCredentials.set(credentials);
            });

            final Credentials initialCredentials = provider.getCredentials();
            assertThat(initialCredentials).isNotNull();

            // Update the credentials file
            Files.write(credentialsFile, UPDATED_CREDENTIALS_JSON.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);

            // Wait for the callback to be called
            await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(callbackCount.get()).isOne());

            assertThat(callbackCount.get()).isOne();
            assertThat(latestCredentials.get()).isNotNull();

            // Update the credentials file again with same semantical contents
            Files.write(credentialsFile, (UPDATED_CREDENTIALS_JSON + " ").getBytes(),
                        StandardOpenOption.TRUNCATE_EXISTING);

            await().atMost(5, TimeUnit.SECONDS).until(() -> callbackCount.get() > 1);
            assertThat(callbackCount.get()).isEqualTo(2);
            assertThat(latestCredentials.get()).isNotNull();
        }
    }

    @Test
    void testPathCredentialsWithAutoReloadDetectionToken(@TempDir final Path tempDir)
        throws IOException {
        final Path credentialsFile = tempDir.resolve("credentials.json");
        Files.write(credentialsFile, ACCESS_TOKEN_CREDENTIALS_JSON.getBytes());

        final AtomicInteger callbackCount = new AtomicInteger(0);
        final AtomicReference<Credentials> latestCredentials = new AtomicReference<>();

        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, null, credentialsFile.toString())) {

            provider.enableWatching(1);

            provider.setCredentialsUpdateCallback(credentials -> {
                callbackCount.incrementAndGet();
                latestCredentials.set(credentials);
            });

            final Credentials initialCredentials = provider.getCredentials();
            assertThat(initialCredentials).isNotNull();

            // Update the credentials file
            Files.write(credentialsFile, ACCESS_TOKEN_CREDENTIALS2_JSON.getBytes(),
                StandardOpenOption.TRUNCATE_EXISTING);

            // Wait for the callback to be called
            await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(callbackCount.get()).isOne());

            assertThat(callbackCount.get()).isOne();
            assertThat(latestCredentials.get()).isNotNull();
            Credentials credentials = provider.getCredentials();
            assertThat(credentials).isNotNull();
            assertThat(credentials.getAuthenticationType()).isEqualTo("OAuth2");
            assertThat(credentials.getRequestMetadata()).containsEntry("Authorization", List.of("Bearer ya29...token2"));

            // Update the credentials file again
            Files.write(credentialsFile, ACCESS_TOKEN_CREDENTIALS3_JSON.getBytes(),
                        StandardOpenOption.TRUNCATE_EXISTING);

            await().atMost(5, TimeUnit.SECONDS).until(() -> callbackCount.get() == 2);
            assertThat(latestCredentials.get()).isNotNull();

            credentials = provider.getCredentials();
            assertThat(credentials).isNotNull();
            assertThat(credentials.getAuthenticationType()).isEqualTo("OAuth2");
            assertThat(credentials.getRequestMetadata()).containsEntry("Authorization", List.of("Bearer ya29...token3"));
        }
    }

    @Test
    void testFileWatchingWithInvalidUpdate(@TempDir final Path tempDir) throws IOException {
        final Path credentialsFile = tempDir.resolve("credentials.json");
        Files.write(credentialsFile, VALID_CREDENTIALS_JSON.getBytes());

        final AtomicInteger successCallbackCount = new AtomicInteger(0);

        try (final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, null, credentialsFile.toString())) {

            provider.enableWatching(1);

            provider.setCredentialsUpdateCallback(credentials -> successCallbackCount.incrementAndGet());

            final Credentials initialCredentials = provider.getCredentials();
            assertThat(initialCredentials).isNotNull();

            // Write invalid JSON to trigger an error
            Files.write(credentialsFile, "invalid json".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);

            // Give some time for the file watcher to process the change and check that successCallbackCount have not
            // been incremented during the wait
            await().during(2, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(successCallbackCount.get()).isZero());

            // Verifying that initial credentials are still there
            assertThat(provider.getCredentials()).isEqualTo(initialCredentials);

            // Write valid JSON again
            Files.write(credentialsFile, UPDATED_CREDENTIALS_JSON.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);

            // Give some time for the file watcher to process the change
            // Now the callback should be called
            await().atMost(5, TimeUnit.SECONDS).pollDelay(ofSeconds(1)).pollInterval(ofSeconds(1))
                .untilAsserted(() -> assertThat(successCallbackCount.get()).isOne());

            // Verifying that updated credentials are loaded
            final Credentials updatedCredentials = provider.getCredentials();
            assertThat(updatedCredentials)
                .isInstanceOfSatisfying(ServiceAccountCredentials.class, creds ->
                    assertThat(creds.getProjectId()).isEqualTo("updated-project")
                );
        }
    }

    @Test
    void testClose(@TempDir final Path tempDir) throws IOException {
        final Path credentialsFile = tempDir.resolve("credentials.json");
        Files.write(credentialsFile, VALID_CREDENTIALS_JSON.getBytes());

        final ReloadableCredentialsProvider provider = new ReloadableCredentialsProvider(
            null, null, credentialsFile.toString());

        provider.enableWatching(1);

        // Should not throw any exception
        provider.close();

        // Should be able to call close multiple times
        provider.close();
    }
}
