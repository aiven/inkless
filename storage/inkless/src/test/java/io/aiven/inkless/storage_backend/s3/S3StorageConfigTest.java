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
package io.aiven.inkless.storage_backend.s3;

import org.apache.kafka.common.config.ConfigException;

import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3StorageConfigTest {
    private static final String BUCKET_NAME = "b1";
    private static final Region TEST_REGION = Region.US_EAST_2;
    private static final String MINIO_URL = "http://minio";

    // Test scenarios
    // - Minimal config
    @Test
    void minimalConfig() {
        final var configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id()
        );
        final var config = new S3StorageConfig(configs);

        assertThat(config.bucketName()).isEqualTo(BUCKET_NAME);
        assertThat(config.credentialsProvider()).isNull();
        assertThat(config.pathStyleAccessEnabled()).isNull();
        assertThat(config.certificateCheckEnabled()).isTrue();
        assertThat(config.httpMaxConnections()).isEqualTo(150);
        assertThat(config.checksumCheckEnabled()).isFalse();
        assertThat(config.region()).isEqualTo(TEST_REGION);
        assertThat(config.s3ServiceEndpoint()).isNull();
        assertThat(config.apiCallTimeout()).isNull();
        assertThat(config.apiCallAttemptTimeout()).isNull();
    }

    // - Credential provider scenarios
    //   - Without provider
    @Test
    void configWithoutCredentialsProvider() {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "s3.path.style.access.enabled", true
        );
        final var config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(BUCKET_NAME);
        assertThat(config.credentialsProvider()).isNull();
        assertThat(config.getBoolean(S3StorageConfig.S3_PATH_STYLE_ENABLED_CONFIG)).isTrue();
        assertThat(config.pathStyleAccessEnabled()).isTrue();
        assertThat(config.certificateCheckEnabled()).isTrue();
        assertThat(config.httpMaxConnections()).isEqualTo(150);
        assertThat(config.region()).isEqualTo(TEST_REGION);
        assertThat(config.s3ServiceEndpoint()).extracting(URI::getHost).isEqualTo("minio");
        assertThat(config.apiCallTimeout()).isNull();
        assertThat(config.apiCallAttemptTimeout()).isNull();
    }

    //   - With provider
    @Test
    void configWithProvider() {
        final var customCredentialsProvider = EnvironmentVariableCredentialsProvider.class;
        final int partSize = 10 * 1024 * 1024;
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "s3.path.style.access.enabled", false,
            "aws.credentials.provider.class", customCredentialsProvider.getName());

        final var config = new S3StorageConfig(configs);

        assertThat(config.bucketName()).isEqualTo(BUCKET_NAME);
        assertThat(config.pathStyleAccessEnabled()).isFalse();
        assertThat(config.certificateCheckEnabled()).isTrue();
        assertThat(config.httpMaxConnections()).isEqualTo(150);
        assertThat(config.credentialsProvider()).isInstanceOf(customCredentialsProvider);
        assertThat(config.region()).isEqualTo(TEST_REGION);
        assertThat(config.s3ServiceEndpoint()).extracting(URI::getHost).isEqualTo("minio");
        assertThat(config.apiCallTimeout()).isNull();
        assertThat(config.apiCallAttemptTimeout()).isNull();
    }

    //   - With static credentials
    @Test
    void configWithStaticCredentials() {
        final Region region = Region.US_EAST_2;
        final String username = "username";
        final String password = "password";
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", region.id(),
            "s3.endpoint.url", MINIO_URL,
            "aws.access.key.id", username,
            "aws.secret.access.key", password,
            "aws.certificate.check.enabled", "false",
            "aws.http.max.connections", "200",
            "aws.checksum.check.enabled", "true");

        final var config = new S3StorageConfig(configs);

        assertThat(config.bucketName()).isEqualTo(BUCKET_NAME);
        assertThat(config.getString("s3.region")).isEqualTo(region.id());
        assertThat(config.getString("s3.endpoint.url")).isEqualTo(MINIO_URL);
        assertThat(config.getPassword("aws.access.key.id").value()).isEqualTo(username);
        assertThat(config.getPassword("aws.secret.access.key").value()).isEqualTo(password);
        assertThat(config.certificateCheckEnabled()).isFalse();
        assertThat(config.httpMaxConnections()).isEqualTo(200);
        assertThat(config.checksumCheckEnabled()).isTrue();

        final AwsCredentialsProvider credentialsProvider = config.credentialsProvider();
        assertThat(credentialsProvider).isInstanceOf(StaticCredentialsProvider.class);
        final var awsCredentials = credentialsProvider.resolveCredentials();
        assertThat(awsCredentials.accessKeyId()).isEqualTo(username);
        assertThat(awsCredentials.secretAccessKey()).isEqualTo(password);
        assertThat(config.region()).isEqualTo(TEST_REGION);
        assertThat(config.s3ServiceEndpoint()).extracting(URI::getHost).isEqualTo("minio");
        assertThat(config.apiCallTimeout()).isNull();
        assertThat(config.apiCallAttemptTimeout()).isNull();
    }

    @Test
    void configWithCredentialsInFile() throws IOException {
        final Region region = Region.US_EAST_2;
        final String username = "username";
        final String password = "password";
        final String sessiontoken = "sessiontoken";
        final File credentialsFile = Files.newTemporaryFile();
        try (FileWriter writer = new FileWriter(credentialsFile)) {
            writer.write("inkless.storage.aws.access.key.id=" + username + "\n");
            writer.write("inkless.storage.aws.secret.access.key=" + password + "\n");
            writer.write("inkless.storage.aws.session.token=" + sessiontoken + "\n");
        }

        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", region.id(),
            "s3.endpoint.url", MINIO_URL,
            "aws.credentials.file", credentialsFile.getAbsolutePath());

        final var config = new S3StorageConfig(configs);

        final AwsCredentialsProvider credentialsProvider = config.credentialsProvider();
        assertThat(credentialsProvider).isInstanceOf(S3RotatingCredentialsProvider.class);
        final var awsCredentials = credentialsProvider.resolveCredentials();
        assertThat(awsCredentials).isInstanceOf(AwsSessionCredentials.class);
        assertThat(awsCredentials.accessKeyId()).isEqualTo(username);
        assertThat(awsCredentials.secretAccessKey()).isEqualTo(password);
        assertThat(((AwsSessionCredentials) awsCredentials).sessionToken()).isEqualTo(sessiontoken);
    }

    //   - With missing static credentials
    @Test
    void configWithMissingStaticConfig() {
        final String username = "username";
        final String password = "password";
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "aws.access.key.id", username)))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.access.key.id and aws.secret.access.key must be defined together");
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "aws.secret.access.key", password)))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.access.key.id and aws.secret.access.key must be defined together");
    }

    //   - With empty static credentials
    @Test
    void configWithEmptyStaticConfig() {
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "aws.access.key.id", "")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.access.key.id value must not be empty");
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "aws.secret.access.key", "")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.secret.access.key value must not be empty");
    }

    //   - With conflict between static and custom
    @Test
    void configWithConflictBetweenCustomProviderAndStaticCredentials() {
        final String username = "username";
        final String password = "password";
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "aws.credentials.provider.class", EnvironmentVariableCredentialsProvider.class.getName(),
            "aws.access.key.id", username,
            "aws.secret.access.key", password);
        assertThatThrownBy(() -> new S3StorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Either  static credential pair aws.access.key.id and aws.secret.access.key "
                + "must be set together, "
                + "or a custom provider class aws.credentials.provider.class.");
    }

    // - Failing configs scenarios
    @Test
    void shouldRequireS3BucketName() {
        assertThatThrownBy(() -> new S3StorageConfig(Map.of()))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"s3.bucket.name\" which has no default value.");
    }

    @Test
    void withApiCallTimeouts() {
        final var configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.api.call.timeout", 5000,
            "s3.api.call.attempt.timeout", 1000
        );
        final var config = new S3StorageConfig(configs);
        assertThat(config.apiCallTimeout()).isEqualTo(Duration.ofMillis(5000));
        assertThat(config.apiCallAttemptTimeout()).isEqualTo(Duration.ofMillis(1000));
    }

    @Test
    void emptyBucketName() {
        final var configs = Map.of(
            "s3.bucket.name", "",
            "s3.region", TEST_REGION.id()
        );
        assertThatThrownBy(() -> new S3StorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value  for configuration s3.bucket.name: String must be non-empty");
    }

    @Test
    void invalidUrl() {
        final var configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", "in valid"
        );
        assertThatThrownBy(() -> new S3StorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value in valid for configuration s3.endpoint.url: Must be a valid URL");
    }

    @Test
    void invalidApiCallTimeout() {
        final var configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.api.call.timeout", "0"
        );
        assertThatThrownBy(() -> new S3StorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration s3.api.call.timeout: Value must be at least 1");
    }

    @Test
    void invalidApiCallAttemptTimeout() {
        final var configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "s3.api.call.attempt.timeout", "0"
        );
        assertThatThrownBy(() -> new S3StorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration s3.api.call.attempt.timeout: Value must be at least 1");
    }

    @Test
    void invalidAwsCredentialsProviderClass() {
        final var configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "aws.credentials.provider.class", String.class.getCanonicalName()
        );
        assertThatThrownBy(() -> new S3StorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.credentials.provider.class should be a subclass of software.amazon.awssdk.auth.credentials.AwsCredentialsProvider");
    }

    @Test
    void staticCredentialsProviderWithoutCredentials() {
        final var configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "aws.credentials.provider.class", StaticCredentialsProvider.class
        );
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThatThrownBy(config::credentialsProvider)
            .isInstanceOf(ConfigException.class)
            .hasMessage("With software.amazon.awssdk.auth.credentials.StaticCredentialsProvider AWS credentials must be provided");
    }

    @Test
    void anonymousCredentialsProvider() {
        final var configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class
        );
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.credentialsProvider())
            .isInstanceOf(AnonymousCredentialsProvider.class);
    }

    @Test
    void staticCredentials() {
        final var configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", TEST_REGION.id(),
            "aws.access.key.id", "key",
            "aws.secret.access.key", "key"
        );
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.credentialsProvider())
            .isInstanceOf(StaticCredentialsProvider.class);
    }
}
