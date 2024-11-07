// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.s3;

import java.net.URI;
import java.time.Duration;
import java.util.Map;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import static org.assertj.core.api.Assertions.assertThat;

class S3ClientBuilderTest {
    private static final Region TEST_REGION = Region.US_EAST_2;
    private static final String MINIO_URL = "http://minio";

    @Test
    void minimalConfig() {
        final var configs = Map.of(
            "s3.bucket.name", "b",
            "s3.region", TEST_REGION.id()
        );
        final var config = new S3StorageConfig(configs);
        final var s3Client = S3ClientBuilder.build(config);
        final var clientConfiguration = s3Client.serviceClientConfiguration();
        assertThat(clientConfiguration.region()).isEqualTo(TEST_REGION);
        assertThat(clientConfiguration.endpointOverride()).isNotPresent();
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers())
            .allSatisfy(metricPublisher -> assertThat(metricPublisher).isInstanceOf(MetricCollector.class));
        assertThat(clientConfiguration.overrideConfiguration().apiCallTimeout()).isEmpty();
        assertThat(clientConfiguration.overrideConfiguration().apiCallAttemptTimeout()).isEmpty();
    }

    @Test
    void configWithoutCredentialsProvider() {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", "b",
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "s3.path.style.access.enabled", true
        );
        final var config = new S3StorageConfig(configs);
        final var s3Client = S3ClientBuilder.build(config);
        final var clientConfiguration = s3Client.serviceClientConfiguration();
        assertThat(clientConfiguration.region()).isEqualTo(TEST_REGION);
        assertThat(clientConfiguration.endpointOverride()).hasValue(URI.create("http://minio"));
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers())
            .allSatisfy(metricPublisher -> assertThat(metricPublisher).isInstanceOf(MetricCollector.class));
        assertThat(clientConfiguration.overrideConfiguration().apiCallTimeout()).isEmpty();
        assertThat(clientConfiguration.overrideConfiguration().apiCallAttemptTimeout()).isEmpty();
        assertThat(clientConfiguration.credentialsProvider()).isInstanceOf(DefaultCredentialsProvider.class);
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers()).hasSize(1);
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers()).element(0)
            .isInstanceOf(MetricCollector.class);
    }

    @Test
    void configWithProvider() {
        final var customCredentialsProvider = EnvironmentVariableCredentialsProvider.class;
        final int partSize = 10 * 1024 * 1024;
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", "b",
            "s3.region", TEST_REGION.id(),
            "s3.endpoint.url", MINIO_URL,
            "s3.path.style.access.enabled", false,
            "s3.multipart.upload.part.size", partSize,
            "aws.credentials.provider.class", customCredentialsProvider.getName());

        final var config = new S3StorageConfig(configs);
        final var s3Client = S3ClientBuilder.build(config);
        final var clientConfiguration = s3Client.serviceClientConfiguration();
        assertThat(clientConfiguration.region()).isEqualTo(TEST_REGION);
        assertThat(clientConfiguration.endpointOverride()).hasValue(URI.create("http://minio"));
        assertThat(clientConfiguration.overrideConfiguration().apiCallTimeout()).isEmpty();
        assertThat(clientConfiguration.overrideConfiguration().apiCallAttemptTimeout()).isEmpty();
        assertThat(clientConfiguration.credentialsProvider()).isInstanceOf(customCredentialsProvider);
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers()).hasSize(1);
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers()).element(0)
            .isInstanceOf(MetricCollector.class);
    }

    @Test
    void configWithStaticCredentials() {
        final Region region = Region.US_EAST_2;
        final String username = "username";
        final String password = "password";
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", "b",
            "s3.region", region.id(),
            "s3.endpoint.url", MINIO_URL,
            "aws.access.key.id", username,
            "aws.secret.access.key", password,
            "aws.certificate.check.enabled", "false",
            "aws.checksum.check.enabled", "true");

        final var config = new S3StorageConfig(configs);
        final var s3Client = S3ClientBuilder.build(config);
        final var clientConfiguration = s3Client.serviceClientConfiguration();
        assertThat(clientConfiguration.region()).isEqualTo(TEST_REGION);
        assertThat(clientConfiguration.endpointOverride()).hasValue(URI.create("http://minio"));
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers())
            .allSatisfy(metricPublisher -> assertThat(metricPublisher).isInstanceOf(MetricCollector.class));
        assertThat(clientConfiguration.overrideConfiguration().apiCallTimeout()).isEmpty();
        assertThat(clientConfiguration.overrideConfiguration().apiCallAttemptTimeout()).isEmpty();
    }

    @Test
    void withApiCallTimeouts() {
        final var configs = Map.of(
            "s3.bucket.name", "b",
            "s3.region", TEST_REGION.id(),
            "s3.api.call.timeout", 5000,
            "s3.api.call.attempt.timeout", 1000
        );
        final var config = new S3StorageConfig(configs);
        final var s3Client = S3ClientBuilder.build(config);
        final var clientConfiguration = s3Client.serviceClientConfiguration();
        assertThat(clientConfiguration.region()).isEqualTo(TEST_REGION);
        assertThat(clientConfiguration.endpointOverride()).isNotPresent();
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers())
            .allSatisfy(metricPublisher -> assertThat(metricPublisher).isInstanceOf(MetricCollector.class));
        assertThat(clientConfiguration.overrideConfiguration().apiCallTimeout()).hasValue(Duration.ofMillis(5000));
        assertThat(clientConfiguration.overrideConfiguration().apiCallAttemptTimeout())
            .hasValue(Duration.ofMillis(1000));
    }
}
