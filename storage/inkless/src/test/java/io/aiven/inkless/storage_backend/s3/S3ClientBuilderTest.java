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

import org.apache.kafka.common.metrics.Metrics;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Duration;
import java.util.Map;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.AttributeMap;

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
        final var s3Client = S3ClientBuilder.build(new Metrics(), config);
        final var clientConfiguration = s3Client.serviceClientConfiguration();
        assertThat(clientConfiguration.region()).isEqualTo(TEST_REGION);
        assertThat(clientConfiguration.endpointOverride()).isNotPresent();
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers())
            .allSatisfy(metricPublisher -> assertThat(metricPublisher).isInstanceOf(MetricCollector.class));
        assertThat(clientConfiguration.overrideConfiguration().apiCallTimeout()).isEmpty();
        assertThat(clientConfiguration.overrideConfiguration().apiCallAttemptTimeout()).isEmpty();

        final AttributeMap resolvedOptions = getInternalHttpClientResolvedOptions(s3Client);
        assertThat(resolvedOptions.get(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES)).isFalse();
        assertThat(resolvedOptions.get(SdkHttpConfigurationOption.MAX_CONNECTIONS)).isEqualTo(150);
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
        final var s3Client = S3ClientBuilder.build(new Metrics(), config);
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

        final AttributeMap resolvedOptions = getInternalHttpClientResolvedOptions(s3Client);
        assertThat(resolvedOptions.get(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES)).isFalse();
        assertThat(resolvedOptions.get(SdkHttpConfigurationOption.MAX_CONNECTIONS)).isEqualTo(150);
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
        final var s3Client = S3ClientBuilder.build(new Metrics(), config);
        final var clientConfiguration = s3Client.serviceClientConfiguration();
        assertThat(clientConfiguration.region()).isEqualTo(TEST_REGION);
        assertThat(clientConfiguration.endpointOverride()).hasValue(URI.create("http://minio"));
        assertThat(clientConfiguration.overrideConfiguration().apiCallTimeout()).isEmpty();
        assertThat(clientConfiguration.overrideConfiguration().apiCallAttemptTimeout()).isEmpty();
        assertThat(clientConfiguration.credentialsProvider()).isInstanceOf(customCredentialsProvider);
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers()).hasSize(1);
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers()).element(0)
            .isInstanceOf(MetricCollector.class);

        final AttributeMap resolvedOptions = getInternalHttpClientResolvedOptions(s3Client);
        assertThat(resolvedOptions.get(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES)).isFalse();
        assertThat(resolvedOptions.get(SdkHttpConfigurationOption.MAX_CONNECTIONS)).isEqualTo(150);
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
            "aws.http.max.connections", "200",
            "aws.checksum.check.enabled", "true");

        final var config = new S3StorageConfig(configs);
        final var s3Client = S3ClientBuilder.build(new Metrics(), config);
        final var clientConfiguration = s3Client.serviceClientConfiguration();
        assertThat(clientConfiguration.region()).isEqualTo(TEST_REGION);
        assertThat(clientConfiguration.endpointOverride()).hasValue(URI.create("http://minio"));
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers())
            .allSatisfy(metricPublisher -> assertThat(metricPublisher).isInstanceOf(MetricCollector.class));
        assertThat(clientConfiguration.overrideConfiguration().apiCallTimeout()).isEmpty();
        assertThat(clientConfiguration.overrideConfiguration().apiCallAttemptTimeout()).isEmpty();

        final AttributeMap resolvedOptions = getInternalHttpClientResolvedOptions(s3Client);
        assertThat(resolvedOptions.get(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES)).isTrue();
        assertThat(resolvedOptions.get(SdkHttpConfigurationOption.MAX_CONNECTIONS)).isEqualTo(200);
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
        final var s3Client = S3ClientBuilder.build(new Metrics(), config);
        final var clientConfiguration = s3Client.serviceClientConfiguration();
        assertThat(clientConfiguration.region()).isEqualTo(TEST_REGION);
        assertThat(clientConfiguration.endpointOverride()).isNotPresent();
        assertThat(clientConfiguration.overrideConfiguration().metricPublishers())
            .allSatisfy(metricPublisher -> assertThat(metricPublisher).isInstanceOf(MetricCollector.class));
        assertThat(clientConfiguration.overrideConfiguration().apiCallTimeout()).hasValue(Duration.ofMillis(5000));
        assertThat(clientConfiguration.overrideConfiguration().apiCallAttemptTimeout())
            .hasValue(Duration.ofMillis(1000));

        final AttributeMap resolvedOptions = getInternalHttpClientResolvedOptions(s3Client);
        assertThat(resolvedOptions.get(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES)).isFalse();
        assertThat(resolvedOptions.get(SdkHttpConfigurationOption.MAX_CONNECTIONS)).isEqualTo(150);
    }

    private static AttributeMap getInternalHttpClientResolvedOptions(final S3Client s3Client) {
        try {
            final Field clientConfigurationField = s3Client.getClass().getDeclaredField("clientConfiguration");
            clientConfigurationField.setAccessible(true);
            final SdkClientConfiguration sdkClientConfiguration = (SdkClientConfiguration) clientConfigurationField.get(s3Client);
            final SdkHttpClient httpClient = sdkClientConfiguration.option(SdkClientOption.CONFIGURED_SYNC_HTTP_CLIENT);
            final Field delegateField = httpClient.getClass().getDeclaredField("delegate");
            delegateField.setAccessible(true);
            final SdkHttpClient httpClientDelegate = (SdkHttpClient) delegateField.get(httpClient);
            final Field resolvedOptionsField = httpClientDelegate.getClass().getDeclaredField("resolvedOptions");
            resolvedOptionsField.setAccessible(true);
            return (AttributeMap) resolvedOptionsField.get(httpClientDelegate);
        } catch (final NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
