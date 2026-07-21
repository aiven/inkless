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

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.AttributeMap;

class S3ClientBuilder {
    static S3Client build(final Metrics metrics, final S3StorageConfig config) {
        final MetricCollector metricCollector = new MetricCollector(metrics);
        final software.amazon.awssdk.services.s3.S3ClientBuilder s3ClientBuilder = S3Client.builder();
        final Region region = config.region();
        if (config.s3ServiceEndpoint() == null) {
            s3ClientBuilder.region(region);
        } else {
            s3ClientBuilder.region(region)
                .endpointOverride(config.s3ServiceEndpoint());
        }
        if (config.pathStyleAccessEnabled() != null) {
            s3ClientBuilder.forcePathStyle(config.pathStyleAccessEnabled());
        }

        final AttributeMap.Builder httpClientAttributeBuilder = AttributeMap.builder();
        httpClientAttributeBuilder.put(SdkHttpConfigurationOption.MAX_CONNECTIONS, config.httpMaxConnections());
        if (!config.certificateCheckEnabled()) {
            httpClientAttributeBuilder.put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true);
        }
        s3ClientBuilder.httpClient(
            new DefaultSdkHttpClientBuilder().buildWithDefaults(httpClientAttributeBuilder.build())
        );

        s3ClientBuilder.serviceConfiguration(builder ->
            builder.checksumValidationEnabled(config.checksumCheckEnabled()));

        final AwsCredentialsProvider credentialsProvider = config.credentialsProvider();
        if (credentialsProvider != null) {
            s3ClientBuilder.credentialsProvider(credentialsProvider);
        }
        s3ClientBuilder.overrideConfiguration(c -> {
            c.addMetricPublisher(metricCollector);
            c.apiCallTimeout(config.apiCallTimeout());
            c.apiCallAttemptTimeout(config.apiCallAttemptTimeout());
            // Optimizes for concurrency and high throughput use-cases by adjusting request rate dynamically
            c.retryStrategy(RetryMode.ADAPTIVE_V2);
        });
        return s3ClientBuilder.build();
    }
}
