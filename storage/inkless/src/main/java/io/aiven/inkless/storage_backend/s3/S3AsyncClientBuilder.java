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
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * Builder for S3AsyncClient using AWS CRT for true non-blocking I/O.
 *
 * <p>This builder creates an S3AsyncClient backed by AwsCrtAsyncHttpClient which uses
 * native CRT (Common Runtime) for non-blocking network operations without consuming
 * JVM threads during I/O.
 */
class S3AsyncClientBuilder {

    static S3AsyncClient build(final Metrics metrics, final S3StorageConfig config) {
        final MetricCollector metricCollector = new MetricCollector(metrics);
        final software.amazon.awssdk.services.s3.S3AsyncClientBuilder s3ClientBuilder = S3AsyncClient.builder();

        // Region and endpoint
        final Region region = config.region();
        if (config.s3ServiceEndpoint() == null) {
            s3ClientBuilder.region(region);
        } else {
            s3ClientBuilder.region(region).endpointOverride(config.s3ServiceEndpoint());
        }
        if (config.pathStyleAccessEnabled() != null) {
            s3ClientBuilder.forcePathStyle(config.pathStyleAccessEnabled());
        }

        // CRT HTTP client
        // Note: CRT does not have TRUST_ALL_CERTIFICATES equivalent.
        // Certificate validation is always enabled in CRT for security.
        final AwsCrtAsyncHttpClient.Builder crtBuilder = AwsCrtAsyncHttpClient.builder()
            .maxConcurrency(config.httpMaxConnections());
        s3ClientBuilder.httpClient(crtBuilder.build());

        // Checksum validation
        s3ClientBuilder.serviceConfiguration(builder ->
            builder.checksumValidationEnabled(config.checksumCheckEnabled()));

        // Credentials
        final AwsCredentialsProvider credentialsProvider = config.credentialsProvider();
        if (credentialsProvider != null) {
            s3ClientBuilder.credentialsProvider(credentialsProvider);
        }

        // Override configuration (metrics, timeouts, retry)
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
