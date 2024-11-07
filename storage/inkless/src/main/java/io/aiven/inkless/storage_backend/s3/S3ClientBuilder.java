// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.s3;

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.internal.http.loader.DefaultSdkHttpClientBuilder;
import software.amazon.awssdk.http.SdkHttpConfigurationOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.AttributeMap;

class S3ClientBuilder {
    static S3Client build(final S3StorageConfig config) {
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

        if (!config.certificateCheckEnabled()) {
            s3ClientBuilder.httpClient(
                new DefaultSdkHttpClientBuilder()
                    .buildWithDefaults(
                        AttributeMap.builder()
                            .put(SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES, true)
                            .build()
                    )
            );
        }

        s3ClientBuilder.serviceConfiguration(builder ->
            builder.checksumValidationEnabled(config.checksumCheckEnabled()));

        final AwsCredentialsProvider credentialsProvider = config.credentialsProvider();
        if (credentialsProvider != null) {
            s3ClientBuilder.credentialsProvider(credentialsProvider);
        }
        s3ClientBuilder.overrideConfiguration(c -> {
            c.addMetricPublisher(new MetricCollector());
            c.apiCallTimeout(config.apiCallTimeout());
            c.apiCallAttemptTimeout(config.apiCallAttemptTimeout());
        });
        return s3ClientBuilder.build();
    }
}
