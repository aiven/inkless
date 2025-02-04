// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.s3.integration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.storage_backend.common.fixtures.TestObjectKey;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.test_utils.S3TestContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

@Tag("integration")
@Testcontainers
class S3StorageMetricsTest {

    @Container
    private static final LocalStackContainer LOCALSTACK = S3TestContainer.localstack();

    private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    private static final String BUCKET_NAME = "test-bucket";

    private S3Storage storage;

    @BeforeAll
    static void setupS3() {
        final var clientBuilder = S3Client.builder();
        clientBuilder.region(Region.of(LOCALSTACK.getRegion()))
            .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        LOCALSTACK.getAccessKey(),
                        LOCALSTACK.getSecretKey()
                    )
                )
            );
        try (final S3Client s3Client = clientBuilder.build()) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
        }
    }

    @BeforeEach
    void setupStorage() {
        storage = new S3Storage();
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", LOCALSTACK.getRegion(),
            "s3.endpoint.url", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
            "aws.access.key.id", LOCALSTACK.getAccessKey(),
            "aws.secret.access.key", LOCALSTACK.getSecretKey(),
            "s3.path.style.access.enabled", true
        );
        storage.configure(configs);
    }

    @Test
    void metricsShouldBeReported() throws Exception {
        final byte[] data = new byte[100];

        final ObjectKey key = new TestObjectKey("x");

        storage.upload(key, data);
        try (final InputStream fetch = storage.fetch(key, ByteRange.maxRange())) {
            fetch.readAllBytes();
        }
        try (final InputStream fetch = storage.fetch(key, new ByteRange(0, 1))) {
            fetch.readAllBytes();
        }
        storage.delete(key);
        storage.delete(Set.of(key));

        final ObjectName segmentCopyPerSecName = ObjectName.getInstance(
            "aiven.inkless.server.s3:type=s3-client-metrics");
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "get-object-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "get-object-requests-total"))
            .isEqualTo(2.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "get-object-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "get-object-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "put-object-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "put-object-requests-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "put-object-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "put-object-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-object-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-object-requests-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-object-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-object-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);

        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-objects-requests-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-objects-requests-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-objects-time-avg"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(segmentCopyPerSecName, "delete-objects-time-max"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }
}
