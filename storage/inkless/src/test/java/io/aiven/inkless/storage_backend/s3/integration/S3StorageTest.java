// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.s3.integration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Map;

import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.fixtures.BaseStorageTest;
import io.aiven.inkless.storage_backend.common.fixtures.TestUtils;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.test_utils.S3TestContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

@Testcontainers
@Tag("integration")
public class S3StorageTest extends BaseStorageTest {
    @Container
    private static final LocalStackContainer LOCALSTACK = S3TestContainer.localstack();

    private static S3Client s3Client;
    private String bucketName;

    @BeforeAll
    static void setUpClass() {
        s3Client = S3Client.builder()
            .region(Region.of(LOCALSTACK.getRegion()))
            .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        LOCALSTACK.getAccessKey(),
                        LOCALSTACK.getSecretKey()
                    )
                )
            )
            .build();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        bucketName = TestUtils.testNameToBucketName(testInfo);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }

    @AfterAll
    static void closeAll() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Override
    protected StorageBackend storage() {
        final S3Storage s3Storage = new S3Storage();
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", LOCALSTACK.getRegion(),
            "s3.endpoint.url", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
            "aws.access.key.id", LOCALSTACK.getAccessKey(),
            "aws.secret.access.key", LOCALSTACK.getSecretKey(),
            "s3.path.style.access.enabled", true
        );
        s3Storage.configure(configs);
        return s3Storage;
    }
}
