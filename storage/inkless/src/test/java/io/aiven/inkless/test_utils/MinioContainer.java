// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class MinioContainer extends GenericContainer<MinioContainer> {
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("quay.io/minio/minio");

    private static final int DEFAULT_PORT = 9000;
    private static final String DEFAULT_ACCESS_KEY = "minioadmin";
    private static final String DEFAULT_SECRET_KEY = "minioadmin";
    public static final Region REGION = Region.US_EAST_1;

    public MinioContainer() {
        this(DEFAULT_IMAGE_NAME.withTag("latest"));
    }

    public MinioContainer(DockerImageName dockerImageName) {
        super(dockerImageName);

        withExposedPorts(DEFAULT_PORT);
        withCommand("server", "/data");
        withEnv("MINIO_ACCESS_KEY", DEFAULT_ACCESS_KEY);
        withEnv("MINIO_SECRET_KEY", DEFAULT_SECRET_KEY);
    }

    public S3Client getS3Client() {
        return S3Client.builder()
            .endpointOverride(URI.create(getEndpoint()))
            .credentialsProvider(StaticCredentialsProvider.create(
                AwsBasicCredentials.create(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)))
            .region(REGION)
            .forcePathStyle(true)
            .build();
    }

    public String getEndpoint() {
        return String.format("http://%s:%d", getHost(), getMappedPort(DEFAULT_PORT));
    }

    public String getRegion() {
        return REGION.id();
    }

    public String getAccessKey() {
        return DEFAULT_ACCESS_KEY;
    }

    public String getSecretKey() {
        return DEFAULT_SECRET_KEY;
    }
}
