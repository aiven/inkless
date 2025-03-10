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
package io.aiven.inkless.test_utils;

import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

public class MinioContainer extends GenericContainer<MinioContainer> {
    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("quay.io/minio/minio");

    private static final Logger log = LoggerFactory.getLogger(MinioContainer.class);

    private static final int DEFAULT_PORT = 9000;
    private static final int DEFAULT_CONSOLE_PORT = 9001;
    private static final String DEFAULT_ACCESS_KEY = "minioadmin";
    private static final String DEFAULT_SECRET_KEY = "minioadmin";
    private static final Region REGION = Region.US_EAST_1;

    private String bucketName;

    public MinioContainer() {
        this(DEFAULT_IMAGE_NAME.withTag("latest"));
    }

    public MinioContainer(DockerImageName dockerImageName) {
        super(dockerImageName);

        withExposedPorts(DEFAULT_PORT, DEFAULT_CONSOLE_PORT);
        withCommand("server", "/data", "--console-address", ":" + DEFAULT_CONSOLE_PORT);
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

    public void createBucket(final TestInfo testInfo) {
        createBucket(bucketNameFromTestInfo(testInfo));
    }

    private static String bucketNameFromTestInfo(final TestInfo testInfo) {
        String dbName = testInfo.getDisplayName()
            .toLowerCase()
            .replace(" ", "")
            .replace("\"", "")
            .replace(",", "")
            .replace(".", "")
            .replace("=", "")
            .replace("_", "")
            .replace("(", "")
            .replace(")", "")
            .replace("[", "")
            .replace("]", "");
        dbName = dbName.substring(0, Math.min(40, dbName.length()));
        dbName = "d" + dbName;  // handle preceding digits
        dbName += "-" + TestUtils.randomString(20);
        return dbName.toLowerCase();
    }


    public void createBucket(final String bucketName) {
        this.bucketName = bucketName;
        getS3Client().createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
        log.info("Created bucket: {}", bucketName);
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

    public String getBucketName() {
        return bucketName;
    }
}
