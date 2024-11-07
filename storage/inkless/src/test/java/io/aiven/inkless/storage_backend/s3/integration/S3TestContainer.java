// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.s3.integration;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

final class S3TestContainer {
    static LocalStackContainer container() {
        return new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8.1")
        ).withServices(LocalStackContainer.Service.S3);
    }
}
