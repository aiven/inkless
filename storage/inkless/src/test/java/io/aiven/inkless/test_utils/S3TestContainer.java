// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

public final class S3TestContainer {
    public static MinioContainer minio() {
        return new MinioContainer();
    }
}
