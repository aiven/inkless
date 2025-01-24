// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

public record BatchInfo(
    long batchId,
    String objectKey,
    BatchMetadata metadata
) {
}
