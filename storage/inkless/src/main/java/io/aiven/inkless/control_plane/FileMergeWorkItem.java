// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import java.time.Instant;
import java.util.List;

public record FileMergeWorkItem(long workItemId,
                                Instant createdAt,
                                List<File> files) {

    public record File(long fileId,
                       String objectKey,
                       List<BatchInfo> batches) {
    }
}
