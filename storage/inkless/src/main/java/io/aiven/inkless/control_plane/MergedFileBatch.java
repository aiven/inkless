// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import java.util.List;

public record MergedFileBatch(BatchMetadata metadata,
                              List<Long> parentBatches) {
}
