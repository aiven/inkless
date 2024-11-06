// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import io.aiven.inkless.common.ObjectKey;

public record BatchInfo(ObjectKey objectKey,
                        int byteOffset,
                        int size,
                        long numberOfRecords) {
}
