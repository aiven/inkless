// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.cache;

import java.nio.ByteBuffer;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;

public record FileExtent(
        ObjectKey key,
        ByteRange range,
        ByteBuffer buffer) {
}
