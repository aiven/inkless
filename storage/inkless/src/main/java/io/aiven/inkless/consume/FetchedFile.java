// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.consume;

import java.nio.ByteBuffer;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;

public record FetchedFile(
        ObjectKey key,
        ByteRange range,
        ByteBuffer buffer) {

}
