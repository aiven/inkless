// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.cache;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;

public record CacheKey(ObjectKey object, ByteRange range) {
}
