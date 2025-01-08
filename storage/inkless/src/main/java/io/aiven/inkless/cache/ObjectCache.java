// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.cache;

import org.apache.kafka.common.cache.Cache;

import io.aiven.inkless.generated.CacheKey;
import io.aiven.inkless.generated.FileExtent;

public interface ObjectCache extends Cache<CacheKey, FileExtent> {
}
