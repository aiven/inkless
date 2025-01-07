// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.cache;

import org.apache.kafka.common.cache.Cache;

public interface ObjectCache extends Cache<CacheKey, FileExtent> {
}
