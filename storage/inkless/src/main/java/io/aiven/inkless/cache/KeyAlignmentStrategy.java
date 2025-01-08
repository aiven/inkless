// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.cache;

import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;

/**
 * Computes a set of aligned cache keys that cover the requested byte ranges.
 * Aligned keys often equal other aligned keys, improving cache hit rates.
 */
public interface KeyAlignmentStrategy {

    Set<ByteRange> align(List<ByteRange> ranges);
}
