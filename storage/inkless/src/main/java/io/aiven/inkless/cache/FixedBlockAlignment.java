// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.cache;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;

/**
 * Strategy which breaks files into blocks of a specified fixed size for caching and fetching.
 */
public class FixedBlockAlignment implements KeyAlignmentStrategy {

    private final int blockSize;

    public FixedBlockAlignment(int blockSize) {
        this.blockSize = blockSize;
    }

    @Override
    public Set<CacheKey> align(ObjectKey key, List<ByteRange> ranges) {
        if (key == null || ranges == null) {
            return Collections.emptySet();
        }
        HashSet<CacheKey> keys = new HashSet<>();
        for (ByteRange requestRange : ranges) {
            // Rely on integer division to align ranges within some offset.
            long firstOffset = blockSize * (requestRange.offset() / blockSize);
            ByteRange blockRange = new ByteRange(firstOffset, blockSize);
            if (blockRange.contains(requestRange)) {
                keys.add(new CacheKey(key, blockRange));
            } else {
                // TODO INK-77: For ranges which cross multiple blocks, issue multiple requests and concatenate them later.
                keys.add(new CacheKey(key, requestRange));
            }
        }
        return Collections.unmodifiableSet(keys);
    }
}
